defmodule Socketelx.Backend.Redis.Topology do
  @moduledoc """
  Finds and follows the Redis Cluster node that owns this app's keys.

  In cluster mode every key of an app shares one hash tag (see
  `Socketelx.Config`), so they all live in one slot, on one node. That keeps the
  multi-key Lua scripts working, and it means this process only has to answer one
  question: *which node owns that slot right now?*

  It asks with `CLUSTER SLOTS`, connects `Redix` (commands) and `Redix.PubSub`
  (broadcasts) to that node under the usual names, and moves them when the answer
  changes: a failover, a resharding, or the node going away. It starts from the
  Redis URL, plus any `:redis_cluster_nodes`, plus every master it has seen.

  Callers never talk to this process on the hot path. `Socketelx.Backend.Redis`
  uses the connection directly and only asks for a `refresh/1` after a `MOVED` or
  similar answer.
  """
  use GenServer
  import Bitwise
  require Logger

  alias Socketelx.{Config, Naming}

  @slots 16_384
  @connect_timeout 800
  @command_timeout 2_000
  @tick_ms 2_000
  @retry_ms 500
  # a refresh that just ran is trusted for this long
  @fresh_ms 1_000
  @max_candidates 5

  # CRC16/XMODEM, the checksum Redis Cluster uses to pick a slot.
  @crc_table (for i <- 0..255 do
                Enum.reduce(1..8, bsl(i, 8), fn _, crc ->
                  shifted = band(bsl(crc, 1), 0xFFFF)
                  if band(crc, 0x8000) != 0, do: bxor(shifted, 0x1021), else: shifted
                end)
              end)
             |> List.to_tuple()

  @doc "The hash slot (0..16383) of a key, honouring `{hash tags}` like Redis does."
  def key_slot(key) when is_binary(key), do: key |> hash_tag() |> crc16() |> rem(@slots)

  defp hash_tag(key) do
    with [_before, rest] <- :binary.split(key, "{"),
         [tag, _after] when tag != "" <- :binary.split(rest, "}") do
      tag
    else
      _ -> key
    end
  end

  defp crc16(binary), do: crc16(binary, 0)
  defp crc16(<<>>, crc), do: crc

  defp crc16(<<byte, rest::binary>>, crc) do
    index = band(bxor(bsr(crc, 8), byte), 0xFF)
    crc16(rest, band(bxor(bsl(crc, 8), elem(@crc_table, index)), 0xFFFF))
  end

  @doc false
  def start_link(name),
    do: GenServer.start_link(__MODULE__, name, name: Naming.redis_topology(name))

  @doc """
  Re-reads the slot map and moves the connections if the owner changed. Blocks
  until done (a few milliseconds normally). A `MOVED` answer names the new owner;
  pass it as `hint` (`{host, port}`) and the connections move there at once, without
  waiting for the slot map to catch up (it can lag a moment behind a failover). `:ok` when the owner is known,
  `:error` when no node could be reached.
  """
  def refresh(name, hint \\ nil) do
    GenServer.call(Naming.redis_topology(name), {:refresh, hint}, 6_000)
  catch
    :exit, _ -> :error
  end

  @doc "Same, without waiting. Used after connection errors."
  def refresh_async(name) do
    GenServer.cast(Naming.redis_topology(name), :refresh)
  end

  # ---- server ----

  @impl true
  def init(name) do
    Process.flag(:trap_exit, true)
    config = Config.get(name)
    slot = key_slot(config.prefix <> ":")

    state = %{
      name: name,
      config: config,
      slot: slot,
      owner: nil,
      conn: nil,
      pubsub: nil,
      known: [],
      last_ok: nil,
      last_failed: nil,
      warned_not_cluster: false
    }

    Process.send_after(self(), :tick, @tick_ms)
    # connect before the subscriber and janitor start, so their first calls find a connection
    {:ok, do_refresh(state)}
  end

  @impl true
  def handle_call({:refresh, {host, port} = hint}, _from, state)
      when is_binary(host) and host != "" and is_integer(port) do
    state =
      if state.owner == hint and state.conn != nil and Process.alive?(state.conn),
        do: state,
        else:
          connect(
            %{state | last_ok: System.monotonic_time(:millisecond), last_failed: nil},
            hint,
            true
          )

    {:reply, :ok, state}
  end

  def handle_call({:refresh, _hint}, _from, state) do
    state = if fresh?(state), do: state, else: do_refresh(state)
    {:reply, if(state.owner && state.last_ok, do: :ok, else: :error), state}
  end

  @impl true
  def handle_cast(:refresh, state) do
    {:noreply, if(fresh?(state), do: state, else: do_refresh(state))}
  end

  @impl true
  def handle_info(:tick, state) do
    if healthy?(state) do
      Process.send_after(self(), :tick, @tick_ms)
      {:noreply, state}
    else
      # look again soon: a failover usually settles within a few seconds
      state = do_refresh(state)
      Process.send_after(self(), :tick, if(healthy?(state), do: @tick_ms, else: @retry_ms))
      {:noreply, state}
    end
  end

  # Redix processes only stop when we stop them, or if they crash; a crash is
  # noticed by the next tick.
  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}
  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    stop_connections(state)
    :ok
  end

  # ---- refreshing ----

  defp fresh?(%{last_ok: t, last_failed: f}) do
    now = System.monotonic_time(:millisecond)
    (t != nil and now - t < @fresh_ms) or (f != nil and now - f < @fresh_ms)
  end

  defp healthy?(%{conn: nil}), do: false

  defp healthy?(%{conn: conn}) do
    Process.alive?(conn) and
      match?({:ok, "PONG"}, safe(fn -> Redix.command(conn, ["PING"], timeout: 1_000) end))
  end

  defp do_refresh(state) do
    now = System.monotonic_time(:millisecond)

    case find_owner(state) do
      {:ok, owner, masters} ->
        %{state | known: masters, last_ok: now, last_failed: nil} |> move_to(owner)

      :error ->
        %{state | last_failed: now, last_ok: nil}
    end
  end

  defp candidates(state) do
    url = URI.parse(state.config.redis_url)
    url_node = if url.host, do: [{url.host, url.port || 6379}], else: []

    ([state.owner] ++ url_node ++ state.config.redis_cluster_nodes ++ state.known)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.take(@max_candidates)
  end

  defp find_owner(state) do
    Enum.find_value(candidates(state), :error, fn {host, port} ->
      case ask(state, host, port) do
        {:ok, ranges} -> pick_owner(ranges, state.slot, host)
        _ -> nil
      end
    end)
  end

  # CLUSTER SLOTS -> [[from, to, [host, port | _], replica...], ...]
  defp ask(state, host, port) do
    opts =
      Keyword.merge(state.config.redis_opts,
        host: host,
        port: port,
        sync_connect: true,
        timeout: @connect_timeout
      )

    case safe(fn -> Redix.start_link(state.config.redis_url, opts) end) do
      {:ok, conn} ->
        result =
          safe(fn -> Redix.command(conn, ["CLUSTER", "SLOTS"], timeout: @command_timeout) end)

        safe(fn -> Redix.stop(conn) end)

        case result do
          {:ok, ranges} when is_list(ranges) ->
            {:ok, ranges}

          {:error, %Redix.Error{message: message}} ->
            if not state.warned_not_cluster, do: warn_not_cluster(host, port, message)
            :error

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp warn_not_cluster(host, port, message) do
    Logger.warning(
      "Socketelx Redis: #{host}:#{port} did not answer CLUSTER SLOTS (#{message}). " <>
        "Is it a Redis Cluster node? Without a cluster, remove `redis_cluster`."
    )
  end

  defp pick_owner(ranges, slot, asked_host) do
    masters = for [_from, _to, [h, p | _] | _] <- ranges, do: {host_or(h, asked_host), p}

    owner =
      Enum.find_value(ranges, fn
        [from, to, [h, p | _] | _] when from <= slot and slot <= to -> {host_or(h, asked_host), p}
        _ -> nil
      end)

    if owner, do: {:ok, owner, Enum.uniq(masters)}, else: nil
  end

  # an empty or missing host means "the node you asked"
  defp host_or(host, asked) when host in [nil, ""], do: asked
  defp host_or(host, _asked), do: host

  # ---- connections ----

  defp move_to(%{owner: owner, conn: conn} = state, owner) when conn != nil do
    if Process.alive?(conn), do: state, else: connect(state, owner, false)
  end

  defp move_to(state, owner), do: connect(state, owner, state.owner != nil)

  defp connect(state, {host, port} = owner, replacing?) do
    stop_connections(state)
    config = state.config

    opts = Keyword.merge(config.redis_opts, host: host, port: port, sync_connect: false)

    {:ok, conn} = Redix.start_link(config.redis_url, [name: Naming.redis(state.name)] ++ opts)

    {:ok, pubsub} =
      Redix.PubSub.start_link(config.redis_url, [name: Naming.redis_pubsub(state.name)] ++ opts)

    if replacing? do
      Logger.warning("Socketelx Redis Cluster: the app's keys are now served by #{host}:#{port}")
    else
      Logger.info("Socketelx Redis Cluster: using #{host}:#{port} for this app's keys")
    end

    # a new PubSub connection has no subscriptions: tell the subscriber (it ignores this if
    # it already subscribed on this very connection)
    notify_subscriber(state.name)
    %{state | owner: owner, conn: conn, pubsub: pubsub}
  end

  defp notify_subscriber(name) do
    case Process.whereis(Naming.redis_subscriber(name)) do
      nil -> :ok
      pid -> send(pid, :pubsub_replaced)
    end
  end

  defp stop_connections(%{conn: conn, pubsub: pubsub}) do
    if conn, do: safe(fn -> Redix.stop(conn) end)
    if pubsub, do: safe(fn -> Redix.PubSub.stop(pubsub) end)
    :ok
  end

  defp safe(fun) do
    fun.()
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
