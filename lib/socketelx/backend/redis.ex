defmodule Socketelx.Backend.Redis do
  @moduledoc """
  Redis backend: lets several `socketelx` nodes behave as one.

  ## What lives in Redis

  | Concern | How |
  |---|---|
  | Broadcasts | One pub/sub channel per app (`<prefix>:events`). A node delivers to its own sockets first, then `PUBLISH`es; every other node's subscriber re-delivers locally. Each message carries the sender's `node_id` so a node ignores its own. |
  | Presence | Hash per channel (`<prefix>:presence:<channel>`, `socket_id -> {user, info, node}`). A Lua script decides atomically whether a join is the user's *first* connection (or a leave their *last*) across all nodes, so `member_added` / `member_removed` fire exactly once cluster-wide. |
  | Occupancy | Hash per channel (`<prefix>:subs:<channel>`, `node_id -> local subscriber count`) so `channel_occupied` / `channel_vacated` fire once cluster-wide and subscription counts are cluster-wide. |
  | Liveness | Each node refreshes a TTL key (`<prefix>:node:<node_id>`). A janitor (see `Socketelx.Backend.Redis.Janitor`) removes presence/occupancy left behind by nodes whose key expired. |

  ## Failure behaviour

  A Redis error never crashes a websocket connection. Operations log and
  fall back to node-local behaviour, so during a Redis outage each node
  keeps serving its own clients (cross-node delivery pauses). Pub/sub is
  at-most-once, like Pusher itself: messages published while a node is
  disconnected from Redis are not replayed.

  ## Redis Cluster

  Set `redis_cluster: true` (`SOCKETELX_REDIS_CLUSTER=true`) and point the Redis URL at
  any node of the cluster. Every key of an app then carries the same hash tag, so they
  live in one slot, on one node: the Lua scripts keep working, and the app follows
  that slot through failovers and resharding (see `Socketelx.Backend.Redis.Topology`).
  Different apps get different tags and may land on different nodes. Broadcasts use
  ordinary pub/sub, which Redis Cluster propagates to every node.

  Without `redis_cluster`, use a single Redis endpoint: standalone, Sentinel, or a
  managed primary/replica service all work.
  """
  @behaviour Socketelx.Backend

  require Logger
  alias Socketelx.{Config, Naming}

  @timeout 2_000
  @node_ttl_ms 30_000

  # ---- Lua (run atomically inside Redis) ----

  # KEYS: presence hash, presence-channels set
  # ARGV: socket_id, entry json, user_id, channel   -> 1 if first connection for user, else 0
  @presence_add """
  local seen = 0
  for _, v in ipairs(redis.call('HVALS', KEYS[1])) do
    local ok, e = pcall(cjson.decode, v)
    if ok and tostring(e.u) == ARGV[3] then
      seen = 1
      break
    end
  end
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  redis.call('SADD', KEYS[2], ARGV[4])
  return 1 - seen
  """

  # KEYS: presence hash, presence-channels set
  # ARGV: socket_id, channel   -> {1|0|-1, user_id}   (1 = last connection, 0 = user still present, -1 = unknown socket)
  @presence_remove """
  local v = redis.call('HGET', KEYS[1], ARGV[1])
  if not v then
    return {-1, ''}
  end
  redis.call('HDEL', KEYS[1], ARGV[1])
  local ok, e = pcall(cjson.decode, v)
  local uid = ''
  if ok then uid = tostring(e.u) end
  local last = 1
  for _, o in ipairs(redis.call('HVALS', KEYS[1])) do
    local ok2, e2 = pcall(cjson.decode, o)
    if ok2 and tostring(e2.u) == uid then
      last = 0
      break
    end
  end
  if redis.call('HLEN', KEYS[1]) == 0 then
    redis.call('SREM', KEYS[2], ARGV[2])
  end
  return {last, uid}
  """

  # KEYS: subs hash, subs-channels set;  ARGV: node_id, channel  -> 1 if channel just became occupied
  @channel_join """
  redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
  redis.call('SADD', KEYS[2], ARGV[2])
  local total = 0
  for _, c in ipairs(redis.call('HVALS', KEYS[1])) do
    total = total + tonumber(c)
  end
  if total == 1 then return 1 end
  return 0
  """

  # KEYS: subs hash, subs-channels set;  ARGV: node_id, channel  -> 1 if channel just became vacant
  @channel_leave """
  local n = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
  if n <= 0 then
    redis.call('HDEL', KEYS[1], ARGV[1])
  end
  if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('SREM', KEYS[2], ARGV[2])
    return 1
  end
  return 0
  """

  # KEYS: subs hash, subs-channels set;  ARGV: channel, dead_node...  -> 1 if channel is now vacant
  @subs_reap """
  for i = 2, #ARGV do
    redis.call('HDEL', KEYS[1], ARGV[i])
  end
  if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('SREM', KEYS[2], ARGV[1])
    return 1
  end
  return 0
  """

  # ---- Backend callbacks ----

  @impl true
  def child_specs(name, %{redis_cluster: true}) do
    [
      %{
        id: {:redis_topology, name},
        start: {__MODULE__.Topology, :start_link, [name]},
        type: :worker
      },
      subscriber_spec(name),
      janitor_spec(name)
    ]
  end

  def child_specs(name, config) do
    redix_opts = [sync_connect: false] ++ config.redis_opts

    [
      %{
        id: {:redis, name},
        start: {Redix, :start_link, [config.redis_url, [name: Naming.redis(name)] ++ redix_opts]},
        type: :worker
      },
      %{
        id: {:redis_pubsub, name},
        start:
          {Redix.PubSub, :start_link,
           [config.redis_url, [name: Naming.redis_pubsub(name)] ++ redix_opts]},
        type: :worker
      },
      subscriber_spec(name),
      janitor_spec(name)
    ]
  end

  defp subscriber_spec(name),
    do: %{
      id: {:redis_subscriber, name},
      start: {__MODULE__.Subscriber, :start_link, [name]},
      type: :worker
    }

  defp janitor_spec(name),
    do: %{
      id: {:redis_janitor, name},
      start: {__MODULE__.Janitor, :start_link, [name]},
      type: :worker,
      shutdown: 5_000
    }

  @impl true
  def publish(name, channel, encoded, exclude_socket_id) do
    config = Config.get(name)
    payload = encode_envelope(config.node_id, channel, encoded, exclude_socket_id)

    case command(name, ["PUBLISH", events_channel(config), payload]) do
      {:ok, _receivers} -> :ok
      {:error, reason} -> log_error("publish to #{channel}", reason)
    end
  end

  @impl true
  def channel_joined(name, channel) do
    config = Config.get(name)

    case command(name, [
           "EVAL",
           @channel_join,
           "2",
           subs_key(config, channel),
           subs_channels_key(config),
           config.node_id,
           channel
         ]) do
      {:ok, 1} -> true
      {:ok, _} -> false
      {:error, reason} -> log_error("channel_joined #{channel}", reason, false)
    end
  end

  @impl true
  def channel_left(name, channel) do
    config = Config.get(name)

    case command(name, [
           "EVAL",
           @channel_leave,
           "2",
           subs_key(config, channel),
           subs_channels_key(config),
           config.node_id,
           channel
         ]) do
      {:ok, 1} -> true
      {:ok, _} -> false
      {:error, reason} -> log_error("channel_left #{channel}", reason, false)
    end
  end

  @impl true
  def subscription_count(name, channel) do
    config = Config.get(name)

    case command(name, ["HVALS", subs_key(config, channel)]) do
      {:ok, counts} -> counts |> Enum.map(&String.to_integer/1) |> Enum.sum()
      {:error, reason} -> log_error("subscription_count #{channel}", reason, 0)
    end
  end

  @impl true
  def list_channels(name) do
    config = Config.get(name)

    case command(name, ["SMEMBERS", subs_channels_key(config)]) do
      {:ok, channels} ->
        channels

      {:error, reason} ->
        # Redis is down: the best we can say is what this node knows.
        log_error("list_channels", reason, Socketelx.Backend.Local.local_channels(name))
    end
  end

  @impl true
  def presence_add(name, channel, socket_id, user_id, user_info) do
    config = Config.get(name)
    entry = Jason.encode!(%{u: user_id, i: user_info, n: config.node_id})

    case command(name, [
           "EVAL",
           @presence_add,
           "2",
           presence_key(config, channel),
           presence_channels_key(config),
           socket_id,
           entry,
           user_id,
           channel
         ]) do
      {:ok, 1} -> true
      {:ok, _} -> false
      {:error, reason} -> log_error("presence_add #{channel}", reason, true)
    end
  end

  @impl true
  def presence_remove(name, channel, socket_id) do
    config = Config.get(name)

    case command(name, [
           "EVAL",
           @presence_remove,
           "2",
           presence_key(config, channel),
           presence_channels_key(config),
           socket_id,
           channel
         ]) do
      {:ok, [1, user_id]} -> {true, user_id}
      {:ok, [0, user_id]} -> {false, user_id}
      {:ok, _unknown} -> {false, nil}
      {:error, reason} -> log_error("presence_remove #{channel}", reason, {false, nil})
    end
  end

  @impl true
  def presence_members(name, channel) do
    config = Config.get(name)

    case command(name, ["HVALS", presence_key(config, channel)]) do
      {:ok, values} ->
        values
        |> Enum.flat_map(fn json ->
          case Jason.decode(json) do
            {:ok, %{"u" => user_id} = entry} -> [{user_id, entry["i"] || %{}}]
            _ -> []
          end
        end)
        |> Enum.uniq_by(fn {user_id, _info} -> user_id end)
        |> Enum.sort_by(fn {user_id, _info} -> user_id end)

      {:error, reason} ->
        log_error("presence_members #{channel}", reason, [])
    end
  end

  @impl true
  def presence_restore(name, channel, socket_id, user_id, user_info) do
    config = Config.get(name)
    entry = Jason.encode!(%{u: user_id, i: user_info, n: config.node_id})

    # HSET overwrites the same field, so running this twice changes nothing.
    case pipeline(name, [
           ["HSET", presence_key(config, channel), socket_id, entry],
           ["SADD", presence_channels_key(config), channel]
         ]) do
      {:ok, _} -> :ok
      {:error, reason} -> log_error("presence_restore #{channel}", reason)
    end
  end

  @doc """
  Re-publishes this node's live state into Redis after Redis lost it (a
  restart, a flush, a failover to an empty replica) or after this node was
  cut off from Redis for a while. Called by the janitor.

  Channel occupancy is rebuilt here in one pipeline, by *setting* this
  node's subscriber count per channel to what the local `Registry` holds —
  an overwrite, not an increment, so calling it again is harmless. Presence
  entries need per-socket user info that only the sockets have, so each
  socket is asked to restore its own (spread over a couple of seconds so
  thousands of sockets don't hit a freshly restarted Redis at once).

  Nothing is announced: clients were already told about these members, and
  `member_added` for them again would be wrong.
  """
  def resync(name) do
    config = Config.get(name)
    registry = Naming.registry(name)

    counts =
      registry
      |> Registry.select([{{:"$1", :_, :_}, [], [:"$1"]}])
      |> Enum.reject(&(&1 == :__all_connections__))
      |> Enum.frequencies()

    commands =
      Enum.flat_map(counts, fn {channel, count} ->
        [
          ["HSET", subs_key(config, channel), config.node_id, Integer.to_string(count)],
          ["SADD", subs_channels_key(config), channel]
        ]
      end)

    commands
    |> Enum.chunk_every(500)
    |> Enum.each(fn chunk ->
      case pipeline(name, chunk) do
        {:ok, _} -> :ok
        {:error, reason} -> log_error("resync occupancy", reason)
      end
    end)

    Registry.dispatch(registry, :__all_connections__, fn entries ->
      for {pid, _socket_id} <- entries, do: send(pid, :resync_presence)
    end)

    Logger.info("Socketelx Redis backend: resynced #{map_size(counts)} channel(s) into Redis")
    :ok
  end

  # ---- shared helpers (used by Subscriber and Janitor too) ----

  @doc false
  def command(name, command) do
    run(name, fn -> Redix.command(Naming.redis(name), command, timeout: @timeout) end)
  end

  @doc false
  def pipeline(name, commands) do
    run(name, fn -> Redix.pipeline(Naming.redis(name), commands, timeout: @timeout) end)
  end

  # Runs one Redis call. Single node: just the call. Cluster mode: a redirect answer
  # (MOVED and friends) means the command did NOT run, so it is safe to ask the topology
  # manager for the new owner and run it once more. A broken connection may or may not
  # have run it, so it is not retried (a retry could count a join twice); the manager is
  # only told to look for the new owner so the next call works.
  defp run(name, fun) do
    result = safe(fun)

    if Config.get(name).redis_cluster do
      cond do
        redirected?(result) ->
          case __MODULE__.Topology.refresh(name, moved_hint(result)) do
            :ok -> safe(fun)
            :error -> result
          end

        match?({:error, %Redix.ConnectionError{}}, result) or match?({:error, {:exit, _}}, result) ->
          __MODULE__.Topology.refresh_async(name)
          result

        true ->
          result
      end
    else
      result
    end
  end

  defp safe(fun) do
    fun.()
  catch
    # the Redix process isn't running (e.g. during shutdown)
    :exit, reason -> {:error, {:exit, reason}}
  end

  @redirects ["MOVED", "ASK", "CLUSTERDOWN", "READONLY", "TRYAGAIN"]

  # "MOVED 5863 127.0.0.1:7006" -> {"127.0.0.1", 7006}
  defp moved_hint({:error, %Redix.Error{message: m}}), do: parse_moved(m)

  defp moved_hint({:ok, results}) when is_list(results) do
    Enum.find_value(results, fn
      %Redix.Error{message: m} -> parse_moved(m)
      _ -> nil
    end)
  end

  defp moved_hint(_), do: nil

  defp parse_moved("MOVED " <> rest) do
    with [_slot, address] <- String.split(rest, " ", parts: 2),
         [port | host_parts] when host_parts != [] <-
           address |> String.split(":") |> Enum.reverse(),
         {port, ""} <- Integer.parse(port),
         host when host != "" <- host_parts |> Enum.reverse() |> Enum.join(":") do
      {host, port}
    else
      _ -> nil
    end
  end

  defp parse_moved(_), do: nil

  defp redirected?({:error, %Redix.Error{message: m}}), do: redirect_message?(m)

  defp redirected?({:ok, results}) when is_list(results),
    do: Enum.any?(results, &redirect_result?/1)

  defp redirected?(_), do: false

  defp redirect_result?(%Redix.Error{message: m}), do: redirect_message?(m)
  defp redirect_result?(_), do: false

  defp redirect_message?(m), do: String.starts_with?(m, @redirects)

  @doc false
  def node_ttl_ms, do: @node_ttl_ms

  @doc false
  def subs_reap_script, do: @subs_reap

  @doc false
  def presence_key(config, channel), do: "#{config.prefix}:presence:#{channel}"
  @doc false
  def presence_channels_key(config), do: "#{config.prefix}:presence_channels"
  @doc false
  def subs_key(config, channel), do: "#{config.prefix}:subs:#{channel}"
  @doc false
  def subs_channels_key(config), do: "#{config.prefix}:subs_channels"
  @doc false
  def node_key(config, node_id), do: "#{config.prefix}:node:#{node_id}"
  @doc false
  def events_channel(config), do: "#{config.prefix}:events"
  @doc false
  def lock_key(config), do: "#{config.prefix}:reaper_lock"

  @doc false
  def encode_envelope(node_id, channel, encoded, exclude_socket_id) do
    Jason.encode!(%{n: node_id, c: channel, m: encoded, x: exclude_socket_id})
  end

  @doc false
  def decode_envelope(payload) do
    case Jason.decode(payload) do
      {:ok, %{"n" => node, "c" => channel, "m" => message} = envelope}
      when is_binary(node) and is_binary(channel) and is_binary(message) ->
        {:ok, %{node: node, channel: channel, message: message, exclude: envelope["x"]}}

      _ ->
        :error
    end
  end

  @doc false
  def log_error(what, reason, default \\ :ok) do
    Logger.warning("Socketelx Redis backend: #{what} failed: #{inspect(reason)}")
    default
  end
end
