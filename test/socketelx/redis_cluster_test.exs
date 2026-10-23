defmodule Socketelx.RedisClusterTest do
  @moduledoc """
  Needs a real Redis Cluster (3+ masters, each with a replica) and
  `SOCKETELX_TEST_REDIS_CLUSTER=true`, with `SOCKETELX_TEST_REDIS_URL` pointing at one node.
  Exercises what a single-node Redis never does: the app's keys moving to another node
  while clients are connected.
  """
  use ExUnit.Case, async: false
  @moduletag :redis
  @moduletag :redis_cluster
  @moduletag timeout: 60_000

  alias Socketelx.{Cluster, Naming, TestWsClient}
  alias Socketelx.Backend.Redis.Topology

  @key "k"
  @secret "s"

  setup do
    app_id = "fo#{System.unique_integer([:positive])}"
    %{app_id: app_id}
  end

  test "the topology manager connects to the node that owns the app's slot", %{app_id: app_id} do
    {name, _port} = start_node(:a, app_id)
    state = :sys.get_state(Naming.redis_topology(name))

    slot = Topology.key_slot("{socketelx:#{app_id}}:")
    assert {host, port} = state.owner
    assert owner_of(slot) == {host, port}
  end

  test "after a failover the app follows its keys and keeps working", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)
    slot = Topology.key_slot("{socketelx:#{app_id}}:")

    {ca, sid_a} = connect(a)
    {cb, sid_b} = connect(b)
    subscribe(ca, "news")
    subscribe(cb, "news")
    subscribe_presence(ca, sid_a, "presence-room", "1")
    subscribe_presence(cb, sid_b, "presence-room", "2")
    assert_receive {:ws, ^ca, %{"event" => "pusher_internal:subscription_succeeded"}}, 2_000
    flush()

    {old_master, replica} = master_and_replica(slot)

    # restore the original layout afterwards so other tests see the same cluster
    on_exit(fn -> failover(old_master) end)

    :ok = failover(replica)
    eventually(fn -> assert owner_of(slot) == replica end)

    # the state was copied to the new master: presence and counts are intact...
    eventually(fn ->
      assert [{"1", _}, {"2", _}] = Cluster.presence_members(elem(a, 0), "presence-room")
      assert Cluster.subscription_count(elem(a, 0), "news") == 2
    end)

    # ...and an event published through one node still reaches clients on both
    eventually(fn ->
      assert {200, _} = trigger(b, app_id, "news", "after-failover", "{}")
      assert_receive {:ws, ^ca, %{"event" => "after-failover"}}, 500
      assert_receive {:ws, ^cb, %{"event" => "after-failover"}}, 500
    end)

    # a new member joining after the failover is announced across nodes
    {cc, sid_c} = connect(a)
    subscribe_presence(cc, sid_c, "presence-room", "3")

    eventually(fn ->
      assert_receive {:ws, ^cb, %{"event" => "pusher_internal:member_added", "data" => d}}, 500
      assert %{"user_id" => "3"} = Jason.decode!(d)
    end)
  end

  # ---- cluster helpers ----

  defp redis_url, do: System.fetch_env!("SOCKETELX_TEST_REDIS_URL")

  defp slot_ranges do
    {:ok, conn} = Redix.start_link(redis_url())
    ranges = Redix.command!(conn, ["CLUSTER", "SLOTS"])
    Redix.stop(conn)
    ranges
  end

  defp owner_of(slot) do
    Enum.find_value(slot_ranges(), fn
      [from, to, [host, port | _] | _] when from <= slot and slot <= to -> {host, port}
      _ -> nil
    end)
  end

  defp master_and_replica(slot) do
    Enum.find_value(slot_ranges(), fn
      [from, to, [mh, mp | _], [rh, rp | _] | _] when from <= slot and slot <= to ->
        {{mh, mp}, {rh, rp}}

      _ ->
        nil
    end)
  end

  # Asks that node (a replica) to take over from its master, gracefully.
  defp failover({host, port}) do
    {:ok, conn} = Redix.start_link(redis_url(), host: host, port: port)
    result = Redix.command(conn, ["CLUSTER", "FAILOVER"])
    Redix.stop(conn)
    if match?({:ok, "OK"}, result), do: :ok, else: result
  end

  # ---- app helpers ----

  defp eventually(fun, timeout \\ 15_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    fun.()
  rescue
    error in [ExUnit.AssertionError, MatchError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(250)
        do_eventually(fun, deadline)
      else
        reraise error, __STACKTRACE__
      end
  end

  defp flush do
    receive do
      {:ws, _, _} -> flush()
    after
      200 -> :ok
    end
  end

  defp start_node(label, app_id) do
    name = :"cl#{label}_#{System.unique_integer([:positive])}"

    start_supervised!(
      Supervisor.child_spec(
        {Socketelx,
         name: name,
         port: 0,
         app_id: app_id,
         app_key: @key,
         app_secret: @secret,
         redis_url: redis_url(),
         redis_cluster: true},
        id: name,
        restart: :temporary
      )
    )

    {name, :ranch.get_port(Naming.listener(name))}
  end

  defp connect({_name, port}) do
    {:ok, client} = TestWsClient.start(port, @key)
    assert_receive {:ws, ^client, %{"event" => "pusher:connection_established", "data" => data}}
    {client, Jason.decode!(data)["socket_id"]}
  end

  defp subscribe(client, channel) do
    TestWsClient.send_json(client, %{event: "pusher:subscribe", data: %{channel: channel}})

    assert_receive {:ws, ^client,
                    %{"event" => "pusher_internal:subscription_succeeded", "channel" => ^channel}}
  end

  # the caller asserts on the subscription_succeeded reply (it carries the member list)
  defp subscribe_presence(client, socket_id, channel, user_id) do
    channel_data = Jason.encode!(%{user_id: user_id, user_info: %{name: "user #{user_id}"}})
    auth = "#{@key}:#{sign("#{socket_id}:#{channel}:#{channel_data}")}"

    TestWsClient.send_json(client, %{
      event: "pusher:subscribe",
      data: %{channel: channel, auth: auth, channel_data: channel_data}
    })
  end

  defp sign(string),
    do: :crypto.mac(:hmac, :sha256, @secret, string) |> Base.encode16(case: :lower)

  # What a Pusher server library sends: signed POST /apps/:id/events
  defp trigger({_name, port}, app_id, channel, event, data, socket_id \\ nil) do
    path = "/apps/#{app_id}/events"

    body =
      %{name: event, channels: [channel], data: data}
      |> then(&if(socket_id, do: Map.put(&1, :socket_id, socket_id), else: &1))
      |> Jason.encode!()

    md5 = :crypto.hash(:md5, body) |> Base.encode16(case: :lower)
    http(:post, port, path, %{"body_md5" => md5}, body)
  end

  defp http(method, port, path, extra, body) do
    params =
      Map.merge(
        %{
          "auth_key" => @key,
          "auth_timestamp" => to_string(System.system_time(:second)),
          "auth_version" => "1.0"
        },
        extra
      )

    query = params |> Enum.sort() |> Enum.map_join("&", fn {k, v} -> "#{k}=#{v}" end)
    signature = sign("#{String.upcase(to_string(method))}\n#{path}\n#{query}")
    url = ~c"http://127.0.0.1:#{port}#{path}?#{query}&auth_signature=#{signature}"

    request = if method == :post, do: {url, [], ~c"application/json", body}, else: {url, []}
    {:ok, {{_, status, _}, _headers, resp}} = :httpc.request(method, request, [], [])
    {status, if(resp == [], do: %{}, else: Jason.decode!(to_string(resp)))}
  end
end
