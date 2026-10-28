defmodule Socketelx.ClusterRedisTest do
  @moduledoc """
  Two (or three) real `Socketelx` nodes in one BEAM, sharing one real Redis,
  driven the way production drives them: websocket clients over TCP and
  Laravel-style signed REST calls. Run with `SOCKETELX_TEST_REDIS_URL` set. Also
  set `SOCKETELX_TEST_REDIS_CLUSTER=true` (and point the URL at any node of a Redis
  Cluster) to run the very same tests through the Redis Cluster code path.
  """
  use ExUnit.Case, async: false

  @moduletag :redis

  alias Socketelx.{Cluster, Config, Naming, TestWsClient}
  alias Socketelx.Backend.Redis
  alias Socketelx.Backend.Redis.Janitor

  @key "k"
  @secret "s"

  setup do
    app_id = "it#{System.unique_integer([:positive])}"
    on_exit(fn -> flush_app(app_id) end)
    %{app_id: app_id}
  end

  # ---- broadcasting ----

  test "an event triggered through node B reaches subscribers on node A and B exactly once", %{
    app_id: app_id
  } do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {ca, _} = connect(a)
    {cb, _} = connect(b)
    subscribe(ca, "news")
    subscribe(cb, "news")

    assert {200, _} = trigger(b, app_id, "news", "hello", ~s({"n":1}))

    assert_receive {:ws, ^ca, %{"event" => "hello", "channel" => "news", "data" => ~s({"n":1})}}
    assert_receive {:ws, ^cb, %{"event" => "hello", "channel" => "news"}}
    refute_receive {:ws, _, %{"event" => "hello"}}, 300
  end

  test "the channel list and batch events are cluster-wide", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {ca, _} = connect(a)
    {cb, _} = connect(b)
    subscribe(ca, "only-on-a")
    subscribe(cb, "only-on-b")

    # ask node A: it must also know about the channel that lives on node B
    assert {200, %{"channels" => channels}} =
             http(:get, elem(a, 1), "/apps/#{app_id}/channels", %{}, nil)

    assert channels |> Map.keys() |> Enum.sort() == ["only-on-a", "only-on-b"]

    # a batch sent through node A reaches clients on both nodes
    body =
      Jason.encode!(%{
        batch: [
          %{channel: "only-on-a", name: "x", data: "{}"},
          %{channel: "only-on-b", name: "y", data: "{}"}
        ]
      })

    md5 = :crypto.hash(:md5, body) |> Base.encode16(case: :lower)

    assert {200, _} =
             http(:post, elem(a, 1), "/apps/#{app_id}/batch_events", %{"body_md5" => md5}, body)

    assert_receive {:ws, ^ca, %{"event" => "x"}}
    assert_receive {:ws, ^cb, %{"event" => "y"}}
  end

  test "terminating a user's connections through node B closes their socket on node A", %{
    app_id: app_id
  } do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {ca, sid_a} = connect(a)
    {cb, sid_b} = connect(b)

    for {client, sid, id} <- [{ca, sid_a, "7"}, {cb, sid_b, "8"}] do
      %{auth: auth, user_data: user_data} = Socketelx.authenticate_user(n(a), sid, %{id: id})

      TestWsClient.send_json(client, %{
        event: "pusher:signin",
        data: %{auth: auth, user_data: user_data}
      })

      assert_receive {:ws, ^client, %{"event" => "pusher:signin_success"}}
    end

    assert {200, _} =
             http(
               :post,
               elem(b, 1),
               "/apps/#{app_id}/users/7/terminate_connections",
               %{
                 "body_md5" => :crypto.hash(:md5, "{}") |> Base.encode16(case: :lower)
               },
               "{}"
             )

    assert_receive {:ws_closed, ^ca}, 2_000
    refute_receive {:ws_closed, ^cb}, 300
  end

  test "exclude_socket_id is honoured for sockets on other nodes", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {ca, sid_a} = connect(a)
    {cb, _} = connect(b)
    subscribe(ca, "news")
    subscribe(cb, "news")

    assert {200, _} = trigger(b, app_id, "news", "hello", "{}", sid_a)

    assert_receive {:ws, ^cb, %{"event" => "hello"}}
    refute_receive {:ws, ^ca, %{"event" => "hello"}}, 300
  end

  test "an event for a channel nobody is on is accepted and goes nowhere", %{app_id: app_id} do
    a = start_node(:a, app_id)
    _b = start_node(:b, app_id)
    {ca, _} = connect(a)
    subscribe(ca, "other")

    assert {200, _} = trigger(a, app_id, "empty-channel", "hello", "{}")
    refute_receive {:ws, _, %{"event" => "hello"}}, 200
  end

  test "client events cross nodes and skip the sender", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {ca, sid_a} = connect(a)
    {cb, sid_b} = connect(b)
    subscribe_private(ca, sid_a, "private-chat")
    subscribe_private(cb, sid_b, "private-chat")

    TestWsClient.send_json(ca, %{
      event: "client-typing",
      channel: "private-chat",
      data: %{who: "a"}
    })

    assert_receive {:ws, ^cb, %{"event" => "client-typing", "channel" => "private-chat"}}
    refute_receive {:ws, ^ca, %{"event" => "client-typing"}}, 300
  end

  test "private channel auth is enforced on every node", %{app_id: app_id} do
    b = start_node(:b, app_id)
    {cb, _sid} = connect(b)

    TestWsClient.send_json(cb, %{
      event: "pusher:subscribe",
      data: %{channel: "private-x", auth: "k:deadbeef"}
    })

    assert_receive {:ws, ^cb, %{"event" => "pusher:error", "data" => %{"code" => 4009}}}
  end

  # ---- presence ----

  test "presence is cluster-wide: member_added/removed fire once, per user, not per connection",
       %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    # user 1 joins via node A
    {u1a, sid} = connect(a)
    subscribe_presence(u1a, sid, "presence-room", "1")

    assert_receive {:ws, ^u1a,
                    %{"event" => "pusher_internal:subscription_succeeded", "data" => d1}}

    assert %{"presence" => %{"count" => 1, "ids" => ["1"]}} = Jason.decode!(d1)

    # user 2 joins via node B and sees user 1; user 1 is told about user 2 (across nodes)
    {u2b, sid2} = connect(b)
    subscribe_presence(u2b, sid2, "presence-room", "2")

    assert_receive {:ws, ^u2b,
                    %{"event" => "pusher_internal:subscription_succeeded", "data" => d2}}

    assert %{"presence" => %{"count" => 2, "ids" => ["1", "2"]}} = Jason.decode!(d2)
    assert_receive {:ws, ^u1a, %{"event" => "pusher_internal:member_added", "data" => added}}
    assert %{"user_id" => "2"} = Jason.decode!(added)

    # user 1 opens a second tab on node B: not a new member
    {u1b, sid3} = connect(b)
    subscribe_presence(u1b, sid3, "presence-room", "1")

    assert_receive {:ws, ^u1b,
                    %{"event" => "pusher_internal:subscription_succeeded", "data" => d3}}

    assert %{"presence" => %{"count" => 2}} = Jason.decode!(d3)
    refute_receive {:ws, ^u2b, %{"event" => "pusher_internal:member_added"}}, 300

    # closing one of user 1's two tabs must not announce a departure...
    TestWsClient.kill(u1a)
    refute_receive {:ws, ^u2b, %{"event" => "pusher_internal:member_removed"}}, 500

    # ...closing the last one does, and it reaches the other node
    TestWsClient.kill(u1b)

    assert_receive {:ws, ^u2b, %{"event" => "pusher_internal:member_removed", "data" => removed}},
                   2_000

    assert %{"user_id" => "1"} = Jason.decode!(removed)

    assert [{"2", _}] = Cluster.presence_members(n(a), "presence-room")
  end

  test "the channel info endpoint on one node sees subscribers on another", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    assert {200, %{"occupied" => false, "subscription_count" => 0}} =
             channel_info(a, app_id, "news")

    {cb, _} = connect(b)
    subscribe(cb, "news")

    assert {200, %{"occupied" => true, "subscription_count" => 1}} =
             channel_info(a, app_id, "news")

    {u, sid} = connect(b)
    subscribe_presence(u, sid, "presence-room", "9")
    assert_receive {:ws, ^u, %{"event" => "pusher_internal:subscription_succeeded"}}
    assert {200, %{"user_count" => 1}} = channel_info(a, app_id, "presence-room")
  end

  # ---- occupancy ----

  test "channel_occupied fires on the first subscriber cluster-wide, channel_vacated on the last",
       %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {:ok, pa} = spawn_member(a, "chan", "s-a")
    assert Cluster.channel_joined(n(a), "chan")

    {:ok, pb} = spawn_member(b, "chan", "s-b")
    refute Cluster.channel_joined(n(b), "chan")

    assert Cluster.subscription_count(n(a), "chan") == 2
    assert Cluster.subscription_count(n(b), "chan") == 2

    stop_member(pa)
    refute Cluster.channel_left(n(a), "chan")
    assert Cluster.subscription_count(n(b), "chan") == 1

    stop_member(pb)
    assert Cluster.channel_left(n(b), "chan")
    assert Cluster.subscription_count(n(a), "chan") == 0
  end

  # ---- failure handling ----

  test "state left behind by a dead node is reaped and announced to survivors", %{app_id: app_id} do
    a = start_node(:a, app_id)
    {b, b_sup} = start_node_with_pid(:b, app_id)

    {u1, sid1} = connect(a)
    subscribe_presence(u1, sid1, "presence-room", "1")
    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:subscription_succeeded"}}

    {u2, sid2} = connect(b)
    subscribe_presence(u2, sid2, "presence-room", "2")
    assert_receive {:ws, ^u2, %{"event" => "pusher_internal:subscription_succeeded"}}
    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:member_added"}}

    {cb, _} = connect(b)
    subscribe(cb, "only-on-b")
    assert Cluster.subscription_count(n(a), "only-on-b") == 1

    # node B dies without cleaning up (its sockets never run terminate/3)...
    Process.exit(b_sup, :kill)
    assert_receive {:ws_closed, ^u2}, 2_000

    # ...and its heartbeat key expires
    Redis.command(n(a), ["DEL", Redis.node_key(Config.get(n(b)), Config.get(n(b)).node_id)])

    assert [{"1", _}, {"2", _}] = Cluster.presence_members(n(a), "presence-room")
    assert Cluster.subscription_count(n(a), "only-on-b") == 1

    Janitor.reap(n(a))

    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:member_removed", "data" => removed}},
                   2_000

    assert %{"user_id" => "2"} = Jason.decode!(removed)
    assert [{"1", _}] = Cluster.presence_members(n(a), "presence-room")
    assert Cluster.subscription_count(n(a), "only-on-b") == 0
  end

  test "after Redis loses all its data, live nodes write their state back and leaves still work",
       %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {u1, sid1} = connect(a)
    subscribe_presence(u1, sid1, "presence-room", "1")
    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:subscription_succeeded"}}

    {u2, sid2} = connect(b)
    subscribe_presence(u2, sid2, "presence-room", "2")
    assert_receive {:ws, ^u2, %{"event" => "pusher_internal:subscription_succeeded"}}
    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:member_added"}}

    {cb, _} = connect(b)
    subscribe(cb, "news")

    # Redis restarts empty: every key, including the nodes' heartbeats, is gone
    flush_app(app_id)
    assert Cluster.presence_members(n(a), "presence-room") == []
    assert Cluster.subscription_count(n(a), "news") == 0

    # the janitors notice on their next tick (triggered here instead of waiting 10s)
    tick(n(a))
    tick(n(b))

    eventually(fn ->
      assert [{"1", _}, {"2", _}] = Cluster.presence_members(n(a), "presence-room")
      assert Cluster.subscription_count(n(a), "news") == 1
      assert Cluster.subscription_count(n(a), "presence-room") == 2
    end)

    # restoring is silent: nobody is told a member "joined" a second time
    refute_receive {:ws, _, %{"event" => "pusher_internal:member_added"}}, 300

    # and the restored state is real: user 2 leaving is announced across nodes again
    TestWsClient.kill(u2)

    assert_receive {:ws, ^u1, %{"event" => "pusher_internal:member_removed", "data" => removed}},
                   2_000

    assert %{"user_id" => "2"} = Jason.decode!(removed)
  end

  test "resync is idempotent: running it again does not double-count", %{app_id: app_id} do
    a = start_node(:a, app_id)
    {c1, _} = connect(a)
    {c2, _} = connect(a)
    subscribe(c1, "news")
    subscribe(c2, "news")
    assert Cluster.subscription_count(n(a), "news") == 2

    Redis.resync(n(a))
    Redis.resync(n(a))

    assert Cluster.subscription_count(n(a), "news") == 2
  end

  test "a live node is never reaped", %{app_id: app_id} do
    a = start_node(:a, app_id)
    b = start_node(:b, app_id)

    {u2, sid2} = connect(b)
    subscribe_presence(u2, sid2, "presence-room", "2")
    assert_receive {:ws, ^u2, %{"event" => "pusher_internal:subscription_succeeded"}}

    Janitor.reap(n(a))

    assert [{"2", _}] = Cluster.presence_members(n(a), "presence-room")
  end

  test "a node whose Redis is unreachable keeps serving its own clients" do
    name = :"nodeX_#{System.unique_integer([:positive])}"

    start_supervised!(
      Supervisor.child_spec(
        {Socketelx,
         name: name,
         port: 0,
         app_id: "down",
         app_key: @key,
         app_secret: @secret,
         redis_url: "redis://127.0.0.1:1",
         redis_cluster: cluster?()},
        id: name
      )
    )

    node = {name, :ranch.get_port(Naming.listener(name))}
    {c, sid} = connect(node)

    subscribe(c, "news")
    subscribe_presence(c, sid, "presence-room", "1")

    assert_receive {:ws, ^c,
                    %{
                      "event" => "pusher_internal:subscription_succeeded",
                      "channel" => "presence-room"
                    }},
                   3_000

    assert {200, _} = trigger(node, "down", "news", "hello", "{}")
    assert_receive {:ws, ^c, %{"event" => "hello"}}, 3_000
  end

  # ---- helpers ----

  defp n({name, _port}), do: name

  # run the janitor's periodic work now instead of waiting for its 10s timer
  defp tick(name), do: send(Process.whereis(Naming.redis_janitor(name)), :tick)

  defp eventually(fun, timeout \\ 6_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline)
  end

  defp do_eventually(fun, deadline) do
    fun.()
  rescue
    error in [ExUnit.AssertionError, MatchError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(100)
        do_eventually(fun, deadline)
      else
        reraise error, __STACKTRACE__
      end
  end

  defp redis_url, do: System.fetch_env!("SOCKETELX_TEST_REDIS_URL")
  defp cluster?, do: System.get_env("SOCKETELX_TEST_REDIS_CLUSTER") in ["true", "1"]

  defp start_node(label, app_id), do: label |> start_node_with_pid(app_id) |> elem(0)

  # returns {{name, port}, supervisor_pid}
  defp start_node_with_pid(label, app_id) do
    name = :"node#{label}_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        Supervisor.child_spec(
          {Socketelx,
           name: name,
           port: 0,
           app_id: app_id,
           app_key: @key,
           app_secret: @secret,
           redis_url: redis_url(),
           redis_cluster: cluster?()},
          id: name,
          restart: :temporary
        )
      )

    {{name, :ranch.get_port(Naming.listener(name))}, pid}
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

  defp subscribe_private(client, socket_id, channel) do
    auth = "#{@key}:#{sign("#{socket_id}:#{channel}")}"

    TestWsClient.send_json(client, %{
      event: "pusher:subscribe",
      data: %{channel: channel, auth: auth}
    })

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

  defp channel_info({_name, port}, app_id, channel),
    do: http(:get, port, "/apps/#{app_id}/channels/#{channel}", %{}, nil)

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

  # A process that holds a Registry entry, like a connected websocket does.
  defp spawn_member({name, _port}, channel, socket_id) do
    parent = self()

    pid =
      spawn_link(fn ->
        Registry.register(Naming.registry(name), channel, socket_id)
        send(parent, :registered)

        receive do
          :stop ->
            Registry.unregister(Naming.registry(name), channel)
            send(parent, :unregistered)
        end
      end)

    assert_receive :registered
    {:ok, pid}
  end

  defp stop_member(pid) do
    send(pid, :stop)
    assert_receive :unregistered
  end

  defp flush_app(app_id) do
    {:ok, conn} = Redix.start_link(redis_url())

    # every master holds part of the keyspace in cluster mode; a plain Redis is its own master
    masters =
      if cluster?() do
        for [_from, _to, [host, port | _] | _] <- Redix.command!(conn, ["CLUSTER", "SLOTS"]),
            uniq: true,
            do: {host, port}
      else
        [nil]
      end

    for master <- masters do
      {:ok, node} =
        case master do
          nil -> Redix.start_link(redis_url())
          {host, port} -> Redix.start_link(redis_url(), host: host, port: port)
        end

      prefix = if cluster?(), do: "{socketelx:#{app_id}}", else: "socketelx:#{app_id}"
      scan_and_delete(node, "0", prefix <> ":*")
      Redix.stop(node)
    end

    Redix.stop(conn)
  end

  defp scan_and_delete(conn, cursor, pattern) do
    [next, keys] = Redix.command!(conn, ["SCAN", cursor, "MATCH", pattern, "COUNT", "1000"])
    Enum.each(keys, &Redix.command!(conn, ["DEL", &1]))
    if next != "0", do: scan_and_delete(conn, next, pattern)
  end
end
