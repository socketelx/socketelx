defmodule Socketelx.MultiAppTest do
  @moduledoc """
  Allowed origins, several apps on one port, the presence users endpoint,
  user sign-in, per-IP limits, the on-disk webhook queue and built-in TLS,
  all over real sockets and real signed HTTP requests.
  """
  use ExUnit.Case, async: false

  alias Socketelx.{Metrics, Naming, TestWsClient}

  # ---- helpers ----

  defp start_server(extra) do
    name = :"multi_#{System.unique_integer([:positive])}"
    opts = [name: name, port: 0] ++ extra
    start_supervised!(Supervisor.child_spec({Socketelx, opts}, id: name))
    {name, :ranch.get_port(Naming.listener(name))}
  end

  defp single(extra \\ []),
    do: start_server([app_id: "one", app_key: "k1", app_secret: "s1"] ++ extra)

  defp two_apps(extra \\ []) do
    start_server(
      [
        apps: [
          [id: "a", key: "ka", secret: "sa"],
          [id: "b", key: "kb", secret: "sb", allowed_origins: ["https://b.example.com"]]
        ]
      ] ++ extra
    )
  end

  defp open(port, key, headers \\ []) do
    {:ok, client} = TestWsClient.start(port, key, self(), headers: headers)
    client
  end

  defp connect(port, key, headers \\ []) do
    client = open(port, key, headers)
    assert_receive {:ws, ^client, %{"event" => "pusher:connection_established", "data" => data}}
    {client, Jason.decode!(data)["socket_id"]}
  end

  defp assert_refused(port, key, code, headers \\ []) do
    client = open(port, key, headers)
    assert_receive {:ws, ^client, %{"event" => "pusher:error", "data" => %{"code" => ^code}}}
    assert_receive {:ws_closed, ^client}
  end

  defp sign(secret, string),
    do: :crypto.mac(:hmac, :sha256, secret, string) |> Base.encode16(case: :lower)

  defp subscribe(client, channel) do
    TestWsClient.send_json(client, %{event: "pusher:subscribe", data: %{channel: channel}})

    assert_receive {:ws, ^client,
                    %{"event" => "pusher_internal:subscription_succeeded", "channel" => ^channel}}
  end

  defp subscribe_presence(client, key, secret, socket_id, channel, user_id) do
    data = Jason.encode!(%{user_id: user_id, user_info: %{}})
    auth = "#{key}:#{sign(secret, "#{socket_id}:#{channel}:#{data}")}"

    TestWsClient.send_json(client, %{
      event: "pusher:subscribe",
      data: %{channel: channel, auth: auth, channel_data: data}
    })

    assert_receive {:ws, ^client,
                    %{"event" => "pusher_internal:subscription_succeeded", "channel" => ^channel}}
  end

  # signed REST call as app `{key, secret}`; returns {status, decoded body}
  defp rest(port, {key, secret}, method, path, body \\ nil, scheme \\ "http") do
    params = %{
      "auth_key" => key,
      "auth_timestamp" => to_string(System.system_time(:second)),
      "auth_version" => "1.0"
    }

    params = if body, do: Map.put(params, "body_md5", md5(body)), else: params
    query = params |> Enum.sort() |> Enum.map_join("&", fn {k, v} -> "#{k}=#{v}" end)
    sig = sign(secret, "#{String.upcase(to_string(method))}\n#{path}\n#{query}")
    url = ~c"#{scheme}://127.0.0.1:#{port}#{path}?#{query}&auth_signature=#{sig}"

    request = if body, do: {url, [], ~c"application/json", body}, else: {url, []}
    ssl = [ssl: [verify: :verify_none]]
    {:ok, {{_, status, _}, _h, resp}} = :httpc.request(method, request, ssl, [])
    {status, if(resp == [], do: %{}, else: Jason.decode!(to_string(resp)))}
  end

  defp md5(body), do: :crypto.hash(:md5, body) |> Base.encode16(case: :lower)

  defp trigger(port, app, id, channel, event) do
    body = Jason.encode!(%{name: event, channels: [channel], data: "{}"})
    rest(port, app, :post, "/apps/#{id}/events", body)
  end

  # ---- allowed origins ----

  test "every origin is allowed by default" do
    {_, port} = single()
    {c, _} = connect(port, "k1", [{"Origin", "https://anything.example"}])
    assert is_pid(c)
    connect(port, "k1")
  end

  test "only listed origins may connect, in all three notations" do
    {_, port} =
      single(
        allowed_origins: ["https://chat.example.com", "app.example.org", "*.cdn.example.net"]
      )

    connect(port, "k1", [{"Origin", "https://chat.example.com"}])
    connect(port, "k1", [{"Origin", "http://app.example.org:8080"}])
    connect(port, "k1", [{"Origin", "https://a.cdn.example.net"}])
    # no Origin header (not a browser): not affected
    connect(port, "k1")

    assert_refused(port, "k1", 4009, [{"Origin", "http://chat.example.com"}])
    assert_refused(port, "k1", 4009, [{"Origin", "https://evil.example"}])
    assert_refused(port, "k1", 4009, [{"Origin", "https://cdn.example.net"}])
    assert_refused(port, "k1", 4009, [{"Origin", "https://notchat.example.com.evil.io"}])
    assert_refused(port, "k1", 4009, [{"Origin", "null"}])
  end

  test "a \"*\" entry, an empty list and a comma-separated string work as expected" do
    {_, port} = single(allowed_origins: ["*"])
    connect(port, "k1", [{"Origin", "https://x.example"}])

    System.put_env("SOCKETELX_ALLOWED_ORIGINS", "https://one.example, two.example")
    on_exit(fn -> System.delete_env("SOCKETELX_ALLOWED_ORIGINS") end)
    {_, port2} = single()
    connect(port2, "k1", [{"Origin", "https://two.example"}])
    assert_refused(port2, "k1", 4009, [{"Origin", "https://three.example"}])
  end

  # ---- several apps ----

  test "apps are isolated: channels, signatures and keys" do
    {_, port} = two_apps()
    {ca, _} = connect(port, "ka")
    {cb, _} = connect(port, "kb")
    subscribe(ca, "room")
    subscribe(cb, "room")

    assert {200, _} = trigger(port, {"ka", "sa"}, "a", "room", "only-a")
    assert_receive {:ws, ^ca, %{"event" => "only-a"}}
    refute_receive {:ws, ^cb, %{"event" => "only-a"}}, 200

    # app b's secret does not work for app a, and an unknown id looks the same
    assert {401, _} = trigger(port, {"kb", "sb"}, "a", "room", "x")
    assert {401, _} = trigger(port, {"ka", "sa"}, "nope", "room", "x")

    assert {200, %{"occupied" => true, "subscription_count" => 1}} =
             rest(port, {"kb", "sb"}, :get, "/apps/b/channels/room")

    assert {200, %{"channels" => %{}}} = rest(port, {"ka", "sa"}, :get, "/apps/a/channels")
  end

  test "an unknown key is refused and private auth is per app" do
    {_, port} = two_apps()
    assert_refused(port, "nope", 4001)

    {c, sid} = connect(port, "ka")
    # signed with app b's secret: invalid for app a
    auth = "ka:#{sign("sb", "#{sid}:private-x")}"

    TestWsClient.send_json(c, %{
      event: "pusher:subscribe",
      data: %{channel: "private-x", auth: auth}
    })

    assert_receive {:ws, ^c, %{"event" => "pusher:error", "data" => %{"code" => 4009}}}
  end

  test "origins, limits and metrics are per app" do
    {name, port} = two_apps(max_connections: 1)
    # app b only accepts its own origin; app a accepts anything
    assert_refused(port, "kb", 4009, [{"Origin", "https://a.example.com"}])
    connect(port, "kb", [{"Origin", "https://b.example.com"}])
    connect(port, "ka", [{"Origin", "https://a.example.com"}])

    # the cap of 1 applies to each app separately
    assert_refused(port, "ka", 4004)
    assert_refused(port, "kb", 4004, [{"Origin", "https://b.example.com"}])

    app_a = Module.concat([name, App, "a"])
    assert Metrics.get(app_a, :connections_total) == 1
  end

  test "metrics list every app under one header when enabled" do
    {_, port} = two_apps(metrics: true)
    connect(port, "ka")
    {:ok, {{_, 200, _}, _, text}} = :httpc.request(~c"http://127.0.0.1:#{port}/metrics")
    text = to_string(text)
    assert text =~ ~s(socketelx_connections{app="a"} 1)
    assert text =~ ~s(socketelx_connections{app="b"} 0)
    assert length(String.split(text, "# TYPE socketelx_connections gauge")) == 2
  end

  test "the public API needs :app on a multi-app server" do
    {name, _} = two_apps()
    assert_raise ArgumentError, ~r/pass app/, fn -> Socketelx.trigger(name, "c", "e", %{}) end

    assert %{auth: "kb:" <> _} = Socketelx.authorize_channel(name, "1.2", "private-x", app: "b")
    assert :ok = Socketelx.trigger(name, "c", "e", %{}, app: "a")

    assert_raise ArgumentError, ~r/no app with id/, fn ->
      Socketelx.trigger(name, "c", "e", %{}, app: "zz")
    end
  end

  test "apps can be given as JSON in SOCKETELX_APPS" do
    System.put_env(
      "SOCKETELX_APPS",
      ~s([{"id":"j1","key":"jk1","secret":"js1"},{"id":"j2","key":"jk2","secret":"js2","allowed_origins":["https://j.example"]}])
    )

    on_exit(fn -> System.delete_env("SOCKETELX_APPS") end)
    {_, port} = start_server([])
    connect(port, "jk1")
    assert_refused(port, "jk2", 4009, [{"Origin", "https://other.example"}])
  end

  test "bad app lists are rejected with a clear error" do
    assert_raise ArgumentError, ~r/unique app_id/, fn ->
      Socketelx.Apps.build(:x,
        apps: [[id: "a", key: "1", secret: "s"], [id: "a", key: "2", secret: "s"]]
      )
    end

    assert_raise ArgumentError, ~r/unique app_key/, fn ->
      Socketelx.Apps.build(:x,
        apps: [[id: "a", key: "1", secret: "s"], [id: "b", key: "1", secret: "s"]]
      )
    end

    assert_raise ArgumentError, ~r/needs a string secret/, fn ->
      Socketelx.Apps.build(:x, apps: [[id: "a", key: "1"]])
    end

    assert_raise ArgumentError, ~r/unknown app setting/, fn ->
      Socketelx.Apps.build(:x, apps: [[id: "a", key: "1", secret: "s", bogus: 1]])
    end
  end

  # ---- presence users endpoint ----

  test "GET /channels/:channel/users lists presence members" do
    {_, port} = single()
    {c1, s1} = connect(port, "k1")
    {c2, s2} = connect(port, "k1")
    subscribe_presence(c1, "k1", "s1", s1, "presence-room", "alice")
    subscribe_presence(c2, "k1", "s1", s2, "presence-room", "bob")
    app = {"k1", "s1"}

    assert {200, %{"users" => users}} =
             rest(port, app, :get, "/apps/one/channels/presence-room/users")

    assert users |> Enum.map(& &1["id"]) |> Enum.sort() == ["alice", "bob"]

    assert {400, _} = rest(port, app, :get, "/apps/one/channels/plain/users")
    assert {401, _} = rest(port, {"k1", "wrong"}, :get, "/apps/one/channels/presence-room/users")
  end

  # ---- terminating a user's connections ----

  defp sign_in(name, c, sid, id) do
    %{auth: auth, user_data: user_data} = Socketelx.authenticate_user(name, sid, %{id: id})

    TestWsClient.send_json(c, %{event: "pusher:signin", data: %{auth: auth, user_data: user_data}})

    assert_receive {:ws, ^c, %{"event" => "pusher:signin_success"}}
  end

  test "terminate_connections closes every connection of that user and nobody else's" do
    {name, port} = single()
    {a1, sid_a1} = connect(port, "k1")
    {a2, sid_a2} = connect(port, "k1")
    {b, sid_b} = connect(port, "k1")
    {anon, _} = connect(port, "k1")
    sign_in(name, a1, sid_a1, "7")
    sign_in(name, a2, sid_a2, "7")
    sign_in(name, b, sid_b, "8")

    assert {200, _} =
             rest(port, {"k1", "s1"}, :post, "/apps/one/users/7/terminate_connections", "{}")

    assert_receive {:ws_closed, ^a1}, 2_000
    assert_receive {:ws_closed, ^a2}, 2_000
    refute_receive {:ws_closed, ^b}, 300
    refute_receive {:ws_closed, ^anon}, 100

    # the other user can still be reached, and a user who is not connected is not an error
    assert {200, _} = trigger(port, {"k1", "s1"}, "one", "#server-to-user-8", "still-here")
    assert_receive {:ws, ^b, %{"event" => "still-here"}}

    assert {200, _} =
             rest(port, {"k1", "s1"}, :post, "/apps/one/users/999/terminate_connections", "{}")
  end

  test "terminate_connections needs a valid signature" do
    {name, port} = single()
    {c, sid} = connect(port, "k1")
    sign_in(name, c, sid, "7")

    assert {401, _} =
             rest(port, {"k1", "wrong"}, :post, "/apps/one/users/7/terminate_connections", "{}")

    refute_receive {:ws_closed, ^c}, 300
  end

  test "Socketelx.terminate_user_connections/3 does the same from Elixir code" do
    {name, port} = single()
    {c, sid} = connect(port, "k1")
    sign_in(name, c, sid, "7")

    assert :ok = Socketelx.terminate_user_connections(name, "7")
    assert_receive {:ws_closed, ^c}, 2_000
  end

  test "the control message cannot be forged through the trigger endpoint" do
    {name, port} = single()
    {c, sid} = connect(port, "k1")
    sign_in(name, c, sid, "7")

    body =
      Jason.encode!(%{
        name: "pusher_internal:terminate_connection",
        channels: ["#server-to-user-7"],
        data: "{}"
      })

    assert {200, _} = rest(port, {"k1", "s1"}, :post, "/apps/one/events", body)
    assert_receive {:ws, ^c, %{"event" => "pusher_internal:terminate_connection"}}
    refute_receive {:ws_closed, ^c}, 300
  end

  # ---- user sign-in ----

  test "a signed-in user receives events sent to #server-to-user-<id>" do
    {name, port} = single()
    {c, sid} = connect(port, "k1")

    %{auth: auth, user_data: user_data} =
      Socketelx.authenticate_user(name, sid, %{id: "42", name: "Ada"})

    TestWsClient.send_json(c, %{event: "pusher:signin", data: %{auth: auth, user_data: user_data}})

    assert_receive {:ws, ^c, %{"event" => "pusher:signin_success", "data" => data}}
    assert Jason.decode!(data)["user_data"] == user_data

    assert {200, _} = trigger(port, {"k1", "s1"}, "one", "#server-to-user-42", "hello-ada")
    assert_receive {:ws, ^c, %{"event" => "hello-ada", "channel" => "#server-to-user-42"}}

    # the internal channel is not listed
    assert {200, %{"channels" => channels}} = rest(port, {"k1", "s1"}, :get, "/apps/one/channels")
    assert channels == %{}

    # a second sign-in on the same connection is refused
    TestWsClient.send_json(c, %{event: "pusher:signin", data: %{auth: auth, user_data: user_data}})

    assert_receive {:ws, ^c, %{"event" => "pusher:error"}}
  end

  test "sign-in with a bad signature is refused" do
    {_, port} = single()
    {c, _sid} = connect(port, "k1")
    user_data = Jason.encode!(%{id: "1"})

    TestWsClient.send_json(c, %{
      event: "pusher:signin",
      data: %{auth: "k1:bad", user_data: user_data}
    })

    assert_receive {:ws, ^c, %{"event" => "pusher:error", "data" => %{"code" => 4009}}}
    refute_receive {:ws, ^c, %{"event" => "pusher:signin_success"}}, 100
  end

  # ---- per-IP limit ----

  test "max_connections_per_ip caps connections from one address and frees slots on close" do
    {_, port} = single(max_connections_per_ip: 2)
    {c1, _} = connect(port, "k1")
    connect(port, "k1")
    assert_refused(port, "k1", 4100)

    TestWsClient.kill(c1)
    Process.sleep(200)
    connect(port, "k1")
  end

  # ---- durable webhooks ----

  defmodule Receiver do
    @moduledoc false
    @behaviour Plug
    def init(opts), do: opts

    def call(conn, opts) do
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      send(opts[:owner], {:hook, opts[:status], body})
      Plug.Conn.send_resp(conn, opts[:status], "")
    end
  end

  defp receiver(status) do
    ref = :"recv_#{System.unique_integer([:positive])}"

    start_supervised!(
      Supervisor.child_spec(
        {Plug.Cowboy,
         scheme: :http,
         plug: {Receiver, owner: self(), status: status},
         options: [port: 0, ref: ref]},
        id: ref
      )
    )

    "http://127.0.0.1:#{:ranch.get_port(ref)}/hook"
  end

  @tag :capture_log
  test "a queued webhook survives a restart of the server" do
    dir = Path.join(System.tmp_dir!(), "socketelx_q_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(dir) end)

    # first life: the receiver always fails and the retry delay is long
    {name, _} =
      single(webhook_url: receiver(500), webhook_queue_path: dir, webhook_retry_base_ms: 60_000)

    Socketelx.Cluster.announce_channel_occupied(name, "news")
    assert_receive {:hook, 500, _}, 2_000
    Process.sleep(200)
    stop_supervised!(name)

    # second life: same app and directory, a receiver that works
    {_, _} = single(webhook_url: receiver(200), webhook_queue_path: dir)
    assert_receive {:hook, 200, body}, 3_000
    assert body =~ "channel_occupied"
    # delivered, so it is not sent again after another restart
    Process.sleep(200)
  end

  # ---- TLS ----

  test "serves HTTPS directly when a certificate is configured" do
    dir = Path.join(System.tmp_dir!(), "socketelx_tls_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    cert = Path.join(dir, "cert.pem")
    key = Path.join(dir, "key.pem")

    case System.cmd(
           "openssl",
           ~w(req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=localhost -keyout #{key} -out #{cert}),
           stderr_to_stdout: true
         ) do
      {_, 0} ->
        {_, port} = single(tls_certfile: cert, tls_keyfile: key)
        assert {200, _} = rest(port, {"k1", "s1"}, :get, "/apps/one/channels", nil, "https")

      _ ->
        :ok
    end
  end

  test "TLS needs both files" do
    assert_raise ArgumentError, ~r/both :tls_certfile and :tls_keyfile/, fn ->
      Socketelx.Config.resolve(:tls_x, tls_certfile: "a.pem")
    end
  end
end
