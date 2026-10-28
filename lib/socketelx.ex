defmodule Socketelx do
  @moduledoc """
  Embed a Pusher-protocol-compatible websocket server in your own
  application's supervision tree:

      children = [
        {Socketelx,
         name: MyApp.Socketelx,
         port: 4001,
         app_key: "my-key",
         app_secret: "my-secret",
         app_id: "my-app-id"}
      ]

  Every option can be omitted; it falls back to the `SOCKETELX_APP_KEY` /
  `SOCKETELX_APP_SECRET` / `SOCKETELX_APP_ID` / `PORT` / `SOCKETELX_WEBHOOK_URL` /
  `SOCKETELX_ENABLE_CLIENT_MESSAGES` / `SOCKETELX_REDIS_URL` environment
  variables (the same ones the standalone Docker release reads), so this
  also works with zero config for the common case of one instance per app.

  ## Running several servers

  Add `redis_url: "redis://host:6379"` (or set `SOCKETELX_REDIS_URL`) on every
  node and they behave as one: a client connected to node A receives events
  triggered through node B, presence is shared, and
  `member_added`/`channel_occupied`-style events fire once cluster-wide.
  Every node must use the same `app_id`/`app_key`/`app_secret`.

  Redis is optional — without `:redis_url` an instance is fully in-memory and
  never opens a Redis connection.

  ## Several apps in one server

  Pass `:apps` (or set `SOCKETELX_APPS` to a JSON array) to serve several
  Pusher apps from one port. Each app has its own id, key and secret, and is
  completely isolated: channels, presence, webhooks, limits and metrics are
  never shared between apps.

      {Socketelx,
       name: MyApp.Socketelx,
       port: 6001,
       apps: [
         [id: "chat", key: "chat-key", secret: "chat-secret",
          allowed_origins: ["https://chat.example.com"]],
         [id: "admin", key: "admin-key", secret: "admin-secret",
          webhook_url: "https://admin.example.com/hooks"]
       ]}

  Everything else you pass (limits, Redis, cluster settings, ...) applies to
  every app unless an app sets it itself. When calling `trigger/5` or
  `authorize_channel/4` on a multi-app server, say which app with `app: "chat"`.

  ## Several instances in one app

  Running more than one named instance in the same app (e.g. two
  different Pusher app_ids on two different ports) works too — just give
  each a unique `:name`. Each instance gets its own channel registry and
  presence state; they never see each other's connections or channels.
  """
  use Supervisor
  require Logger

  alias Socketelx.{Apps, Config, Naming}

  @default_name __MODULE__

  @doc """
  Starts one instance (its supervisor, registry, backend processes and HTTP/WebSocket listener).

  Usually you don't call this directly; add `{Socketelx, opts}` to your supervision
  tree instead. See `Socketelx.Config` for every option.
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    Supervisor.start_link(__MODULE__, {name, opts}, name: Naming.supervisor(name))
  end

  # use Supervisor's default child_spec/1 keys every child on `id: __MODULE__`,
  # which would collide if a host app mounts two named Socketelx
  # instances under the same supervisor. Key on {module, name} instead.
  @doc false
  def child_spec(opts) do
    name = Keyword.get(opts, :name, @default_name)

    %{
      id: {__MODULE__, name},
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor
    }
  end

  @impl true
  def init({name, opts}) do
    configs =
      for {instance, instance_opts} <- Apps.build(name, opts) do
        Socketelx.Metrics.init(instance)
        Socketelx.RateLimiter.init(instance)
        {instance, Config.resolve(instance, instance_opts)}
      end

    Apps.register(name, Enum.map(configs, &elem(&1, 0)))

    # Order matters: every app's registry and backend must exist before the
    # listener starts accepting sockets, so the listener goes last.
    app_children =
      Enum.flat_map(configs, fn {instance, config} ->
        [{Registry, keys: :duplicate, name: Naming.registry(instance)}] ++
          config.backend.child_specs(instance, config) ++
          Socketelx.Webhook.child_specs(instance, config)
      end)

    # The port, TLS and metrics settings are server-wide, so any app's copy will do.
    {_instance, first} = hd(configs)
    Supervisor.init(app_children ++ [listener_spec(name, first)], strategy: :one_for_one)
  end

  @doc """
  Stops this node from accepting new connections and tells every
  currently-connected client to reconnect (a clean `pusher:error` + close
  frame instead of the socket just dying). With several nodes behind a load
  balancer, clients reconnect to a node that is still up.

  Call this from your own app's shutdown path if you embed Socketelx;
  the standalone release calls it automatically on SIGTERM.
  """
  def drain(name \\ @default_name) do
    # Suspending (rather than stopping) the listener is what works when the
    # listener is supervised by us: no new connections, existing ones live on
    # long enough to receive the close frame below.
    :ranch.suspend_listener(Naming.listener(name))

    for instance <- Apps.instances(name) do
      Registry.dispatch(Naming.registry(instance), :__all_connections__, fn entries ->
        for {pid, _socket_id} <- entries, do: send(pid, :go_away)
      end)
    end

    :ok
  end

  @doc """
  Sends an event to every subscriber of `channel` (on every node, with Redis)
  without going through the HTTP API. This is how an embedding app publishes.

  `data` is sent as a JSON string, as Pusher does: pass a string to send it
  untouched, or any term `Jason` can encode.

  ## Options

    * `:socket_id`: skip this connection (the sender, usually)
    * `:app`: the app id, needed only when the server serves several apps

  ## Example

      Socketelx.trigger(MyApp.Socketelx, "chat", "message", %{text: "hi"})
  """
  @spec trigger(atom(), String.t(), String.t(), term(), keyword()) :: :ok
  def trigger(name, channel, event, data, opts \\ [])
      when is_binary(channel) and is_binary(event) do
    name = Apps.fetch!(name, Keyword.get(opts, :app))
    payload = if is_binary(data), do: data, else: Jason.encode!(data)
    Socketelx.Metrics.incr(name, :events_triggered_total)

    Socketelx.Cluster.broadcast(
      name,
      channel,
      %{event: event, data: payload, channel: channel},
      Keyword.get(opts, :socket_id)
    )
  end

  @doc """
  Closes every connection of a signed-in user, on every node. Use it when a user logs out
  or is banned. Clients are not told why; they just see the socket close (and pusher-js
  reconnects, but cannot sign in again without a fresh `authenticate_user/4`).

      Socketelx.terminate_user_connections(MyApp.Socketelx, "42")

  The REST equivalent is `POST /apps/:app_id/users/:user_id/terminate_connections`.
  Connections that never signed in cannot be found by user id. On a multi-app server pass
  `app: "<app id>"`.
  """
  @spec terminate_user_connections(atom(), String.t() | integer(), keyword()) :: :ok
  def terminate_user_connections(name, user_id, opts \\ []) do
    Socketelx.Cluster.terminate_user(Apps.fetch!(name, Keyword.get(opts, :app)), user_id)
  end

  @doc """
  Signs a client's request to join a private or presence channel and returns
  the map your auth endpoint should reply with as JSON.

  Call it from the endpoint pusher-js is configured to use
  (`channelAuthorization.endpoint`) after you have decided the user may join.
  For a presence channel pass `user_id` and optionally `user_info`. On a
  multi-app server pass `app: "<app id>"`.

  ## Examples

      Socketelx.authorize_channel(MyApp.Socketelx, socket_id, "private-room-1")
      #=> %{auth: "app-key:9c1f..."}

      Socketelx.authorize_channel(MyApp.Socketelx, socket_id, "presence-room-1",
        user_id: "42", user_info: %{name: "Ada"})
      #=> %{auth: "app-key:...", channel_data: "{\\"user_id\\":\\"42\\",...}"}
  """
  @spec authorize_channel(atom(), String.t(), String.t(), keyword()) :: map()
  def authorize_channel(name, socket_id, channel, opts \\ [])
      when is_binary(socket_id) and is_binary(channel) do
    config = Config.get(Apps.fetch!(name, Keyword.get(opts, :app)))

    case Keyword.fetch(opts, :user_id) do
      {:ok, user_id} ->
        channel_data =
          Jason.encode!(%{
            user_id: to_string(user_id),
            user_info: Keyword.get(opts, :user_info, %{})
          })

        sig = Socketelx.Auth.sign(config.app_secret, "#{socket_id}:#{channel}:#{channel_data}")
        %{auth: "#{config.app_key}:#{sig}", channel_data: channel_data}

      :error ->
        sig = Socketelx.Auth.sign(config.app_secret, "#{socket_id}:#{channel}")
        %{auth: "#{config.app_key}:#{sig}"}
    end
  end

  @doc """
  Signs a user in for pusher-js's `pusher.signin()`. Call it from the endpoint
  pusher-js is configured to use as `userAuthentication.endpoint`, after you
  have identified the user, and reply with the returned map as JSON.

  `user` must have an `:id` (a string or integer). Once signed in, events
  triggered on the channel `"\#server-to-user-<id>"` reach every connection
  of that user.

      Socketelx.authenticate_user(MyApp.Socketelx, socket_id, %{id: "42", name: "Ada"})
      #=> %{auth: "app-key:...", user_data: "{\\"id\\":\\"42\\",\\"name\\":\\"Ada\\"}"}
  """
  @spec authenticate_user(atom(), String.t(), map(), keyword()) :: map()
  def authenticate_user(name, socket_id, user, opts \\ [])
      when is_binary(socket_id) and is_map(user) do
    unless Map.has_key?(user, :id) or Map.has_key?(user, "id") do
      raise ArgumentError, "the user needs an :id"
    end

    config = Config.get(Apps.fetch!(name, Keyword.get(opts, :app)))
    user_data = Jason.encode!(user)
    sig = Socketelx.Auth.sign(config.app_secret, "#{socket_id}::user::#{user_data}")
    %{auth: "#{config.app_key}:#{sig}", user_data: user_data}
  end

  defp listener_spec(name, config) do
    dispatch = [
      {:_,
       [
         {"/app/:app_key", Socketelx.WsHandler, %{name: name}},
         {:_, Plug.Cowboy.Handler, {Socketelx.HttpRouter, name: name}}
       ]}
    ]

    {scheme, tls} =
      if config.tls_certfile,
        do: {:https, [certfile: config.tls_certfile, keyfile: config.tls_keyfile]},
        else: {:http, []}

    spec =
      Plug.Cowboy.child_spec(
        scheme: scheme,
        plug: {Socketelx.HttpRouter, name: name},
        options: [port: config.port, ref: Naming.listener(name), dispatch: dispatch] ++ tls
      )

    apps = Apps.instances(name)

    Logger.info(
      "Socketelx[#{inspect(name)}] listening on port #{config.port}" <>
        if(scheme == :https, do: " (TLS)", else: "") <>
        " (backend: #{inspect(config.backend)}, node: #{config.node_id}" <>
        if(apps == [name], do: ")", else: ", apps: #{length(apps)})")
    )

    Map.put(spec, :id, {:listener, name})
  end
end
