defmodule Socketelx.HttpRouter do
  @moduledoc """
  Plug router for the Pusher REST API:

    * `POST /apps/{app_id}/events`: trigger an event
    * `POST /apps/{app_id}/batch_events`: trigger up to 10 events at once
    * `GET /apps/{app_id}/channels`: list occupied channels
      (`filter_by_prefix`, and `info=user_count` for presence channels)
    * `GET /apps/{app_id}/channels/{channel}`: one channel's info
    * `GET /apps/{app_id}/channels/{channel}/users`: who is in a presence channel
    * `GET /`: health check; `GET /metrics`: Prometheus metrics (opt-in)

  Signature verification happens on the raw body/query, before any JSON
  parsing, since the signature covers the exact bytes sent.

  The instance `name` comes in via the plug opts configured in the
  cowboy dispatch table (`{Plug.Cowboy.Handler, {Socketelx.HttpRouter,
  name: name}}`) and is stashed in `conn.private` by overriding `call/2` —
  the standard Plug technique for parameterizing a `Plug.Router`.
  """
  use Plug.Router

  alias Socketelx.{Apps, Auth, Cluster, Config, Metrics, RateLimiter}

  plug :match
  plug :fetch_query_params
  plug :resolve_app
  plug :rate_limit
  plug :dispatch

  # `use Plug.Router` builds its own call/2 via Plug.Builder, which marks
  # it `defoverridable` precisely so per-instance data can be injected
  # like this before falling through to the normal match/dispatch pipeline.
  @impl Plug
  def call(conn, opts) do
    name = Keyword.get(opts, :name, Socketelx)

    conn
    |> Plug.Conn.put_private(:socketelx_root, name)
    |> Plug.Conn.put_private(:socketelx_instance, name)
    |> super(opts)
  end

  # `/apps/:id/...` belongs to the app with that id (an unknown id looks the
  # same as a bad signature). Everything else is server-wide.
  defp resolve_app(%Plug.Conn{path_info: ["apps", app_id | _]} = conn, _opts) do
    case Apps.by_id(conn.private.socketelx_root, app_id) do
      nil ->
        conn |> send_json(401, %{error: "invalid signature"}) |> halt()

      instance ->
        Plug.Conn.put_private(conn, :socketelx_instance, instance)
    end
  end

  defp resolve_app(conn, _opts), do: conn

  get "/" do
    send_resp(conn, 200, "ok")
  end

  get "/metrics" do
    instances = Apps.instances(conn.private.socketelx_root)
    config = Config.get(hd(instances))

    cond do
      not config.metrics ->
        send_json(conn, 404, %{error: "not found"})

      not metrics_authorized?(conn, config.metrics_token) ->
        conn
        |> put_resp_header("www-authenticate", "Bearer")
        |> send_json(401, %{error: "unauthorized"})

      true ->
        conn
        |> put_resp_content_type("text/plain; version=0.0.4")
        |> send_resp(200, render_metrics(instances))
    end
  end

  post "/apps/:app_id/events" do
    case read_limited_body(conn) do
      {:ok, body, conn} -> handle_trigger(conn, app_id, body)
      {:too_large, conn} -> too_large(conn)
    end
  end

  post "/apps/:app_id/batch_events" do
    case read_limited_body(conn) do
      {:ok, body, conn} -> handle_batch(conn, app_id, body)
      {:too_large, conn} -> too_large(conn)
    end
  end

  post "/apps/:app_id/users/:user_id/terminate_connections" do
    case read_limited_body(conn) do
      {:ok, body, conn} -> handle_terminate_user(conn, app_id, user_id, body)
      {:too_large, conn} -> too_large(conn)
    end
  end

  get "/apps/:app_id/channels" do
    handle_channel_list(conn, app_id)
  end

  get "/apps/:app_id/channels/:channel" do
    handle_channel_info(conn, app_id, channel)
  end

  get "/apps/:app_id/channels/:channel/users" do
    handle_channel_users(conn, app_id, channel)
  end

  match _ do
    send_json(conn, 404, %{error: "not found"})
  end

  # The body is read before the signature is checked, so cap it: Pusher's own limit is 10 KB
  # per event and 10 events per batch, and this leaves plenty of room above that.
  @max_body_bytes 1_000_000

  defp read_limited_body(conn) do
    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, conn} -> {:ok, body, conn}
      {:more, _partial, conn} -> {:too_large, conn}
      {:error, _reason} -> {:too_large, conn}
    end
  end

  defp too_large(conn), do: conn |> send_json(413, %{error: "request body too large"}) |> halt()

  # Applies to the REST API only (not the health check or metrics). Runs
  # before signature checks so a flood of bad requests is cut off cheaply.
  defp rate_limit(%Plug.Conn{request_path: "/apps/" <> _} = conn, _opts) do
    name = conn.private.socketelx_instance
    Metrics.incr(name, :rest_requests_total)

    if RateLimiter.allow?(name, :rest, Config.get(name).rest_rate_limit) do
      conn
    else
      Metrics.incr(name, :rest_rate_limited_total)

      conn
      |> put_resp_header("retry-after", "1")
      |> send_json(429, %{error: "rate limit exceeded"})
      |> halt()
    end
  end

  defp rate_limit(conn, _opts), do: conn

  defp render_metrics([only]), do: Metrics.render(only)

  defp render_metrics(instances),
    do: instances |> Enum.map(&{&1, Config.get(&1).app_id}) |> Metrics.render_many()

  defp metrics_authorized?(_conn, nil), do: true

  defp metrics_authorized?(conn, token) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> provided] -> Plug.Crypto.secure_compare(provided, token)
      _ -> false
    end
  end

  defp handle_trigger(conn, app_id, body) do
    name = conn.private.socketelx_instance

    with :ok <- authorize(conn, "POST", app_id, body),
         {:ok, %{"name" => event_name, "data" => data} = payload} <- Jason.decode(body) do
      channels = List.wrap(payload["channels"] || payload["channel"])
      deliver(name, channels, event_name, data, payload["socket_id"])
      send_json(conn, 200, %{})
    else
      :unauthorized -> send_json(conn, 401, %{error: "invalid signature"})
      {:error, _} -> send_json(conn, 400, %{error: "invalid payload"})
      _ -> send_json(conn, 400, %{error: "bad request"})
    end
  end

  # Pusher libraries send `{}` (with its body_md5); an empty body is accepted too.
  defp handle_terminate_user(conn, app_id, user_id, body) do
    name = conn.private.socketelx_instance

    case authorize(conn, "POST", app_id, if(body == "", do: nil, else: body)) do
      :ok ->
        Cluster.terminate_user(name, user_id)
        send_json(conn, 200, %{})

      :unauthorized ->
        send_json(conn, 401, %{error: "invalid signature"})
    end
  end

  defp handle_batch(conn, app_id, body) do
    name = conn.private.socketelx_instance
    limit = Config.get(name).batch_limit

    with :ok <- authorize(conn, "POST", app_id, body),
         {:ok, %{"batch" => batch}} when is_list(batch) <- Jason.decode(body),
         true <- length(batch) <= limit or {:too_many, length(batch)},
         true <- Enum.all?(batch, &valid_batch_event?/1) do
      for %{"channel" => channel, "name" => event_name, "data" => data} = event <- batch do
        deliver(name, [channel], event_name, data, event["socket_id"])
      end

      send_json(conn, 200, %{})
    else
      :unauthorized ->
        send_json(conn, 401, %{error: "invalid signature"})

      {:too_many, _n} ->
        send_json(conn, 400, %{error: "a batch holds at most #{limit} events"})

      _ ->
        send_json(conn, 400, %{error: "invalid payload"})
    end
  end

  defp valid_batch_event?(%{"channel" => c, "name" => n, "data" => _})
       when is_binary(c) and is_binary(n),
       do: true

  defp valid_batch_event?(_), do: false

  # Delivers to this node's sockets and, through the backend, to every other node's.
  defp deliver(name, channels, event_name, data, exclude_socket_id) do
    for channel <- channels, is_binary(channel) do
      Metrics.incr(name, :events_triggered_total)

      Cluster.broadcast(
        name,
        channel,
        %{event: event_name, data: data, channel: channel},
        exclude_socket_id
      )
    end

    :ok
  end

  # app id + signature + body hash (POST only) + timestamp
  defp authorize(conn, method, app_id, body) do
    name = conn.private.socketelx_instance

    ok? =
      Config.valid_app_id?(name, app_id) and
        Auth.valid_request_signature?(name, method, conn.request_path, conn.query_params) and
        (body == nil or Auth.valid_body_md5?(conn.query_params, body)) and
        Auth.fresh_timestamp?(conn.query_params)

    if ok?, do: :ok, else: :unauthorized
  end

  defp handle_channel_list(conn, app_id) do
    name = conn.private.socketelx_instance
    prefix = conn.query_params["filter_by_prefix"] || ""
    info = conn.query_params["info"]

    cond do
      authorize(conn, "GET", app_id, nil) != :ok ->
        send_json(conn, 401, %{error: "invalid signature"})

      info not in [nil, "", "user_count"] ->
        send_json(conn, 400, %{error: "unsupported info attribute"})

      info == "user_count" and not String.starts_with?(prefix, "presence-") ->
        send_json(conn, 400, %{error: "info=user_count requires filter_by_prefix=presence-"})

      true ->
        channels =
          name
          |> Cluster.list_channels()
          |> Enum.reject(&String.starts_with?(&1, "#server-to-user-"))
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Map.new(fn channel ->
            attrs =
              if info == "user_count",
                do: %{user_count: length(Cluster.presence_members(name, channel))},
                else: %{}

            {channel, attrs}
          end)

        send_json(conn, 200, %{channels: channels})
    end
  end

  defp handle_channel_info(conn, app_id, channel) do
    name = conn.private.socketelx_instance

    case authorize(conn, "GET", app_id, nil) do
      :ok ->
        count = Cluster.subscription_count(name, channel)
        base = %{occupied: count > 0, subscription_count: count}

        info =
          if String.starts_with?(channel, "presence-") do
            Map.put(base, :user_count, length(Cluster.presence_members(name, channel)))
          else
            base
          end

        send_json(conn, 200, info)

      :unauthorized ->
        send_json(conn, 401, %{error: "invalid signature"})
    end
  end

  defp handle_channel_users(conn, app_id, channel) do
    name = conn.private.socketelx_instance

    cond do
      authorize(conn, "GET", app_id, nil) != :ok ->
        send_json(conn, 401, %{error: "invalid signature"})

      not String.starts_with?(channel, "presence-") ->
        send_json(conn, 400, %{error: "users are only available for presence channels"})

      true ->
        users =
          name
          |> Cluster.presence_members(channel)
          |> Enum.map(fn {user_id, _info} -> %{id: user_id} end)

        send_json(conn, 200, %{users: users})
    end
  end

  defp send_json(conn, status, payload) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(payload))
  end
end
