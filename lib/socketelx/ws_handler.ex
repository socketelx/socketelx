defmodule Socketelx.WsHandler do
  @moduledoc """
  Implements the client-facing side of the Pusher protocol over a raw
  cowboy websocket (not Phoenix Channels — a different wire format).

  Handles: pusher:subscribe / unsubscribe / ping, private + presence
  channel auth, client events, and delivering events pushed in via the
  HTTP trigger endpoint (see Socketelx.HttpRouter) or from other nodes.

  Receives the instance `name` via cowboy route opts (set up in
  `Socketelx`), so the same handler code serves any number
  of independently-configured instances. Everything that must be shared
  between nodes goes through `Socketelx.Cluster`.
  """
  @behaviour :cowboy_websocket

  alias Socketelx.{Apps, Auth, Cluster, Config, Metrics, Naming, RateLimiter, Webhook}

  @all_connections :__all_connections__
  @terminate_message Socketelx.Cluster.terminate_message()
  # Clients are told to ping after this many seconds of silence. The server
  # only closes a socket after *twice* that, so a healthy-but-quiet client
  # (whose ping is due right at `@activity_timeout_s`) never races the
  # server's own idle timeout.
  @activity_timeout_s 30
  @idle_timeout_ms @activity_timeout_s * 2 * 1_000
  # Cowboy's default is no limit. Real frames are tiny (Pusher caps client events at 10 KB), so
  # a bigger one is an attack or a bug: Cowboy closes the connection with code 1009.
  @max_frame_bytes 100_000

  # ---- cowboy_websocket callbacks ----

  @impl :cowboy_websocket
  def init(req, %{name: root}) do
    app_key = :cowboy_req.binding(:app_key, req)
    instance = Apps.by_key(root, app_key)
    # `name` is the instance of the app this key belongs to (the server's own
    # name when it serves a single app); everything below is per app.
    name = instance || root

    # A refused connection is reported the way the Pusher protocol specifies:
    # accept the upgrade, send `pusher:error`, close. (Replying with a plain
    # HTTP error would make clients retry instead of surfacing it.)
    reject =
      cond do
        is_nil(instance) -> 4001
        not Config.origin_allowed?(name, origin(req)) -> 4009
        over_capacity?(name) -> 4004
        true -> nil
      end

    state = %{
      name: name,
      ip: if(instance, do: client_ip(req, Config.get(name))),
      ip_counted: false,
      user_id: nil,
      user_channel: nil,
      socket_id: nil,
      channels: MapSet.new(),
      presence: %{},
      reject: reject,
      ce_window: 0,
      ce_count: 0
    }

    {:cowboy_websocket, req, state,
     %{idle_timeout: @idle_timeout_ms, max_frame_size: @max_frame_bytes}}
  end

  @impl :cowboy_websocket
  def websocket_init(%{reject: 4001} = state) do
    {:reply, [{:text, error_json("Application does not exist", 4001)}, :close], state}
  end

  def websocket_init(%{reject: 4009} = state) do
    Metrics.incr(state.name, :connections_rejected_total)
    {:reply, [{:text, error_json("Origin not allowed", 4009)}, :close], state}
  end

  def websocket_init(%{reject: 4004} = state) do
    Metrics.incr(state.name, :connections_rejected_total)
    {:reply, [{:text, error_json("Over capacity", 4004)}, :close], state}
  end

  def websocket_init(state) do
    if RateLimiter.ip_acquire(state.name, state.ip, Config.get(state.name).max_connections_per_ip) do
      accept(%{state | ip_counted: true})
    else
      Metrics.incr(state.name, :connections_rejected_total)
      msg = "Too many connections from your address"
      {:reply, [{:text, error_json(msg, 4100)}, :close], state}
    end
  end

  defp accept(state) do
    Metrics.incr(state.name, :connections_total)
    socket_id = generate_socket_id()
    # tracked separately from per-channel subscriptions so a graceful
    # shutdown can notify every connected client, subscribed or not
    Registry.register(Naming.registry(state.name), @all_connections, socket_id)

    payload = %{
      event: "pusher:connection_established",
      data: Jason.encode!(%{socket_id: socket_id, activity_timeout: @activity_timeout_s})
    }

    # Hibernating shrinks the process to almost nothing while it waits, which is
    # most of a connection's life. It wakes on the next message.
    {:reply, {:text, Jason.encode!(payload)}, %{state | socket_id: socket_id}, :hibernate}
  end

  @impl :cowboy_websocket
  def websocket_handle({:text, msg}, state) do
    case Jason.decode(msg) do
      {:ok, %{"event" => "pusher:subscribe", "data" => data}} ->
        handle_subscribe(data, state)

      {:ok, %{"event" => "pusher:unsubscribe", "data" => %{"channel" => channel}}}
      when is_binary(channel) ->
        handle_unsubscribe(channel, state)

      {:ok, %{"event" => "pusher:signin", "data" => %{} = data}} ->
        handle_signin(data, state)

      {:ok, %{"event" => "pusher:ping"}} ->
        # a ping means the connection is quiet: shrink it again
        {:reply, {:text, Jason.encode!(%{event: "pusher:pong", data: %{}})}, state, :hibernate}

      {:ok, %{"event" => "client-" <> _rest = event_name, "channel" => channel} = decoded}
      when is_binary(channel) ->
        handle_client_event(event_name, channel, decoded["data"], state)

      _ ->
        {:ok, state}
    end
  end

  def websocket_handle(_frame, state), do: {:ok, state}

  # Delivery from the HTTP trigger endpoint, a client event, or another node
  @impl :cowboy_websocket
  # `POST /apps/:id/users/:user_id/terminate_connections` (see Socketelx.Cluster.terminate_user/2)
  def websocket_info({:broadcast, @terminate_message, _exclude}, state), do: {:stop, state}

  def websocket_info({:broadcast, message, exclude}, state) do
    if excluded?(state, exclude) do
      {:ok, state}
    else
      {:reply, {:text, message}, state}
    end
  end

  def websocket_info({:broadcast, message}, state) do
    {:reply, {:text, message}, state}
  end

  # Sent by Socketelx.drain/1 during a graceful shutdown so clients get
  # a clean close instead of the TCP connection just dying.
  def websocket_info(:go_away, state) do
    {:reply, [{:text, error_json("Server is restarting, please reconnect.", 4000)}, :close],
     state}
  end

  # The Redis janitor noticed Redis lost its data: re-register this socket's
  # presence entries. Jittered so a big node doesn't stampede a fresh Redis.
  def websocket_info(:resync_presence, %{presence: presence} = state)
      when map_size(presence) > 0 do
    Process.send_after(self(), :do_resync_presence, :rand.uniform(2_000))
    {:ok, state}
  end

  def websocket_info(:do_resync_presence, state) do
    for {channel, {user_id, user_info}} <- state.presence do
      Cluster.presence_restore(state.name, channel, state.socket_id, user_id, user_info)
    end

    {:ok, state}
  end

  def websocket_info(_other, state), do: {:ok, state}

  @impl :cowboy_websocket
  def terminate(
        _reason,
        _partial_req,
        %{name: name, socket_id: socket_id, channels: channels} = state
      ) do
    registry = Naming.registry(name)
    if state.ip_counted, do: RateLimiter.ip_release(name, state.ip)

    # The per-user channel is internal: it never produces occupancy webhooks.
    if state.user_channel do
      Registry.unregister(registry, state.user_channel)
      Cluster.channel_left(name, state.user_channel)
    end

    Enum.each(channels, fn channel ->
      # Registry removes this pid's entries once the process actually exits,
      # but that is after terminate/3 returns — unregister explicitly so the
      # occupancy check is accurate.
      Registry.unregister(registry, channel)
      if presence?(channel), do: leave_presence(channel, socket_id, name)
      if Cluster.channel_left(name, channel), do: Cluster.announce_channel_vacated(name, channel)
    end)

    :ok
  end

  def terminate(_reason, _partial_req, _state), do: :ok

  # ---- user sign-in ----

  # pusher-js's `pusher.signin()`: the browser got `auth` + `user_data` from
  # your auth endpoint (`Socketelx.authenticate_user/4`). Signing in lets you
  # send events to one user on any of their connections by triggering the
  # channel `#server-to-user-<id>`.
  defp handle_signin(_data, %{user_id: user_id} = state) when not is_nil(user_id),
    do: error_reply("Already signed in", state)

  defp handle_signin(%{"auth" => auth, "user_data" => user_data}, state)
       when is_binary(user_data) do
    with true <- Auth.valid_signin?(state.name, state.socket_id, user_data, auth),
         {:ok, %{"id" => id}} when is_binary(id) or is_integer(id) <- Jason.decode(user_data) do
      user_id = to_string(id)
      channel = "#server-to-user-" <> user_id
      Registry.register(Naming.registry(state.name), channel, state.socket_id)
      Cluster.channel_joined(state.name, channel)

      reply =
        Jason.encode!(%{
          event: "pusher:signin_success",
          data: Jason.encode!(%{user_data: user_data})
        })

      {:reply, {:text, reply}, %{state | user_id: user_id, user_channel: channel}}
    else
      _ -> {:reply, {:text, error_json("Invalid signature for sign-in", 4009)}, state}
    end
  end

  defp handle_signin(_data, state), do: {:ok, state}

  # ---- subscribe/unsubscribe ----

  defp handle_subscribe(%{"channel" => channel} = data, state) when is_binary(channel) do
    auth = data["auth"]
    channel_data = data["channel_data"]

    authorized? =
      if private?(channel) or presence?(channel) do
        Auth.valid_channel_auth?(state.name, state.socket_id, channel, channel_data, auth)
      else
        true
      end

    if authorized? do
      do_subscribe(channel, channel_data, state)
    else
      {:reply, {:text, error_json("Invalid signature for channel #{channel}", 4009)}, state}
    end
  end

  defp handle_subscribe(_data, state), do: {:ok, state}

  defp do_subscribe(channel, channel_data, state) do
    if MapSet.member?(state.channels, channel) do
      # Subscribing twice is harmless in Pusher: answer again, don't register twice.
      data =
        if presence?(channel),
          do: presence_payload(Cluster.presence_members(state.name, channel)),
          else: %{}

      {:reply, {:text, subscription_succeeded(channel, data)}, state, :hibernate}
    else
      case presence_params(channel, channel_data) do
        :error ->
          {:reply,
           {:text, error_json("invalid or missing channel_data for presence channel", 4200)},
           state}

        params ->
          {reply, state} = subscribe_and_reply(channel, params, state)
          {:reply, {:text, reply}, state, :hibernate}
      end
    end
  end

  # Only called once the subscription is known to be valid, so a rejected
  # subscribe never touches occupancy, presence or webhooks.
  defp subscribe_and_reply(channel, params, state) do
    Registry.register(Naming.registry(state.name), channel, state.socket_id)
    Metrics.incr(state.name, :subscriptions_total)

    if Cluster.channel_joined(state.name, channel),
      do: Cluster.announce_channel_occupied(state.name, channel)

    state = %{state | channels: MapSet.put(state.channels, channel)}

    case params do
      :none ->
        {subscription_succeeded(channel, %{}), state}

      {:presence, user_id, user_info} ->
        is_new_member =
          Cluster.presence_add(state.name, channel, state.socket_id, user_id, user_info)

        if is_new_member do
          Cluster.announce_member_added(state.name, channel, user_id, user_info, state.socket_id)
        end

        data = presence_payload(Cluster.presence_members(state.name, channel))
        # remembered so this socket can re-register itself if the shared store loses it
        {subscription_succeeded(channel, data),
         %{state | presence: Map.put(state.presence, channel, {user_id, user_info})}}
    end
  end

  defp subscription_succeeded(channel, data) do
    Jason.encode!(%{
      event: "pusher_internal:subscription_succeeded",
      channel: channel,
      data: Jason.encode!(data)
    })
  end

  defp presence_payload(members) do
    %{
      "presence" => %{
        "count" => length(members),
        "ids" => Enum.map(members, fn {uid, _info} -> uid end),
        "hash" => Map.new(members, fn {uid, info} -> {uid, info} end)
      }
    }
  end

  # channel_data is client-controlled input — never trust its shape.
  defp presence_params(channel, channel_data) do
    if presence?(channel), do: parse_presence_channel_data(channel_data), else: :none
  end

  defp parse_presence_channel_data(channel_data) when is_binary(channel_data) do
    case Jason.decode(channel_data) do
      {:ok, %{"user_id" => user_id} = decoded} when is_binary(user_id) or is_integer(user_id) ->
        # Pusher user ids are strings on the wire; normalizing here keeps
        # "42" and 42 the same user, on every backend.
        {:presence, to_string(user_id), decoded["user_info"] || %{}}

      _ ->
        :error
    end
  end

  defp parse_presence_channel_data(_), do: :error

  # Only act on channels this socket is actually subscribed to; otherwise an
  # unsubscribe for a channel we never joined could fire a bogus
  # `channel_vacated` or drop someone else's presence.
  defp handle_unsubscribe(channel, state) do
    if MapSet.member?(state.channels, channel) do
      Registry.unregister(Naming.registry(state.name), channel)
      if presence?(channel), do: leave_presence(channel, state.socket_id, state.name)

      if Cluster.channel_left(state.name, channel),
        do: Cluster.announce_channel_vacated(state.name, channel)

      {:ok,
       %{
         state
         | channels: MapSet.delete(state.channels, channel),
           presence: Map.delete(state.presence, channel)
       }}
    else
      {:ok, state}
    end
  end

  defp leave_presence(channel, socket_id, name) do
    case Cluster.presence_remove(name, channel, socket_id) do
      {true, user_id} when not is_nil(user_id) ->
        Cluster.announce_member_removed(name, channel, user_id)

      _ ->
        :ok
    end
  end

  # ---- client events ----

  # Only allowed on private/presence channels, only from a client actually
  # subscribed to that channel — matches real Pusher's client-event
  # restrictions. Never allowed on public channels.
  defp handle_client_event(event_name, channel, data, state) do
    cond do
      not Config.get(state.name).client_messages_enabled ->
        error_reply("Client events are disabled on this server", state)

      not (private?(channel) or presence?(channel)) ->
        error_reply("Client events are not supported on public channels", state)

      not MapSet.member?(state.channels, channel) ->
        error_reply("Cannot send client event on a channel you're not subscribed to", state)

      true ->
        {allowed?, state} = client_event_allowed(state)

        if allowed? do
          relay_client_event(event_name, channel, data, state)
        else
          Metrics.incr(state.name, :client_events_rate_limited_total)
          error_reply("Client event rate limit exceeded", state)
        end
    end
  end

  defp relay_client_event(event_name, channel, data, state) do
    Metrics.incr(state.name, :client_events_total)

    Cluster.broadcast(
      state.name,
      channel,
      %{event: event_name, channel: channel, data: data},
      state.socket_id
    )

    Webhook.dispatch(state.name, [
      %{
        "name" => "client_event",
        "channel" => channel,
        "event" => event_name,
        "data" => data,
        "socket_id" => state.socket_id
      }
    ])

    {:ok, state}
  end

  # A fixed one-second window per connection; `nil` limit means unlimited.
  defp client_event_allowed(state) do
    case Config.get(state.name).client_event_rate_limit do
      nil ->
        {true, state}

      limit ->
        now = System.system_time(:second)
        count = if state.ce_window == now, do: state.ce_count + 1, else: 1
        {count <= limit, %{state | ce_window: now, ce_count: count}}
    end
  end

  # The browser's Origin header, or nil when absent (servers and apps don't send one).
  defp origin(req) do
    case :cowboy_req.header("origin", req) do
      :undefined -> nil
      value -> value
    end
  end

  # The peer address, or the address a trusted proxy put in `:client_ip_header`
  # (first entry of a comma-separated list such as X-Forwarded-For).
  defp client_ip(req, %{client_ip_header: nil}), do: peer_ip(req)

  defp client_ip(req, %{client_ip_header: header}) do
    case :cowboy_req.header(header, req) do
      :undefined -> peer_ip(req)
      value -> value |> String.split(",") |> hd() |> String.trim()
    end
  end

  defp peer_ip(req) do
    {ip, _port} = :cowboy_req.peer(req)
    ip |> :inet.ntoa() |> to_string()
  end

  defp over_capacity?(name) do
    case Config.get(name).max_connections do
      nil -> false
      max -> Registry.count_match(Naming.registry(name), @all_connections, :_) >= max
    end
  end

  defp error_reply(message, state), do: {:reply, {:text, error_json(message, 4301)}, state}

  # ---- helpers ----

  defp error_json(message, code),
    do: Jason.encode!(%{event: "pusher:error", data: %{message: message, code: code}})

  defp private?(channel), do: String.starts_with?(channel, "private-")
  # `exclude` is a socket id, `nil`, or `{:user, channel, user_id}`: every socket
  # that joined `channel` as that user (used so a user is never told about
  # their own arrival when the cluster re-announces members after a merge).
  defp excluded?(state, {:user, channel, user_id}),
    do: match?({^user_id, _info}, Map.get(state.presence, channel))

  defp excluded?(state, exclude_socket_id), do: state.socket_id == exclude_socket_id

  defp presence?(channel), do: String.starts_with?(channel, "presence-")

  defp generate_socket_id do
    "#{:rand.uniform(1_000_000_000)}.#{:rand.uniform(1_000_000_000)}"
  end
end
