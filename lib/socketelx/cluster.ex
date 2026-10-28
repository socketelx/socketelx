defmodule Socketelx.Cluster do
  @moduledoc """
  The one place the websocket handler and HTTP router go through for
  anything that may need to reach *other* nodes: broadcasting, presence,
  and channel occupancy.

  It always does the node-local part itself (delivering to sockets
  connected to this node via the instance's `Registry`) and then asks the
  configured `Socketelx.Backend` to carry the rest across the cluster.
  With the default `Local` backend that second step is a no-op, so a
  single node behaves exactly as before.
  """

  alias Socketelx.{Config, Naming, Webhook}

  @doc "The backend module configured for this instance."
  def backend(name), do: Config.get(name).backend

  # ---- broadcasting ----

  @doc """
  Delivers `message` (a map, or an already-encoded JSON string) to every
  subscriber of `channel` on this node, then to every other node through
  the backend. `exclude_socket_id` is skipped everywhere.
  """
  def broadcast(name, channel, message, exclude_socket_id \\ nil) do
    encoded = if is_binary(message), do: message, else: Jason.encode!(message)
    local_dispatch(name, channel, encoded, exclude_socket_id)
    backend(name).publish(name, channel, encoded, exclude_socket_id)
    :ok
  end

  @terminate_message ~s({"event":"pusher_internal:terminate_connection"})

  @doc false
  # A control message, not an event: connections that receive it close themselves. It is
  # recognised by being exactly this string, which no event sent through `trigger/5` or the
  # REST API can produce (those always carry `channel` and `data` as well).
  def terminate_message, do: @terminate_message

  @doc """
  Closes every connection of `user_id` on every node (the user must have signed in
  with `pusher:signin`). Returns `:ok` whether or not the user is connected.
  """
  def terminate_user(name, user_id) do
    broadcast(name, "#server-to-user-" <> to_string(user_id), @terminate_message)
  end

  @doc "Delivers to this node's subscribers only. Used for messages that arrived from other nodes."
  def local_dispatch(name, channel, encoded, exclude_socket_id) do
    Registry.dispatch(Naming.registry(name), channel, fn entries ->
      for {pid, _socket_id} <- entries, do: send(pid, {:broadcast, encoded, exclude_socket_id})
    end)
  end

  # ---- channel occupancy ----

  @doc "Records a local subscribe. `true` if the channel just became occupied cluster-wide."
  def channel_joined(name, channel), do: backend(name).channel_joined(name, channel)

  @doc "Records a local unsubscribe. `true` if the channel just became vacant cluster-wide."
  def channel_left(name, channel), do: backend(name).channel_left(name, channel)

  @doc "Total subscribers on `channel` across the cluster."
  def subscription_count(name, channel), do: backend(name).subscription_count(name, channel)

  @doc "Names of every channel with at least one subscriber, across the cluster."
  def list_channels(name), do: backend(name).list_channels(name)

  # ---- presence ----

  @doc "Adds a presence entry. `true` if this is the user's first connection on the channel, cluster-wide."
  def presence_add(name, channel, socket_id, user_id, user_info),
    do: backend(name).presence_add(name, channel, socket_id, user_id, user_info)

  @doc "Removes a presence entry. Returns `{is_users_last_connection, user_id}`."
  def presence_remove(name, channel, socket_id),
    do: backend(name).presence_remove(name, channel, socket_id)

  @doc "Unique members of a presence channel as `{user_id, user_info}` pairs."
  def presence_members(name, channel), do: backend(name).presence_members(name, channel)

  @doc "Silently re-records an already-announced presence entry (used after Redis lost its data)."
  def presence_restore(name, channel, socket_id, user_id, user_info),
    do: backend(name).presence_restore(name, channel, socket_id, user_id, user_info)

  @doc false
  def member_added_json(channel, user_id, user_info) do
    Jason.encode!(%{
      event: "pusher_internal:member_added",
      channel: channel,
      data: Jason.encode!(%{user_id: user_id, user_info: user_info})
    })
  end

  @doc false
  def member_removed_json(channel, user_id) do
    Jason.encode!(%{
      event: "pusher_internal:member_removed",
      channel: channel,
      data: Jason.encode!(%{user_id: user_id})
    })
  end

  @doc """
  Tells the cluster (and the webhook, if configured) that a user joined a
  presence channel. `exclude` is a socket id, `{:user, channel, user_id}` to
  skip every socket of that user on that channel, or `nil`.
  """
  def announce_member_added(name, channel, user_id, user_info, exclude) do
    broadcast(name, channel, member_added_json(channel, user_id, user_info), exclude)

    Webhook.dispatch(name, [
      %{"name" => "member_added", "channel" => channel, "user_id" => user_id}
    ])
  end

  @doc "Tells the cluster (and the webhook, if configured) that a user's last connection left a presence channel."
  def announce_member_removed(name, channel, user_id) do
    broadcast(name, channel, member_removed_json(channel, user_id))

    Webhook.dispatch(name, [
      %{"name" => "member_removed", "channel" => channel, "user_id" => user_id}
    ])
  end

  @doc "Fires the `channel_occupied` webhook."
  def announce_channel_occupied(name, channel),
    do: Webhook.dispatch(name, [%{"name" => "channel_occupied", "channel" => channel}])

  @doc "Fires the `channel_vacated` webhook."
  def announce_channel_vacated(name, channel),
    do: Webhook.dispatch(name, [%{"name" => "channel_vacated", "channel" => channel}])
end
