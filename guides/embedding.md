# Embedding in your app

Add the dependency:

```elixir
# mix.exs
defp deps do
  [{:socketelx, "~> 0.5"}]
end
```

Start it from your supervision tree:

```elixir
# lib/my_app/application.ex
children = [
  MyApp.Repo,
  MyAppWeb.Endpoint,
  {Socketelx,
   name: MyApp.Socketelx,
   port: 6001,
   app_key: System.fetch_env!("SOCKETELX_APP_KEY"),
   app_secret: System.fetch_env!("SOCKETELX_APP_SECRET"),
   app_id: System.fetch_env!("SOCKETELX_APP_ID")}
]
```

Nothing starts automatically when the package is a dependency. Only the
standalone release (this repository's own `config/runtime.exs`) turns on
auto-start; a dependency's config files are ignored by the host app.

## It runs its own listener

`Socketelx` starts its own Cowboy listener on its own `:port`. It is not a
Plug you mount in your Phoenix router or endpoint. Your frontend connects to
that port (directly or through your proxy), and your backend sends events to
the same port over the REST API.

## Several instances

Each instance needs a unique `:name` and its own `:port`, and keeps fully
separate channel and presence state:

```elixir
children = [
  {Socketelx, name: MyApp.SocketelxA, port: 6001, app_id: "a", app_key: "ka", app_secret: "sa"},
  {Socketelx, name: MyApp.SocketelxB, port: 6002, app_id: "b", app_key: "kb", app_secret: "sb"}
]
```

## Sending events from Elixir

Publish directly, with no HTTP call:

```elixir
Socketelx.trigger(MyApp.Socketelx, "news", "update", %{hello: "world"})

# skip the sender's own connection
Socketelx.trigger(MyApp.Socketelx, "room-1", "message", %{text: "hi"}, socket_id: socket_id)
```

`data` can be any term `Jason` can encode, or a string that is sent untouched. With
Redis configured the event reaches clients on every node. See `Socketelx.trigger/5`.

## Authorizing private and presence channels

Private and presence channels need an endpoint in your app that signs each join.
After you have checked the user may join:

```elixir
# private channel
Socketelx.authorize_channel(MyApp.Socketelx, socket_id, "private-user-42")

# presence channel
Socketelx.authorize_channel(MyApp.Socketelx, socket_id, "presence-room-1",
  user_id: user.id, user_info: %{name: user.name})
```

Return the resulting map as JSON from the route pusher-js is configured to call.
See `Socketelx.authorize_channel/4`, and the
[chat example](../examples/chat/README.md) for a complete app.

You can also publish over HTTP from another service: the REST API is the standard
signed `POST /apps/:app_id/events` that any Pusher server library speaks.

## Draining on shutdown

When the standalone release stops, it drains automatically. If you embed and
want clients to reconnect cleanly before your app exits, call
`Socketelx.drain/1` from your application's `prep_stop/1`:

```elixir
@impl true
def prep_stop(state) do
  Socketelx.drain(MyApp.Socketelx)
  Process.sleep(1_000)
  state
end
```

It must run in `prep_stop/1`, not `stop/1`: by `stop/1` the supervision tree is
already gone and the sockets are already closed.

## Redis

Pass `redis_url: "redis://..."` to share state between several nodes. See
[Multi-server](multi-server.md).

## Without Redis (experimental)

If your application already runs as a cluster of Erlang nodes, pass
`cluster: :erlang` instead and Socketelx shares events, presence and channel
occupancy over that cluster, with no Redis. Give every node the same `app_id`
and make sure the nodes can reach each other. This mode is experimental; the
[Multi-server guide](multi-server.md#without-redis-erlang-cluster-experimental)
has the details and the limits.
