# Configuration

Every setting can be passed as an option to `Socketelx.start_link/1` (or as
`{Socketelx, opts}` in a supervision tree) or set as an environment
variable. An explicit option wins over the environment variable.

Environment variables are named `SOCKETELX_*`. The older `PUSHER_*` spelling of the
same names (`PUSHER_APP_ID`, `PUSHER_REDIS_URL`, ...) is still accepted, so an
existing deployment keeps working; if both are set, `SOCKETELX_*` wins.

| Option                     | Env var                         | Default            | Notes |
|----------------------------|---------------------------------|--------------------|-------|
| `:app_id`                  | `SOCKETELX_APP_ID`                 | `"app-id"`         | Must match what your backend's Pusher library uses. Also namespaces Redis keys. |
| `:app_key`                 | `SOCKETELX_APP_KEY`                | `"app-key"`        | Public; used in the WebSocket URL `/app/:app_key`. |
| `:app_secret`              | `SOCKETELX_APP_SECRET`             | `"app-secret"`     | Keep private. The default is public and logs a warning. |
| `:port`                    | `PORT`                          | `6001`             | Plain HTTP/WS. Put TLS in front. |
| `:webhook_url`             | `SOCKETELX_WEBHOOK_URL`            | none               | Where webhooks are POSTed. |
| `:client_messages_enabled` | `SOCKETELX_ENABLE_CLIENT_MESSAGES` | `true`             | `false` rejects all `client-*` events. |
| `:redis_url`               | `SOCKETELX_REDIS_URL`              | none               | Enables multi-server mode. `rediss://` for TLS. |
| `:redis_cluster`           | `SOCKETELX_REDIS_CLUSTER`          | `false`            | The Redis URL points at a Redis Cluster. See [Multi-server](multi-server.md#with-redis-cluster). |
| `:redis_cluster_nodes`     | `SOCKETELX_REDIS_CLUSTER_NODES`    | none               | More `host:port` nodes to ask if the URL's node is down, comma-separated. |
| `:redis_opts`              | none                            | `[]`               | Extra [Redix](https://hexdocs.pm/redix) options, e.g. `ssl: true`. |
| `:webhook_retries`         | `SOCKETELX_WEBHOOK_RETRIES`        | `3`                | Retries after the first attempt. `0` turns retries off. |
| `:webhook_retry_base_ms`   | none                            | `1000`             | First retry delay; doubles each time. |
| `:rest_rate_limit`         | `SOCKETELX_REST_RATE_LIMIT`        | none               | Max REST API requests per second for the instance. `0` = no limit. |
| `:client_event_rate_limit` | `SOCKETELX_CLIENT_EVENT_RATE_LIMIT`| none               | Max client events per second, per connection. `0` = no limit. |
| `:max_connections`         | `SOCKETELX_MAX_CONNECTIONS`        | none               | Max WebSocket connections on this node. `0` = no limit. |
| `:batch_limit`             | `SOCKETELX_BATCH_LIMIT`            | `10`               | Max events in one `batch_events` request. Pusher's own limit is 10. |
| `:metrics`                 | `SOCKETELX_METRICS`                | `false`            | Serve Prometheus metrics at `GET /metrics`. |
| `:metrics_token`           | `SOCKETELX_METRICS_TOKEN`          | none               | Require `Authorization: Bearer <token>` on `/metrics`. |
| `:cluster`                 | `SOCKETELX_CLUSTER`                | none               | `erlang` turns on the experimental Redis-free cluster mode. Not combinable with `:redis_url`. See [Multi-server](multi-server.md). |
| `:cluster_nodes`           | `SOCKETELX_CLUSTER_NODES`          | none               | Comma-separated Erlang node names to connect to (`socketelx@host1,socketelx@host2`). Optional if your app already forms a cluster. |
| `:cluster_dns`             | `SOCKETELX_CLUSTER_DNS`            | none               | A hostname that resolves to every node's IP; nodes are named `<basename>@<ip>`. |
| `:cluster_node_basename`   | `SOCKETELX_CLUSTER_NODE_BASENAME`  | `socketelx`        | The part before the `@` used with `:cluster_dns`. |
| `:name`                    | none                            | `Socketelx`     | Embedded only. Unique per instance. |

In Docker, cluster mode also uses the release's own variables, which are not
Socketelx settings: `RELEASE_COOKIE` (shared secret, required in practice),
`RELEASE_NODE` and `RELEASE_DISTRIBUTION`. See [Multi-server](multi-server.md).

| `:allowed_origins`         | `SOCKETELX_ALLOWED_ORIGINS`        | all origins        | Which websites may open a WebSocket. See [Allowed origins](#allowed-origins). |
| `:max_connections_per_ip`  | `SOCKETELX_MAX_CONNECTIONS_PER_IP` | none               | Simultaneous WebSocket connections per client IP. See [Per-IP limit](#per-ip-limit). |
| `:client_ip_header`        | `SOCKETELX_CLIENT_IP_HEADER`       | none               | Take the client IP from this header (behind a proxy). |
| `:webhook_queue_path`      | `SOCKETELX_WEBHOOK_QUEUE_PATH`     | none               | Directory where failed webhooks wait, so they survive a restart. |
| `:tls_certfile`, `:tls_keyfile` | `SOCKETELX_TLS_CERTFILE`, `SOCKETELX_TLS_KEYFILE` | none | Serve HTTPS and WSS directly, without a proxy. |
| `:apps`                    | `SOCKETELX_APPS`                   | none               | Several apps in one server. See [Several apps](multi-app.md). |

## One app, or several

By default a server has one app (one id, key and secret). To serve several apps
from one port, see [Several apps in one server](multi-app.md).

## Allowed origins

Browsers send an `Origin` header when opening a WebSocket. By default every
origin is accepted. To accept only your own sites:

```bash
SOCKETELX_ALLOWED_ORIGINS=https://chat.example.com,*.example.org
```

Each entry is one of:

| Entry | Matches |
|---|---|
| `https://chat.example.com` | exactly that origin (scheme and port included) |
| `chat.example.com` | that host on any scheme or port |
| `*.example.org` | any subdomain of `example.org`, but not `example.org` itself |
| `*` | everything (the default) |

A refused connection receives `pusher:error` 4009 and is closed. Connections
without an `Origin` header (backend scripts, mobile apps) are not affected: this
stops other websites from using your visitors' browsers, it is not a login. In
Elixir, pass a list: `allowed_origins: ["https://chat.example.com"]`.

## Per-IP limit

`SOCKETELX_MAX_CONNECTIONS_PER_IP=20` allows each client address at most 20
simultaneous WebSocket connections (more get `pusher:error` 4100). Behind a
proxy or tunnel every connection looks like it comes from the proxy, so also set
`SOCKETELX_CLIENT_IP_HEADER` to the header your proxy fills in with the real
address (`cf-connecting-ip` on Cloudflare, `x-forwarded-for` for most others;
the first address in a list is used). Only set it when the proxy overwrites that
header, otherwise clients can fake it.

## Built-in TLS

Set `SOCKETELX_TLS_CERTFILE` and `SOCKETELX_TLS_KEYFILE` (PEM files) and the
server speaks HTTPS and WSS on its port. Most deployments terminate TLS at a
proxy or Cloudflare instead, which is fine; use this when nothing sits in front.
Certificates are read at start, so restart to renew.

## User sign-in and events to one user

`pusher.signin()` in `pusher-js` identifies the person behind a connection.
Point `userAuthentication.endpoint` at a route that calls
`Socketelx.authenticate_user/4` (or signs `socket_id::user::user_data` with
HMAC-SHA256 in any language). Once signed in, an event triggered on the channel
`#server-to-user-<id>` reaches every connection of that user, on any node.

## Client events

Clients can send `client-*` events on private and presence channels, only to
channels they are subscribed to, and never on public channels. Set
`SOCKETELX_ENABLE_CLIENT_MESSAGES=false` to refuse them all (the client receives
error 4301).

## Webhooks

With `SOCKETELX_WEBHOOK_URL` set, the server POSTs JSON for these events:

- `channel_occupied`, `channel_vacated`
- `member_added`, `member_removed` (presence channels)
- `client_event`

Each request carries `X-Pusher-Key` and `X-Pusher-Signature` (HMAC-SHA256 of the
raw body with your app secret), the same as Pusher's webhooks. Verify the
signature before trusting a request.

Delivery runs in the background and never blocks a connection. A failed
delivery (network error, timeout, HTTP 5xx, 408 or 429) is retried up to
`SOCKETELX_WEBHOOK_RETRIES` times (default 3) with a doubling delay: 1 s, 2 s,
4 s. Other 4xx responses are not retried, because the receiver rejected the
payload. Two things to design for:

- By default retries are held in memory, so a webhook still waiting for a retry
  is lost if the node stops. Set `SOCKETELX_WEBHOOK_QUEUE_PATH` to a directory
  (a mounted volume in Docker) and a webhook that failed its first attempt is
  saved there and sent again after a restart. It protects against the node
  restarting, not against losing the disk.
- Retried events can arrive after later events, so make your receiver
  idempotent and don't rely on order.

## Rate limits and capacity

All are off by default. Setting one to `0` also means "no limit", so you can keep the
variable in a template and turn the limit off without deleting it.

- `SOCKETELX_REST_RATE_LIMIT=100` allows 100 REST API requests per second for the
  whole instance (the health check and `/metrics` are exempt). Extra requests
  get `429` with `Retry-After: 1`. Most server libraries treat that as a failed
  publish, so set it above your real peak.
- `SOCKETELX_CLIENT_EVENT_RATE_LIMIT=10` allows each connection 10 client events
  per second. Extra events are dropped and the client gets `pusher:error` 4301.
- `SOCKETELX_MAX_CONNECTIONS=10000` refuses new WebSocket connections on this
  node beyond that count with `pusher:error` 4004. Clients reconnect, and with
  a load balancer they may land on another node.

The limits are per instance and per node. With several nodes, each node
applies its own limit.

## Metrics

Set `SOCKETELX_METRICS=true` and scrape `GET /metrics` (Prometheus text format).
Counters are per node and reset on restart; scrape every node.

| Metric | Type |
|---|---|
| `socketelx_connections` | gauge: open WebSocket connections on this node |
| `socketelx_channels` | gauge: channels with a subscriber on this node |
| `socketelx_connections_total`, `socketelx_connections_rejected_total` | counter |
| `socketelx_subscriptions_total` | counter |
| `socketelx_events_triggered_total` | counter: events delivered via the REST API, one per channel |
| `socketelx_client_events_total`, `socketelx_client_events_rate_limited_total` | counter |
| `socketelx_rest_requests_total`, `socketelx_rest_rate_limited_total` | counter |
| `socketelx_webhooks_sent_total`, `socketelx_webhook_retries_total`, `socketelx_webhooks_failed_total` | counter |

Set `SOCKETELX_METRICS_TOKEN` to require `Authorization: Bearer <token>`. Without
a token anyone who can reach the port can read `/metrics`, so block that path
at your proxy.

## Redis

See [Multi-server](multi-server.md). Redis is used only when `:redis_url` is set.
