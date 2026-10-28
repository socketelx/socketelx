# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/), versioning
follows [Semantic Versioning](https://semver.org/).

## [0.5.2] - 2026-10-28

### Added
- **Terminate a user's connections** (`POST /apps/:app_id/users/:user_id/terminate_connections`,
  or `Socketelx.terminate_user_connections/3`). Closes every connection of a signed-in user
  on every node, for logout or a ban. Works across Redis and Erlang cluster nodes.

## [0.5.1] - 2026-10-23

### Fixed
- REST request bodies are capped at 1 MB; a bigger one gets `413`. Before, the body was
  read in full (up to Plug's 8 MB) before the signature was checked, and anything
  larger crashed the request with a 500.
- WebSocket frames are capped at 100 KB; a bigger one closes the connection (code
  1009). Cowboy's default is no limit, so one client could make the server buffer an
  arbitrarily large frame. Real Pusher frames are a few hundred bytes (client events
  are limited to 10 KB by Pusher).

## [0.5.0] - 2026-10-23

### Added
- **Redis Cluster support.** Set `redis_cluster: true` (`SOCKETELX_REDIS_CLUSTER=true`)
  and point `redis_url` at any node of the cluster; `redis_cluster_nodes`
  (`SOCKETELX_REDIS_CLUSTER_NODES`) adds more places to look. All of an app's keys carry
  one hash tag, so they stay in one slot and the atomic presence and occupancy scripts
  keep working. Socketelx finds the node that owns the slot, follows it through
  failovers and resharding, and moves its pub/sub subscription with it. Tested against a
  real 3-master, 3-replica cluster, including a manual failover and killing the owning
  master under load. Standalone Redis, Sentinel and single-endpoint managed services work
  as before and are unchanged. See [Multi-server](guides/multi-server.md#with-redis-cluster).
- CI runs the Redis tests a second time against a real Redis Cluster.

### Notes
- One app's state lives on one shard. Several apps can spread over shards.
- Delivery between nodes pauses for a few seconds during a Redis failover, like any
  other Redis interruption. Delivery is still at-most-once.

## [0.4.3] - 2026-10-23

### Changed
- Setting a limit to `0` now means "no limit" (`rest_rate_limit`,
  `client_event_rate_limit`, `max_connections`, `max_connections_per_ip`, in options
  and in the matching `SOCKETELX_*` variables). Before, `0` refused every request or
  connection. Negative numbers also mean no limit. If you used `0` on purpose to block
  everything, use `1` instead.
- `bench/load.mjs`: new `--workers N` runs the generator in several processes, so a
  single Node process no longer caps the test at about 50k messages per second.
- *Going to production* guide: new section on CPU cores and Erlang schedulers.

## [0.4.2] - 2026-10-17

### Changed
- **Idle connections use about 3.5 times less memory.** WebSocket processes now
  hibernate while they wait: 10,000 subscribed connections went from about 570 MB to
  about 200 MB in a measurement (about 15 KB each). No change in latency or CPU per
  message.
- `bench/load.mjs`: accepts `--option=value`, rejects unknown options, sends keep-alive
  pings like real clients, checks credentials before starting, reports why sends and
  connections failed, and takes `--targets a:6001,b:6001` to test several nodes.

## [0.4.1] - 2026-10-17

### Added
- Guides: *Going to production* (open-file limits, proxy and OS settings, monitoring)
  and *Capacity planning* (what a connection and a message cost, with an 8 GB example).

## [0.4.0] - 2026-10-16

### Added
- **Several apps in one server** (`apps: [...]` / `SOCKETELX_APPS` JSON). Each app
  has its own id, key, secret and settings, and is fully isolated (channels,
  presence, webhooks, limits, metrics). Metrics gain an `app` label when there is
  more than one app. Single-app setups are unchanged. `Socketelx.trigger/5` and
  `authorize_channel/4` take `app:` on multi-app servers.
- **Allowed origins** (`allowed_origins` / `SOCKETELX_ALLOWED_ORIGINS`): exact
  origins, bare hosts and `*.` wildcards. Default is to accept every origin.
  Refused connections get `pusher:error` 4009.
- `GET /apps/:id/channels/:channel/users` (presence channel members).
- User sign-in: `pusher:signin`, events to a user through
  `#server-to-user-<id>`, and `Socketelx.authenticate_user/4`.
- `max_connections_per_ip` and `client_ip_header` (error 4100).
- `webhook_queue_path`: failed webhooks are kept on disk and sent again after a
  restart.
- Built-in TLS (`tls_certfile` / `tls_keyfile`).
- Chat example demo GIF.

## [0.3.0] - 2026-10-16

### Added
- **Experimental:** Erlang cluster mode (`cluster: :erlang` /
  `SOCKETELX_CLUSTER=erlang`). Several nodes share events, presence, channel
  occupancy and webhooks over Erlang's own clustering, with no Redis. Nodes can
  find each other from `SOCKETELX_CLUSTER_NODES` or `SOCKETELX_CLUSTER_DNS`, or
  use a cluster your app already has. Survives a node dying, the leader dying
  and a network split that later heals. Tested across real separate nodes.
  See the Multi-server guide for setup and limits.
- `docker-compose.cluster.example.yml`, a two-node example with no Redis.

## [0.2.0] - 2026-10-09

### Changed (breaking)
- **Renamed from `pusher_server` to Socketelx.** The Hex package, OTP app and release
  are `socketelx`, the main module is `Socketelx` (was `PusherServer`), and the Docker image
  is `socketelx`. Update `{:pusher_server, ...}` to `{:socketelx, ...}` and
  `{PusherServer, ...}` to `{Socketelx, ...}`.
- Environment variables are now `SOCKETELX_*`. The old `PUSHER_*` names are still
  accepted, and `SOCKETELX_*` wins when both are set.
- Metric names start with `socketelx_` (was `pusher_server_`), and Redis keys start
  with `socketelx:` (was `pusher:`). Redis state is ephemeral, so nothing needs migrating.

### Added
- `Socketelx.trigger/5` publishes an event straight from your Elixir code, and
  `Socketelx.authorize_channel/4` signs private and presence channel joins,
  so an embedded app needs no HTTP round trip.
- `examples/chat`: a complete chat app (presence, messages, typing indicator) as
  a standalone Docker version and an embedded Elixir version, with a smoke test.
- `bench/load.mjs` and a load-testing guide to simulate many connections.
- Documentation reworked around `pusher-js` and the official Pusher server
  libraries; Laravel setup moved to its own guide.
- `POST /apps/:app_id/batch_events` (up to 10 events per request) and
  `GET /apps/:app_id/channels` (with `filter_by_prefix` and
  `info=user_count`), both working across nodes with Redis.
- The Docker workflow can also publish to Docker Hub, and tags semver versions and `latest`.
- `SOCKETELX_BATCH_LIMIT` (default 10) sets the `batch_events` size limit.
- Webhook retries with exponential backoff (`SOCKETELX_WEBHOOK_RETRIES`,
  default 3). 5xx, 408, 429 and network errors are retried; other 4xx are not.
- Optional limits, all off by default: `SOCKETELX_REST_RATE_LIMIT`,
  `SOCKETELX_CLIENT_EVENT_RATE_LIMIT`, `SOCKETELX_MAX_CONNECTIONS`.
- Optional Prometheus metrics at `GET /metrics` (`SOCKETELX_METRICS`,
  `SOCKETELX_METRICS_TOKEN`).
- Multi-server mode: set `SOCKETELX_REDIS_URL` (or the `:redis_url` option) and
  several nodes share broadcasts, presence and channel occupancy through
  Redis. A janitor heartbeats each node, removes state left by dead nodes
  and rebuilds a node's state if Redis loses its data. Single-node use is
  unchanged and still needs no Redis.
- `deploy/nginx.conf` and a two-node `docker-compose.example.yml`
- Redis service in CI so the multi-node tests run there
- User guides (`guides/`), grouped API docs, CONTRIBUTING, SECURITY, code of
  conduct, and issue / PR templates
- A warning is logged when the public default app secret is in use

### Changed
- The server now advertises a 30 s `activity_timeout` and closes connections
  silent for 60 s. Both were 120 s, so a quiet client's ping could race the
  server's own idle timeout.

### Fixed
- Several ExDoc warnings (hidden-function links, missing `Config.t()` type).
- Graceful shutdown never reached clients: the drain ran in
  `Application.stop/1`, which OTP calls after the supervision tree is gone.
  It now runs in `prep_stop/1`, with a regression test.
- CI's `mix format --check-formatted` step would have failed; the code is now
  formatted and `.formatter.exs` knows the `plug` macro.
- `config/runtime.exs` no longer starts a listener under `mix test`.
- Dockerfile copies `mix.lock` when present.

## [0.1.0] - 2026-07-09

### Added
- Pusher protocol websocket handshake, subscribe/unsubscribe, ping/pong
- Private and presence channel auth (HMAC-SHA256)
- Presence membership tracking, deduped by `user_id` across connections
- REST trigger endpoint (`POST /apps/:app_id/events`)
- Client events (`client-*`) on private/presence channels
- Webhooks (`channel_occupied`/`vacated`, `member_added`/`removed`, `client_event`)
- Read-only channel info endpoint (`GET /apps/:app_id/channels/:channel`)
- Graceful shutdown (drains connections on SIGTERM) and idle connection timeout
- Test suite for signature verification, presence semantics, and webhook payloads

### Known limitations
- Redis Cluster (sharded) is not supported; use a single Redis endpoint
- No SSL termination built in (put a reverse proxy in front)
- No rate limiting or connection caps
- No batch events endpoint
