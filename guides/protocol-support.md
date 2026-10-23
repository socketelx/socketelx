# Protocol support

The Pusher Channels protocol, as used by `pusher-js`, `laravel-echo` and the
official Pusher server libraries. Only what is listed under "Supported" is implemented;
anything else is unsupported.

## Supported

**WebSocket** at `/app/:app_key`

- `pusher:connection_established` on connect (advertises a 30 s activity timeout)
- `pusher:subscribe`, `pusher:unsubscribe`
- public, private and presence channels
- channel auth: HMAC-SHA256 over `socket_id:channel` (plus `:channel_data` for
  presence)
- `pusher:ping` / `pusher:pong`
- client events (`client-*`) on private and presence channels
- idle connections are closed after 60 s of silence
- size limits: a WebSocket frame over 100 KB closes the connection (code 1009) and a
  REST body over 1 MB gets `413`. Pusher's own limits (10 KB per event) are lower and are
  not enforced here
- user sign-in (`pusher:signin`, `pusher:signin_success`) and events to a user
  through the channel `#server-to-user-<id>`
- browser origin checks (`:allowed_origins`), per-IP connection limit

**HTTP**

| Endpoint | Purpose |
|---|---|
| `GET /` | Health check, returns `ok` |
| `POST /apps/:app_id/events` | Trigger an event on one or more channels |
| `POST /apps/:app_id/batch_events` | Trigger up to 10 events, configurable with `SOCKETELX_BATCH_LIMIT` (`{"batch": [{channel, name, data, socket_id?}]}`) |
| `GET /apps/:app_id/channels` | List occupied channels. `filter_by_prefix`, and `info=user_count` with `filter_by_prefix=presence-` |
| `GET /apps/:app_id/channels/:channel` | Occupancy and subscription count (and `user_count` for presence channels) |
| `GET /apps/:app_id/channels/:channel/users` | Ids of the users in a presence channel |
| `GET /metrics` | Prometheus metrics, off unless enabled; see [Configuration](configuration.md#metrics) |

REST requests are signed like Pusher's API: sorted query parameters,
`METHOD\nPATH\nquery`, `body_md5`, and an `auth_timestamp` within 10 minutes.

**Webhooks**: `channel_occupied`, `channel_vacated`, `member_added`,
`member_removed`, `client_event`. Signed with `X-Pusher-Key` and
`X-Pusher-Signature`. Failed deliveries are retried, and can be kept on disk
across restarts (see
[Configuration](configuration.md#webhooks)).

## Error codes sent to clients

| Code | Meaning |
|---|---|
| 4000 | Server is shutting down; reconnect |
| 4001 | App key does not exist |
| 4009 | Invalid channel auth or sign-in signature, or origin not allowed |
| 4200 | Invalid `channel_data` |
| 4004 | Over capacity (`:max_connections` reached) |
| 4100 | Too many connections from one address (`:max_connections_per_ip`) |
| 4301 | Client event not allowed, or over the client-event rate limit |

## Not implemented

- terminating a user's connections (`POST /users/:id/terminate_connections`)
  and the sign-in watchlist (online/offline events). Unknown events are
  silently ignored.
- per-IP *request* rate limits (the per-IP limit counts WebSocket connections)
- message replay or caching channels: delivery is at-most-once
