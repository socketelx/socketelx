# Running several servers

One node is enough for many applications and needs no other service. Run
several nodes when you need more capacity or want to restart without
disconnecting everyone at once. There are two ways to let nodes share events,
presence and channel occupancy:

| | Redis (this page, first half) | Erlang cluster (second half) |
|---|---|---|
| Status | Stable. Recommended. | **Experimental.** |
| Needs | A Redis server | Nodes that can reach each other over Erlang distribution |
| Best for | Any backend language, Docker, most setups | Elixir teams who already cluster their nodes, or who want no Redis |
| Scale | Tested with the Redis failure cases below | Suits tens of thousands of connections; see its limits below |

Use one or the other, never both: setting a Redis URL and `cluster: :erlang`
together is rejected at startup.

## With Redis

Nodes coordinate through a single Redis.

## Setup

On every node set the **same**:

- `SOCKETELX_APP_ID`, `SOCKETELX_APP_KEY`, `SOCKETELX_APP_SECRET`
- `SOCKETELX_REDIS_URL`, for example `redis://:password@redis:6379` (`rediss://` for TLS)

Put a load balancer in front. `deploy/nginx.conf` is an example and
`docker-compose.example.yml` runs two nodes, Redis and nginx together:

```bash
docker compose -f docker-compose.example.yml up --build
```

No sticky sessions are needed. A WebSocket stays on the node it connected to,
and REST calls from your backend can land on any node.

Embedded: `{Socketelx, name: MyApp.Socketelx, redis_url: "redis://localhost:6379", ...}`.

## What is shared

- An event sent to any node reaches subscribers on every node.
- Presence is shared. `member_added` / `member_removed` fire once per user for
  the whole cluster.
- `channel_occupied` / `channel_vacated` webhooks fire once cluster-wide.
- The channel info endpoint counts subscribers on all nodes, and the channel
  list (`GET /apps/:app_id/channels`) includes channels from every node.
- Rate limits, `SOCKETELX_MAX_CONNECTIONS` and metrics are per node, not shared.

## When things fail

| What happens | Result |
|---|---|
| Redis becomes unreachable | Every node keeps serving its own clients. Delivery between nodes pauses and resumes when Redis returns. Nothing crashes. |
| Redis restarts empty or is flushed | Running nodes write their subscriptions and presence back within about 10 seconds. Clients are not re-announced as new members. |
| A node crashes or is killed | Its heartbeat key expires (30 s); a surviving node then removes its presence and occupancy and announces `member_removed` / `channel_vacated`. Expect ghost members for roughly 30 to 40 seconds after a hard crash. |
| A node is stopped cleanly | Clients are told to reconnect, then connect to another node. |

## Things to know

- Redis holds only live, ephemeral state. It needs no persistence and stays small.
- Delivery is at-most-once. A client that is disconnected when an event is sent
  does not receive it later.
- Joining or leaving a presence channel scans that channel's members inside
  Redis. That is fine for normal presence channels (Pusher itself caps them at
  100 members) but not for huge ones.
- Keys are prefixed `socketelx:<app_id>`, so several apps can share one Redis.
- Redis Cluster works too, see the next section. Without `redis_cluster`, use a
  single endpoint: plain, Sentinel, or a managed primary/replica service.

## With Redis Cluster

Use this when your Redis is a Redis Cluster (several shards), for example a managed
service in "cluster mode". Add one setting on every node:

```
SOCKETELX_REDIS_URL=redis://:password@redis-node-1:6379   # any one node of the cluster
SOCKETELX_REDIS_CLUSTER=true
SOCKETELX_REDIS_CLUSTER_NODES=redis-node-2:6379,redis-node-3:6379   # optional
```

Embedded: `redis_url: "redis://..."`, `redis_cluster: true`, and optionally
`redis_cluster_nodes: ["redis-node-2:6379"]`.

The URL is the first place Socketelx asks where things are, and it uses the same
username, password and TLS settings for every node it then discovers. The optional
node list gives it other places to ask if that first node is down at startup.

**How it works.** All the keys of one app share a hash tag (`{socketelx:<app_id>}`),
so Redis keeps them in one slot, on one node. Socketelx finds that node with
`CLUSTER SLOTS`, talks to it, and follows it when it changes: a failover, a
resharding, or a node going away. Broadcasts use ordinary pub/sub, which Redis
Cluster passes on to every node.

**What to expect.**

- Everything in "What is shared" and "When things fail" above still applies.
- A failover or a move of the slot pauses delivery between Socketelx nodes for a few
  seconds: the time Redis itself needs to elect a new master, plus up to about two
  seconds for Socketelx to notice. Events sent in that window reach only the sockets
  on the node that received them (at-most-once, like everything here). In one test
  (two Socketelx nodes, the owning master killed with `kill -9`, Redis
  `cluster-node-timeout` 3 seconds) deliveries between the nodes were back after about
  6 seconds and nothing needed restarting.
- One app's state lives on **one** shard (plus its replicas). The other shards hold
  none of it. State is small (a few keys per channel), so this is not a limit on how
  many connections you can serve, but adding shards does not add capacity for a
  single app. With several apps (see [Several apps](multi-app.md)) each app has its
  own tag and may land on a different shard.
- Only database 0 exists in a cluster. Leave any `/N` off the URL.
- Not needed: sticky sessions, or anything in front of Redis.

## Checking it works

Connect a client to node 1, send an event through node 2's REST endpoint, and
confirm the client receives it. Then stop a node and restart Redis and confirm
clients and presence recover.

## Without Redis: Erlang cluster (experimental)

> **Experimental.** It passes automated tests across real separate nodes, but
> it has seen far less real-world use than the Redis mode. Try it on staging,
> read the limits below, and prefer Redis for anything critical. Its behaviour
> and settings may still change before 1.0.

Elixir and Erlang nodes can talk to each other directly, so Socketelx can
share state with no extra service. Turn it on and tell the nodes about each
other:

```bash
SOCKETELX_CLUSTER=erlang
```

or, embedded, `{Socketelx, name: MyApp.Socketelx, cluster: :erlang, ...}`.

Every node needs the same app id, key and secret, as with Redis.

### Standalone (Docker)

The image is an Elixir release, so the Erlang node settings are the usual
release variables. On every node:

| Variable | What to set |
|---|---|
| `SOCKETELX_CLUSTER` | `erlang` |
| `RELEASE_COOKIE` | The **same long random secret on every node** (for example `openssl rand -hex 32`). It is the only thing that authenticates nodes to each other. |
| `RELEASE_NODE` / `RELEASE_DISTRIBUTION` | The node's own name. The default (`socketelx@<hostname>`, short names) works when each node has a resolvable hostname, as in the Compose example. Use `RELEASE_DISTRIBUTION=name` with `RELEASE_NODE=socketelx@<ip-or-fqdn>` when nodes find each other by IP. |
| `SOCKETELX_CLUSTER_NODES` | Comma-separated names of the other nodes, for example `socketelx@socketelx-1,socketelx@socketelx-2`. |
| `SOCKETELX_CLUSTER_DNS` | Alternative to the list: a hostname that resolves to every node's IP (a Kubernetes headless service, a Docker Swarm `tasks.<service>` name). Nodes are then expected to be named `socketelx@<ip>`, so set `RELEASE_DISTRIBUTION=name` and `RELEASE_NODE=socketelx@$POD_IP`. Change the part before the `@` with `SOCKETELX_CLUSTER_NODE_BASENAME`. |

A ready-to-run two-node example without Redis is in
`docker-compose.cluster.example.yml`:

```bash
CLUSTER_COOKIE=$(openssl rand -hex 32) docker compose -f docker-compose.cluster.example.yml up --build
```

Nodes keep retrying every few seconds, so start order does not matter.

### Embedded

If your application already forms a cluster (for example a Phoenix app with
`dns_cluster` or `libcluster`), add `cluster: :erlang` to the Socketelx child
spec and you are done. Otherwise pass `cluster_nodes: [:"app@host1", ...]` or
`cluster_dns: "my-app.internal"`. The application must be started as a named
node (`--sname` / `--name`, or as a release).

### What is shared

The same as with Redis: events reach every node, presence and the
`member_added` / `member_removed` / `channel_occupied` / `channel_vacated`
webhooks fire once for the whole cluster, and the channel list and counts cover
all nodes. Rate limits, the connection cap and metrics stay per node.

### How it works

- **Events** are delivered to the sender's own sockets, then sent straight to
  the other nodes. There is no central point in the path.
- **Presence and occupancy** are decided by one *leader* node (the same one
  every node picks, with no voting) so a user's first and last connection are
  recognised exactly once. Every other node keeps a standby copy.

### When things fail

| What happens | Result |
|---|---|
| A node is stopped cleanly | Clients are told to reconnect; the leader announces what left. |
| A node is killed or loses its network | The leader drops what that node owned and announces the departures once, as soon as Erlang notices the node is gone. |
| The leader's node dies | The next node takes over from its standby copy, asks every node what it owns, and announces only the differences. Presence changes wait briefly (about two seconds) for the new leader; events between surviving nodes keep flowing. |
| The network splits in two | Each side keeps working on its own with its own leader. When it heals, the sides are merged: clients and webhooks are told what each side missed. |

### Limits

- **Throughput of presence.** The leader handles every subscribe, unsubscribe
  and presence change in the cluster. That suits tens of thousands of
  connections, not millions. Use Redis if you need the most headroom.
- **Failure detection time.** A node that vanishes without closing its
  connection is noticed after Erlang's `net_ticktime` (60 s by default). Lower
  it with `ERL_AFLAGS="-kernel net_ticktime 15"` for faster cleanup.
- **Security.** Erlang distribution trusts anyone with the cookie, and a node
  with the cookie can run any code. Never publish port 4369 or the distribution
  ports (restrict them with `ERL_AFLAGS="-kernel inet_dist_listen_min 9100
  inet_dist_listen_max 9105"`), keep them on a private network, and set
  `RELEASE_COOKIE` yourself. The cookie baked into a public image is not a
  secret.
- **Rolling deploys.** Nodes running different Socketelx versions may not agree
  on the cluster protocol. Upgrade all nodes together, or accept a short period
  of inconsistent presence.
- Delivery is at-most-once, like Pusher itself.

### Checking it works

Start two nodes, connect a client to one and trigger an event through the
other. To see the cluster itself, open a shell on a node
(`docker compose exec socketelx-1 bin/socketelx remote`) and run `Node.list()`;
it should list the other nodes.
