# Going to production

A checklist for running Socketelx for real users, and the operating-system limits
that most often catch people out. Read the [Capacity planning](capacity-planning.md)
guide for how much one server can carry.

## Checklist

- [ ] **Secrets.** `SOCKETELX_APP_SECRET` is a long random value (`openssl rand -hex 32`).
      The built-in default is public. The server logs a warning if you leave it.
- [ ] **TLS.** Browsers need `wss://`. Terminate TLS at your proxy, load balancer or
      Cloudflare, or set `SOCKETELX_TLS_CERTFILE` and `SOCKETELX_TLS_KEYFILE`.
- [ ] **Origins.** Set `SOCKETELX_ALLOWED_ORIGINS` to your own sites.
- [ ] **Open-file limit** is at least your peak connections plus 1,000
      ([below](#open-file-limit-ulimit)).
- [ ] **Proxy timeouts** are longer than 60 seconds ([below](#reverse-proxies-and-load-balancers)).
- [ ] **`/metrics`** is protected: set `SOCKETELX_METRICS_TOKEN`, or block the path at your proxy.
- [ ] **Limits** you want are set: `SOCKETELX_MAX_CONNECTIONS`, `SOCKETELX_MAX_CONNECTIONS_PER_IP`,
      `SOCKETELX_REST_RATE_LIMIT`. They are all off by default.
- [ ] **Health check** on `GET /` (the Docker image already has one).
- [ ] **Graceful stop.** Give the container at least 10 seconds to stop (Docker's default).
      On `SIGTERM` the server tells every client to reconnect, then exits.
- [ ] **Two or more nodes** if you cannot accept downtime during a restart or a crash
      ([Multi-server](multi-server.md)).
- [ ] **Webhooks** that matter: set `SOCKETELX_WEBHOOK_QUEUE_PATH` to a mounted volume so
      retries survive a restart, and make your receiver idempotent.
- [ ] **You have load-tested** your own setup, through your real proxy
      ([Load testing](load-testing.md)).

## Open-file limit (ulimit)

Every WebSocket connection is one file descriptor. Operating systems often allow only
256 to 1,024 per process by default. When the limit is reached the server starts
failing to accept connections (and Erlang may log `emfile` errors). It is the most
common cause of "it works with 500 users and breaks at 1,000".

Set it comfortably above your peak connections (65,536 is a good start; use more if you
plan for more than about 60,000 connections on one node).

**Docker**

```bash
docker run --ulimit nofile=65536:65536 -p 6001:6001 ghcr.io/socketelx/socketelx
```

**Docker Compose**

```yaml
services:
  socketelx:
    image: ghcr.io/socketelx/socketelx
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
    stop_grace_period: 15s
```

**systemd**

```ini
[Service]
LimitNOFILE=65536
```

**Kubernetes** has no per-pod `ulimit` setting. The limit comes from the container
runtime on the node, and is usually already high. Check from inside the pod.

**Check what the server actually got** (do this, don't assume):

```bash
docker exec <container> sh -c 'cat /proc/1/limits | grep "open files"'
```

**Your laptop** (for local load tests): run `ulimit -n 65536` in the terminal that starts
the server and in the one that starts the load test. On macOS, if it is refused, run
`sudo launchctl limit maxfiles 65536 200000` first and open new terminals.

## Operating-system network settings (Linux)

You rarely need these below about 20,000 connections per node. Above that, or behind
a proxy, check them.

| Setting | Why it matters |
|---|---|
| `net.core.somaxconn` (e.g. `4096`) | Size of the queue of connections waiting to be accepted. If clients reconnect all at once (after a restart) and the queue is too small, some get refused. Newer kernels default to 4096, older ones to 128. |
| `net.ipv4.ip_local_port_range` (e.g. `1024 65535`) | Ports a **client** can use. See the proxy note below. |
| `net.netfilter.nf_conntrack_max` | With Docker's default networking or any NAT, every connection is tracked. When the table is full the kernel silently drops packets (`nf_conntrack: table full` in `dmesg`). Raise it, or avoid NAT for this service. |

On macOS the equivalent of `somaxconn` is `kern.ipc.somaxconn`, and it defaults to 128.
A burst of new connections on a laptop can be refused for that reason alone. Raise it
with `sudo sysctl -w kern.ipc.somaxconn=4096`.

### The proxy port limit

A proxy (nginx, a load balancer, `cloudflared`) opens one connection to Socketelx for
every browser connection. All of them come from the proxy's one IP address to
Socketelx's one IP and port, and a single source IP can use only about 28,000 ports
by default (32768 to 60999). So one proxy in front of one Socketelx address tops out at
about 28,000 connections, however big the machines are. Fixes: widen
`ip_local_port_range` on the proxy host, give the proxy several upstream addresses, or
run more nodes.

## Reverse proxies and load balancers

- Pass the WebSocket upgrade headers and use HTTP/1.1. See `deploy/nginx.conf`.
- Keep the idle timeout **longer than 60 seconds**. Clients ping about every 30 seconds
  and the server closes a socket that stays silent for 60 seconds. We suggest 120 to 300
  seconds. Cloud load balancers often default to 60 seconds, which is too tight.
- No sticky sessions are needed. A WebSocket stays on the node it connected to, and
  REST calls from your backend can go to any node.
- Behind a proxy every client looks like it has the proxy's address. If you use
  `SOCKETELX_MAX_CONNECTIONS_PER_IP`, also set `SOCKETELX_CLIENT_IP_HEADER`
  (`cf-connecting-ip` on Cloudflare, `x-forwarded-for` elsewhere) and make sure your
  proxy overwrites that header.
- Use a least-connections strategy rather than round robin: connections are long lived.

## Memory

The server's memory grows with connections, and the Erlang VM keeps memory it has
freed for reuse instead of handing it straight back to the OS. Set a container memory
limit of at least twice your expected steady-state use, or the container may be killed
under a burst. Watch `socketelx_connections` next to the process's memory.

## CPU cores

There is no worker setting to tune. Socketelx runs on the Erlang VM, which starts one
scheduler thread per CPU core it can see and spreads the connections (each one is its
own lightweight process) across them automatically. A server with 4 cores uses all 4.

Two things to check when you deploy:

- **Containers.** Recent Erlang versions respect a Docker or Kubernetes CPU limit
  (`--cpus 2`, `resources.limits.cpu`) and start that many schedulers. To see what the
  VM chose, run `bin/socketelx remote` and then `:erlang.system_info(:schedulers_online)`.
  If it shows more than the CPU you pay for, set it yourself with
  `ERL_FLAGS="+S 2:2"` (schedulers : online schedulers).
- **Size by cores, not by count.** More cores raise how many messages per second one
  node can deliver, and more nodes do the same with more machines. See
  [Capacity planning](capacity-planning.md) for the arithmetic.

The `--workers` option in `bench/load.mjs` is unrelated: it only controls how many
processes the *test generator* uses, because one Node process is single-threaded. It
does not change the server.

## Rolling deploys and restarts

- Restart nodes **one at a time** and wait for `socketelx_connections` on the restarted
  node to recover before the next one. A node that stops tells its clients to reconnect,
  and they land on the others.
- Clients reconnect on their own (`pusher-js` backs off and retries). A large node
  restarting means many clients reconnecting together, which is the reason to keep
  `somaxconn` generous and to restart one node at a time.
- Nothing needs backing up: the server keeps no data on disk, apart from the optional
  webhook queue.

## Monitoring

Turn on `SOCKETELX_METRICS=true` and scrape `/metrics` from every node. Useful alerts:

| Metric | Alert when |
|---|---|
| `socketelx_connections` | drops suddenly (a crash, or a proxy cutting connections) |
| `socketelx_connections_rejected_total` | rises (you are at `MAX_CONNECTIONS`, or a per-IP limit is firing) |
| `socketelx_rest_rate_limited_total` | rises (your backend is being refused) |
| `socketelx_webhooks_failed_total` | rises (your webhook receiver is down) |
| process memory | grows steadily without more connections |

## Redis or Erlang nodes?

> **Redis Cluster:** if your Redis is a Redis Cluster, set `SOCKETELX_REDIS_CLUSTER=true`
> (see [Multi-server](multi-server.md#with-redis-cluster)). Plan for a few seconds of paused
> delivery between nodes whenever Redis fails over, and keep at least one replica per shard.


For several servers, Redis is the better-tested path and keeps nodes stateless. The
Erlang cluster mode needs no extra service but is experimental. See
[Multi-server](multi-server.md). Test both on your own network; the
[load-testing guide](load-testing.md) shows how to run the same test against two nodes.

## Troubleshooting capacity problems

| Symptom | Likely cause |
|---|---|
| Connections fail at about 1,000 (or 256, or 10,000) | open-file limit |
| `emfile` in the server log | open-file limit |
| Occasional refused connections during a burst | `somaxconn` too small |
| Connections fail at about 28,000 behind a proxy | the proxy port limit |
| Clients disconnect and reconnect in a regular rhythm | a proxy or load balancer idle timeout shorter than the 30-second ping interval |
| Latency climbs, delivery drops | you are past capacity: CPU or bandwidth (see [Capacity planning](capacity-planning.md)) |
