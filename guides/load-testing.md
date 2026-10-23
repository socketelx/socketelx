# Load testing

`bench/load.mjs` opens many WebSocket connections, sends events through the REST
API, and reports how many arrived and how fast. It needs Node 22 or newer and no
packages.

```bash
node bench/load.mjs --host 127.0.0.1 --port 6001 \
  --key app-key --app-id app-id --secret app-secret \
  --connections 2000 --channels 50 --rate 100 --duration 60
```

Options can be written `--rate 100` or `--rate=100`. Unknown options are an error, so a
typo can't silently fall back to a default. Before connecting anyone, the script sends
one signed event and stops with a clear message if the server refuses it (usually a
wrong `--key`, `--app-id` or `--secret`).

| Option | Default | Meaning |
|---|---|---|
| `--connections` | 1000 | WebSocket clients to open |
| `--ramp` | 200 | New connections per second |
| `--workers` | 1 | Number of generator processes. Connections and send rate are split between them and the results combined. Use about one per free CPU core. |
| `--channels` | 20 | Public channels; each client joins one |
| `--rate` | 50 | Events per second sent through the REST API |
| `--duration` | 30 | Seconds to send events |
| `--tls` | off | Use `wss://` and `https://` |
| `--targets` | none | Several servers, `host:port,host:port`. See [Testing several nodes](#testing-several-nodes) |
| `--no-ping` | off | Don't send keep-alive pings (connections then close after 60 s of silence) |
| `--no-preflight` | off | Skip the first check event |
| `--metrics-url`, `--metrics-token` | none | Also print the server's own `/metrics` gauges |

Example result:

```
connections:        1000/1000 subscribed, 0 failed, 0 dropped during the test
events sent:        500 (50/s), 0 failed, 0 rate limited (429)
messages delivered: 25000 of 25000 expected (100.00%)
latency (ms):       p50 2.5  p95 3.6  p99 6.4  max 18.0
```

That run was a single small machine with the load generator on the same host,
so treat it as a smoke test. Measure your own hardware.

## Reading the numbers

- **Fan-out is what costs.** Each event is delivered to every subscriber of its
  channel. `--rate 100` with 40 subscribers per channel means 4,000 messages per
  second leaving the server. To model a big room, use fewer `--channels`.
- **Delivered below 100%** means events were lost or arrived after the 2 s grace
  period. Under heavy load that is the first sign you are past capacity.
- **Latency percentiles** matter more than the average. Watch p99.
- **Dropped during the test** means connections closed unexpectedly. Check the
  server logs and any proxy timeouts.
- **The script is one Node process.** It prints how many messages per second it
  has to receive. Above roughly 50,000 per second it can become the bottleneck
  instead of the server: you then see multi-second latencies and `ECONNRESET`
  errors that say more about the generator than about the server. Add
  `--workers 4` (about one per free CPU core) to spread the work over several
  processes, or raise `--rate` step by step and stop reading the results once the
  generator is the limit. Run the generator on a different machine from the
  server when you can: on one machine they compete for the same CPU, and with
  many workers that shows up as extra latency.
- **Failed sends** list their reason (`ECONNRESET`, `EPIPE`, an HTTP status).
  `connection problem ... non-101` during the ramp usually means the OS refused
  connections: see the open-file and `somaxconn` notes below.
- **Clients ping like real ones.** The server closes a connection that is silent for
  60 seconds, so the script sends `pusher:ping` at the advertised interval.

## Getting a fair test

1. **Run the load generator on a different machine** from the server, or the two
   compete for the same CPU and the result is meaningless.
2. **Raise file descriptor limits on both sides.** Each connection is a file
   descriptor in the server **and** in the generator. Run `ulimit -n 65536` in the
   terminal that starts the generator and in the one that starts the server (for a
   container, `--ulimit nofile=65536:65536`), and check it with `ulimit -n`. Without
   this, tests fail at about 1,000 connections on many machines, and the server logs
   `emfile`. More in [Going to production](production.md#open-file-limit-ulimit).
   On macOS also raise the accept queue: `sudo sysctl -w kern.ipc.somaxconn=4096`.
3. **Mind the port limit.** One source IP can open about 28,000 connections to a
   single destination IP and port. For more, run several generators from
   different IPs, or give the server more than one address.
4. **Test through your real path.** If production goes through Cloudflare, nginx
   or a tunnel, point the test at that path too. Proxies have their own limits.
5. **Watch the server while it runs.** `docker stats` for CPU and memory, and
   `/metrics` (with `SOCKETELX_METRICS=true`) for `socketelx_connections`.
6. **Ramp up.** Run 1,000, then 5,000, then 10,000, and note where latency starts
   to climb. That knee is your practical capacity per node.

## Testing several nodes

Start the nodes with a shared `SOCKETELX_REDIS_URL` (or in Erlang cluster mode, see
[Multi-server](multi-server.md)) and give the generator **all** of them:

```bash
node bench/load.mjs --targets=10.0.0.11:6001,10.0.0.12:6001,10.0.0.13:6001 \
  --key app-key --app-id app-id --secret app-secret \
  --connections=10000 --rate=50 --duration=60
```

Connection number *i* goes to server *i mod N*, and events are published through the
servers in turn. So most deliveries have to travel from the node that received the
event to a client on another node. That is exactly the path that differs between Redis
and Erlang clustering. The result prints how many connections each server holds.

To compare **Redis against Erlang clustering**, run the identical command against the
same machines set up each way, three times each, alternating, and compare p50 and p99
and the delivery percentage. Then repeat while you stop one node halfway
(`docker stop`, or kill the process): connections on that node drop and reconnect
(the script does not reconnect them, so `dropped` shows how many were on it), while the
others should keep delivering 100% to the clients that remain.

A load balancer in front also works (`--host`/`--port` at the balancer), but then you
can't tell which node each client is on.

## Limits you may hit on purpose

`SOCKETELX_REST_RATE_LIMIT` makes the generator report 429s, and
`SOCKETELX_MAX_CONNECTIONS` makes connections fail with error 4004. That is a good
way to confirm the safety valves work before you rely on them.
