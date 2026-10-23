# Deployment

## Image

The `Dockerfile` builds an Elixir release and runs it as a non-root user. It
exposes port 6001 and has a `HEALTHCHECK` that requests `GET /` (returns `ok`).

```bash
docker build -t socketelx .
```

### Publishing the image

Pushing a git tag like `v0.5.1` runs `.github/workflows/docker-publish.yml`,
which pushes `0.5.1`, `0.5` and `latest` to GitHub Container Registry. To also
publish to Docker Hub, add a repository variable `DOCKERHUB_USERNAME` and a
secret `DOCKERHUB_TOKEN` (a Docker Hub access token with Read & Write) under
Settings > Secrets and variables > Actions. The image is named
`socketelx/socketelx`. To push by hand instead:

```bash
docker build -t socketelx/socketelx:0.5.1 -t socketelx/socketelx:latest .
docker login
docker push socketelx/socketelx:0.5.1 && docker push socketelx/socketelx:latest
```

The image is amd64 only unless you build with `docker buildx --platform
linux/amd64,linux/arm64`.

Configuration is by environment variable; see [Configuration](configuration.md).

## TLS

Browsers need `wss://`. Either terminate TLS at a reverse proxy or load balancer
(nginx, Caddy, Traefik, an ALB, Cloudflare), or let the server do it: set
`SOCKETELX_TLS_CERTFILE` and `SOCKETELX_TLS_KEYFILE` (PEM files) and it serves HTTPS
and WSS on its port. Either way, switch your client and server libraries to TLS
(`forceTLS: true`, `useTLS: true`) and the public port.

Before going live, read [Going to production](production.md): open-file limits,
proxy timeouts, memory and monitoring.

## Reverse proxy

Your proxy must:

- pass the WebSocket upgrade (`Upgrade` and `Connection` headers, HTTP/1.1)
- keep idle connections open longer than 60 seconds. The server closes a
  connection that has been silent for 60 s, and clients ping every 30 s.
  `deploy/nginx.conf` uses 300 s timeouts.
- route `/app/*` (WebSocket) and `/apps/*` (REST) to the server

Minimal nginx location:

```nginx
location / {
    proxy_pass http://pusher_upstream;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 300s;
    proxy_send_timeout 300s;
}
```

## Graceful shutdown

On `SIGTERM` (`docker stop`, a rolling deploy) the server stops accepting new
connections, sends every connected client `pusher:error` code 4000 so it
reconnects, waits about one second, then exits. Give the container more than
that: `docker stop --time 10`, or `stop_grace_period: 10s` in Compose.

With a load balancer in front, clients reconnect to another node. With a single
node, they reconnect when it is back.

## Docker Compose

`docker-compose.example.yml` runs two nodes, Redis and nginx. For one node,
keep a single `socketelx-1` service and drop `SOCKETELX_REDIS_URL`.

`docker-compose.cluster.example.yml` is the same two nodes with no Redis, using
the experimental Erlang cluster mode. See [Multi-server](multi-server.md).

## Checklist

1. Set your own `SOCKETELX_APP_SECRET`.
2. Terminate TLS in front.
3. Make the proxy timeouts longer than 60 s.
4. Do not expose the REST API more widely than needed; it is signed.
5. Monitor the health endpoint and, if you enable it, `/metrics` (protect it
   with `SOCKETELX_METRICS_TOKEN` or block it at the proxy).
6. Consider `SOCKETELX_REST_RATE_LIMIT` and `SOCKETELX_MAX_CONNECTIONS` as safety
   valves; see [Configuration](configuration.md#rate-limits-and-capacity).
7. Load test with your expected connection count before going live.
