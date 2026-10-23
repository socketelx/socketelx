#!/usr/bin/env node
// Load test for a Pusher-protocol server. No dependencies; needs Node 22+.
//
//   node bench/load.mjs --host 127.0.0.1 --port 6001 --key app-key \
//        --app-id app-id --secret app-secret \
//        --connections 2000 --channels 50 --rate 100 --duration 60
//
// One Node process tops out at roughly 50k received messages per second. For more,
// add --workers 4: the connections and the send rate are split over 4 processes
// (about one per CPU core) and the results are combined.
//
// What it does:
//   1. opens --connections WebSocket connections (--ramp new ones per second)
//      and subscribes each to one of --channels public channels,
//   2. sends --rate events per second through the REST API, each to a random
//      channel, for --duration seconds,
//   3. measures how long each event takes to reach each subscriber, and how
//      many were lost.
// Latency is stamped and measured by this script, so it is valid even when
// the server is on another machine (no clock sync needed).
import crypto from 'node:crypto';
import { fork } from 'node:child_process';

const KNOWN = ['host', 'port', 'tls', 'key', 'app-id', 'secret', 'connections', 'channels', 'rate', 'duration', 'ramp', 'workers', 'targets', 'metrics-url', 'metrics-token', 'no-preflight', 'no-ping', 'help'];

// Accepts both `--connections 5000` and `--connections=5000`; a bare `--tls` means true.
const args = {};
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const a = argv[i];
  if (!a.startsWith('--')) { console.error(`Unexpected argument "${a}". Try --help.`); process.exit(1); }
  const eq = a.indexOf('=');
  const name = eq === -1 ? a.slice(2) : a.slice(2, eq);
  if (!KNOWN.includes(name)) { console.error(`Unknown option --${name}. Try --help.`); process.exit(1); }
  if (eq !== -1) args[name] = a.slice(eq + 1);
  else if (argv[i + 1] !== undefined && !argv[i + 1].startsWith('--')) args[name] = argv[++i];
  else args[name] = 'true';
}
if (args.help) {
  console.log(`Options: ${KNOWN.filter((k) => k !== 'help').map((k) => '--' + k).join(' ')}`);
  process.exit(0);
}
const host = args.host ?? '127.0.0.1';
const port = Number(args.port ?? 6001);
const tls = args.tls === 'true';
const key = args.key ?? 'app-key';
const appId = args['app-id'] ?? 'app-id';
const secret = args.secret ?? 'app-secret';
const WORKER_ID = process.env.BENCH_WORKER === undefined ? null : Number(process.env.BENCH_WORKER);
const IS_WORKER = WORKER_ID !== null;
const WORKERS = IS_WORKER ? Number(process.env.BENCH_WORKERS) : Number(args.workers ?? 1);
if (!Number.isInteger(WORKERS) || WORKERS < 1 || WORKERS > 64) { console.error('--workers must be a whole number from 1 to 64.'); process.exit(1); }
const TOTAL = Number(args.connections ?? 1000);
const CHANNELS = Number(args.channels ?? 20);
const TOTAL_RATE = Number(args.rate ?? 50);
const DURATION = Number(args.duration ?? 30);
const TOTAL_RAMP = Number(args.ramp ?? 200);
// A worker takes every WORKERS-th connection and its share of the rate and ramp.
const W = IS_WORKER ? WORKERS : 1;
const N = IS_WORKER ? Math.floor((TOTAL - WORKER_ID - 1) / WORKERS) + 1 : TOTAL;
const RATE = TOTAL_RATE / W;
const RAMP = Math.max(1, Math.round(TOTAL_RAMP / W));
const log = IS_WORKER ? () => {} : (...a) => console.log(...a);
const progress = IS_WORKER ? () => {} : (t) => process.stdout.write(t);
// One or more servers. With several (--targets a:6001,b:6001), connection i goes to
// server i mod N and events are published through the servers in turn, so most
// deliveries have to cross from one node to another: this is the multi-node test.
const targets = (args.targets ?? `${host}:${port}`).split(',').map((t) => {
  const [h, p] = t.trim().split(':');
  return { host: h, port: Number(p ?? 6001), http: `${tls ? 'https' : 'http'}://${h}:${p ?? 6001}`, connected: 0 };
});
const httpBase = targets[0].http;

const stats = {
  connected: 0, connectFailed: 0, closedEarly: 0, subscribed: 0,
  sent: 0, sendFailed: 0, rateLimited: 0, expected: 0, received: 0, latencies: [],
  failReasons: {}, connectReasons: {},
};

async function trigger(channel, id, target = targets[0]) {
  const body = JSON.stringify({ name: 'bench', channels: [channel], data: JSON.stringify({ id, t: performance.timeOrigin + performance.now() }) });
  const params = {
    auth_key: key, auth_timestamp: String(Math.floor(Date.now() / 1000)), auth_version: '1.0',
    body_md5: crypto.createHash('md5').update(body).digest('hex'),
  };
  const query = Object.keys(params).sort().map((k) => `${k}=${params[k]}`).join('&');
  const path = `/apps/${appId}/events`;
  const sig = crypto.createHmac('sha256', secret).update(`POST\n${path}\n${query}`).digest('hex');
  const res = await fetch(`${target.http}${path}?${query}&auth_signature=${sig}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body });
  if (res.status !== 200 && res.status !== 429) {
    // keep the first few reasons so a wrong secret or app id is obvious
    const text = (await res.text()).slice(0, 200);
    const k = `HTTP ${res.status} ${text}`;
    stats.failReasons[k] = (stats.failReasons[k] ?? 0) + 1;
  }
  return res.status;
}

const subscribersPerChannel = new Array(CHANNELS).fill(0);
let phase = 'ramping';

// `i` is the connection's number across all workers, so channels and servers are spread evenly.
function openConnection(i) {
  const channel = `bench-${i % CHANNELS}`;
  return new Promise((resolve) => {
    let pinger;
    let subscribed = false;
    const target = targets[i % targets.length];
    const ws = new WebSocket(`${tls ? 'wss' : 'ws'}://${target.host}:${target.port}/app/${key}?protocol=7`);
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(); } };
    ws.onopen = () => { stats.connected++; };
    ws.onerror = (e) => {
      if (!done) {
        stats.connectFailed++;
        const k = String(e?.message ?? e?.error?.message ?? 'connection error');
        stats.connectReasons[k] = (stats.connectReasons[k] ?? 0) + 1;
      }
      finish();
    };
    ws.onclose = () => {
      clearInterval(pinger);
      if (phase !== 'done' && stats.connected > 0) stats.closedEarly++;
      // a dropped client no longer counts towards the deliveries we expect
      if (subscribed && phase !== 'done') subscribersPerChannel[i % CHANNELS]--;
      finish();
    };
    ws.onmessage = ({ data }) => {
      const msg = JSON.parse(data);
      if (msg.event === 'pusher:error') {
        const k = `server error ${msg.data?.code ?? ''} ${msg.data?.message ?? JSON.stringify(msg.data)}`;
        stats.connectReasons[k] = (stats.connectReasons[k] ?? 0) + 1;
      } else if (msg.event === 'pusher:connection_established') {
        ws.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel } }));
        // Real clients (pusher-js) ping when the connection is quiet; the server
        // closes sockets that stay silent for twice the advertised activity timeout.
        if (args['no-ping'] !== 'true') {
          const every = (JSON.parse(msg.data).activity_timeout ?? 30) * 1000;
          pinger = setInterval(() => ws.readyState === 1 && ws.send('{"event":"pusher:ping","data":{}}'), every);
        }
      } else if (msg.event === 'pusher_internal:subscription_succeeded') {
        subscribed = true;
        target.connected++;
        stats.subscribed++; subscribersPerChannel[i % CHANNELS]++; finish();
      } else if (msg.event === 'bench') {
        stats.received++;
        stats.latencies.push(performance.timeOrigin + performance.now() - JSON.parse(msg.data).t);
      }
    };
  });
}

const pct = (arr, p) => (arr.length ? arr[Math.min(arr.length - 1, Math.floor((p / 100) * arr.length))] : NaN);
const fmt = (n) => (Number.isFinite(n) ? n.toFixed(1) : 'n/a');

async function metrics() {
  if (!args['metrics-url']) return;
  const res = await fetch(args['metrics-url'], { headers: args['metrics-token'] ? { Authorization: `Bearer ${args['metrics-token']}` } : {} });
  const text = await res.text();
  const line = (n) => text.split('\n').find((l) => l.startsWith(n + ' '));
  console.log('server metrics:', ['socketelx_connections', 'socketelx_channels', 'socketelx_events_triggered_total'].map(line).join(' | '));
}

// One signed event before the ramp: a wrong key, app id or secret shows up
// here as a clear message instead of thousands of silent failures later.
if (!IS_WORKER && args['no-preflight'] !== 'true') {
  for (const target of targets) {
    try {
      const status = await trigger('bench-preflight', -1, target);
      if (status !== 200) {
        console.error(`Preflight failed: ${target.http} answered HTTP ${status} to a signed event.`);
        console.error(Object.keys(stats.failReasons).join('\n') || '(no body)');
        console.error('Check --key, --app-id and --secret (and the address). Use --no-preflight to skip this check.');
        process.exit(1);
      }
      stats.failReasons = {};
    } catch (e) {
      console.error(`Preflight failed: could not reach ${target.http} (${e.cause?.code ?? e.message}).`);
      process.exit(1);
    }
  }
}


// ---- shared helpers for the final report ----
const sumInto = (a, b) => { for (const [k, v] of Object.entries(b)) a[k] = (a[k] ?? 0) + v; };

function report(r, p50, p95, p99, max) {
  console.log('\n--- result ---');
  if (r.connectFailed > 0 || r.closedEarly > 0) {
    console.log('  Some connections failed. Before blaming the server, check the open-file limit of BOTH');
    console.log('  terminals (server and this script): run `ulimit -n`. Each connection needs one file');
    console.log('  descriptor, and macOS/Linux often allow only 256-1024. Raise it with `ulimit -n 65536`.');
  }
  for (const [k, n] of Object.entries(r.connectReasons)) console.log(`  connection problem x${n}: ${k}`);
  for (const [k, n] of Object.entries(r.failReasons)) console.log(`  send problem x${n}: ${k}`);
  if (targets.length > 1) console.log(`per server:         ${targets.map((t, ti) => `${t.host}:${t.port}=${r.perTarget[ti]}`).join('  ')} (events are published through each server in turn)`);
  console.log(`connections:        ${r.subscribed}/${TOTAL} subscribed, ${r.connectFailed} failed, ${r.closedEarly} dropped during the test`);
  console.log(`events sent:        ${r.sent} (${(r.sent / r.secs).toFixed(0)}/s), ${r.sendFailed} failed, ${r.rateLimited} rate limited (429)`);
  console.log(`messages delivered: ${r.received} of ${r.expected} expected (${r.expected ? ((100 * r.received) / r.expected).toFixed(2) : 'n/a'}%)`);
  console.log(`delivery rate:      ${(r.received / r.secs).toFixed(0)} messages/s received across all clients`);
  console.log(`latency (ms):       p50 ${fmt(p50)}  p95 ${fmt(p95)}  p99 ${fmt(p99)}  max ${fmt(max)}`);
}

// Latencies are sent between processes as counts per 0.1 ms, not as millions of numbers.
function histogram(sorted) {
  const h = {};
  for (const v of sorted) { const b = Math.floor(v * 10); h[b] = (h[b] ?? 0) + 1; }
  return h;
}

const targetCounts = () => targets.map((t) => t.connected);

// ---- the connect + send run (single process, or one worker) ----
async function run(globalSubs) {
  // globalSubs is set when several workers share the load: an event reaches the subscribers
  // of every worker, so the number we expect is the sum over all workers.
  let expectedSubs = globalSubs ?? subscribersPerChannel;
  phase = 'sending';
  const fanout = TOTAL_RATE * (stats.subscribed / CHANNELS);
  const wholeFanout = TOTAL_RATE * (expectedSubs.reduce((a, b) => a + b, 0) / CHANNELS);
  log(`Sending ${TOTAL_RATE} events/s for ${DURATION} s... (each event reaches ~${Math.round(expectedSubs.reduce((a, b) => a + b, 0) / CHANNELS)} clients, so ~${Math.round(wholeFanout)} messages/s to receive)`);
  if (wholeFanout / WORKERS > 50000) log(WORKERS === 1
    ? '  Note: this script is one Node process; above ~50k messages/s it may become the bottleneck, not the server. Try --workers 4.'
    : `  Note: about ${Math.round(wholeFanout / WORKERS)} messages/s per worker; add more --workers if results look limited by this script.`);
  void fanout;
  const start = Date.now();
  let seq = 0;
  const interval = 1000 / RATE;
  let next = start;
  const inflight = new Set();
  while (Date.now() - start < DURATION * 1000) {
    const ch = Math.floor(Math.random() * CHANNELS);
    stats.expected += expectedSubs[ch];
    stats.sent++;
    const id = seq++;
    const p = trigger(`bench-${ch}`, id, targets[(IS_WORKER ? id * WORKERS + WORKER_ID : id) % targets.length]).then((s) => { if (s === 429) stats.rateLimited++; else if (s !== 200) stats.sendFailed++; }, (e) => {
      stats.sendFailed++;
      const k = `request error ${e?.cause?.code ?? e?.cause?.message ?? e?.message ?? e}`;
      stats.failReasons[k] = (stats.failReasons[k] ?? 0) + 1;
    });
    inflight.add(p); p.finally(() => inflight.delete(p));
    next += interval;
    const wait = next - Date.now();
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    if (seq % Math.max(1, Math.round(RATE)) === 0) progress(`\r  sent ${stats.sent}, received ${stats.received}`);
  }
  const secs = (Date.now() - start) / 1000;
  await Promise.all(inflight);
  await new Promise((r) => setTimeout(r, 2000)); // let the last events arrive
  phase = 'done';
  stats.latencies.sort((a, b) => a - b);
  return secs;
}

async function connectAll() {
  const t0 = Date.now();
  const pending = [];
  for (let i = 0; i < N; i++) {
    pending.push(openConnection(IS_WORKER ? i * WORKERS + WORKER_ID : i));
    if ((i + 1) % RAMP === 0) {
      await new Promise((r) => setTimeout(r, 1000));
      progress(`\r  ${stats.subscribed}/${N} subscribed, ${stats.connectFailed} failed`);
    }
  }
  await Promise.all(pending);
  return (Date.now() - t0) / 1000;
}

// ---- worker: connect, report, wait for the go signal, send, report ----
if (IS_WORKER) {
  const took = await connectAll();
  void took;
  process.send({ type: 'ready', subs: subscribersPerChannel, subscribed: stats.subscribed, connectFailed: stats.connectFailed });
  const go = await new Promise((resolve) => process.once('message', resolve));
  const secs = await run(go.subs);
  process.send({
    type: 'result', secs, hist: histogram(stats.latencies), max: stats.latencies.at(-1),
    r: { ...stats, latencies: undefined }, perTarget: targetCounts(),
  }, () => process.exit(0));
  await new Promise(() => {});
}

// ---- several workers: this process only coordinates ----
if (WORKERS > 1) {
  console.log(`Connecting ${TOTAL} clients to ${targets.map((t) => `${t.host}:${t.port}`).join(', ')} (${TOTAL_RAMP}/s) across ${CHANNELS} channels, using ${WORKERS} worker processes...`);
  const t0 = Date.now();
  const kids = [];
  for (let w = 0; w < WORKERS; w++) {
    const child = fork(new URL(import.meta.url), process.argv.slice(2), { env: { ...process.env, BENCH_WORKER: String(w), BENCH_WORKERS: String(WORKERS) } });
    const k = { child, ready: null, result: null };
    k.readyP = new Promise((resolve) => child.once('message', (m) => { k.ready = m; resolve(); }));
    k.resultP = new Promise((resolve) => {
      child.on('message', (m) => { if (m.type === 'result') { k.result = m; resolve(); } });
      child.on('exit', () => resolve());
    });
    kids.push(k);
  }
  await Promise.all(kids.map((k) => k.readyP));
  const subs = new Array(CHANNELS).fill(0);
  let subscribed = 0; let connectFailed = 0;
  for (const k of kids) { k.ready.subs.forEach((n, c) => { subs[c] += n; }); subscribed += k.ready.subscribed; connectFailed += k.ready.connectFailed; }
  console.log(`  ${subscribed}/${TOTAL} subscribed, ${connectFailed} failed, in ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  await metrics();
  stats.subscribed = subscribed;
  const per = subs.reduce((a, b) => a + b, 0) / CHANNELS;
  const whole = TOTAL_RATE * per;
  console.log(`Sending ${TOTAL_RATE} events/s for ${DURATION} s... (each event reaches ~${Math.round(per)} clients, so ~${Math.round(whole)} messages/s to receive)`);
  if (whole / WORKERS > 50000) console.log(`  Note: about ${Math.round(whole / WORKERS)} messages/s per worker; add more --workers if results look limited by this script.`);
  for (const k of kids) k.child.send({ type: 'go', subs });
  await Promise.all(kids.map((k) => k.resultP));
  if (kids.some((k) => !k.result)) { console.error('A worker exited before finishing; results are incomplete.'); process.exit(1); }
  const total = { subscribed, connectFailed: 0, closedEarly: 0, sent: 0, sendFailed: 0, rateLimited: 0, expected: 0, received: 0, failReasons: {}, connectReasons: {}, perTarget: new Array(targets.length).fill(0), secs: 0 };
  const hist = {}; let max = 0;
  for (const { result: m } of kids) {
    for (const f of ['connectFailed', 'closedEarly', 'sent', 'sendFailed', 'rateLimited', 'expected', 'received']) total[f] += m.r[f];
    sumInto(total.failReasons, m.r.failReasons); sumInto(total.connectReasons, m.r.connectReasons);
    m.perTarget.forEach((n, ti) => { total.perTarget[ti] += n; });
    sumInto(hist, m.hist); total.secs = Math.max(total.secs, m.secs); max = Math.max(max, m.max ?? 0);
  }
  const keys = Object.keys(hist).map(Number).sort((a, b) => a - b);
  const count = keys.reduce((a, b) => a + hist[b], 0);
  const at = (p) => { let acc = 0; const want = Math.min(count - 1, Math.floor((p / 100) * count)); for (const b of keys) { acc += hist[b]; if (acc > want) return b / 10; } return NaN; };
  report(total, at(50), at(95), at(99), max);
  await metrics();
  process.exit(0);
}

// ---- one process ----
console.log(`Connecting ${N} clients to ${targets.map((t) => `${t.host}:${t.port}`).join(', ')} (${RAMP}/s) across ${CHANNELS} channels...`);
const took = await connectAll();
console.log(`\r  ${stats.subscribed}/${N} subscribed, ${stats.connectFailed} failed, in ${took.toFixed(1)} s`);
await metrics();
const secs = await run(null);
report({ ...stats, perTarget: targetCounts(), secs }, pct(stats.latencies, 50), pct(stats.latencies, 95), pct(stats.latencies, 99), stats.latencies.at(-1));
await metrics();
process.exit(0);
