#!/usr/bin/env node
// Reference conformance adapter: a dependency-free implementation of the
// worker protocol (WORKER_PROTOCOL.md) used to validate the kit itself and
// as an executable example for SDK authors. Usage: adapter.mjs <mode>.

import { createServer } from "node:http";
import { createInterface } from "node:readline";

import { verify } from "../lib/sign.mjs";

const env = process.env;
const BASE = env.ORCH8_BASE_URL;
const WORKER_ID = env.ORCH8_WORKER_ID ?? `ref-${process.pid}`;
// REF_BUG injects a known protocol violation; used by the kit's own tests to
// prove each scenario actually detects the violation it is named for.
const BUG = env.REF_BUG ?? "";
const HANDLER_NAMES = ["echo", "fail_retryable", "fail_permanent", "crash", "checkpoint", "slow"];

class ApiError extends Error {
  constructor(status, body) {
    const env = body && typeof body === "object" ? body.error ?? {} : {};
    super(env.message ?? `HTTP ${status}`);
    this.status = status;
    this.code = env.code ?? null;
  }
}
class TransportError extends Error {}
class RetryableError extends Error { retryable = true; }
class PermanentError extends Error { retryable = false; }
class LeaseLost extends Error {}

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(resolve, ms);
  signal?.addEventListener("abort", () => { clearTimeout(t); resolve(); }, { once: true });
});
const RETRYABLE = new Set([408, 425, 429, 500, 502, 503, 504]);
const isRetryable = (e) => e instanceof TransportError || (e instanceof ApiError && RETRYABLE.has(e.status));

async function request(method, path, body) {
  let res;
  try {
    res = await fetch(`${BASE}${path}`, {
      method,
      headers: {
        "x-api-key": env.ORCH8_API_KEY,
        "x-tenant-id": env.ORCH8_TENANT_ID,
        accept: "application/json",
        ...(body === undefined ? {} : { "content-type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    throw new TransportError(String(e));
  }
  const text = await res.text();
  let json = null;
  if (text) { try { json = JSON.parse(text); } catch { json = null; } }
  if (res.status >= 400) throw new ApiError(res.status, json);
  return json;
}

async function requestWithRetry(method, path, body, attempts) {
  const base = Number(env.ORCH8_RETRY_BASE_DELAY_MS ?? 250);
  for (let i = 1; ; i += 1) {
    try {
      return await request(method, path, body);
    } catch (e) {
      if (i >= attempts || !isRetryable(e)) throw e;
      await sleep(base * 2 ** (i - 1));
    }
  }
}

// --------------------------------------------------------------------------
// Handlers
// --------------------------------------------------------------------------

const handlers = {
  echo: async (task) => ({ echo: task.params }),
  fail_retryable: async (task) => { throw new RetryableError(task.params?.message ?? "boom"); },
  fail_permanent: async () => { throw new PermanentError("fatal"); },
  crash: async () => { throw new Error("crash"); },
  checkpoint: async (task, ctx) => {
    const start = task.resume_checkpoint?.step ?? 0;
    const steps = task.params?.steps ?? 3;
    for (let i = start + 1; i <= steps; i += 1) await ctx.checkpoint({ step: i });
    return { resumed_from: start, final_step: steps };
  },
  slow: async (task, ctx) => {
    await sleep(task.params?.sleep_ms ?? 1000, ctx.signal);
    return { slept: task.params?.sleep_ms ?? 1000 };
  },
};

// --------------------------------------------------------------------------
// Worker
// --------------------------------------------------------------------------

class Worker {
  constructor({ concurrency, pollIntervalMs, queue, version }) {
    this.concurrency = concurrency;
    this.pollIntervalMs = pollIntervalMs;
    this.queue = queue;
    this.version = version;
    this.free = concurrency;
    this.running = false;
    this.inFlight = new Map();
    this.heartbeatMs = 15000;
    this.loops = [];
    this.wake = new AbortController();
  }

  start() {
    this.running = true;
    for (const name of HANDLER_NAMES) this.loops.push(this.#loop(name));
    this.hbTimer = setTimeout(() => this.#heartbeatTick(), 200);
  }

  async stop(drainMs) {
    this.running = false;
    this.wake.abort();
    await Promise.allSettled(this.loops);
    const deadline = Date.now() + drainMs;
    while (this.inFlight.size > 0 && Date.now() < deadline) await sleep(20);
    clearTimeout(this.hbTimer);
  }

  async #loop(handlerName) {
    let failures = 0;
    while (this.running) {
      if (this.free <= 0) { await sleep(25, this.wake.signal); continue; }
      let delay = this.pollIntervalMs;
      try {
        const res = await this.pollOnce(handlerName, this.queue, this.concurrency);
        failures = 0;
        if (res.tasks.length === 0) delay = BUG === "ignore_poll_hint" ? delay : Math.max(delay, res.poll_after_ms ?? 0);
        else delay = 0;
      } catch {
        failures += 1;
        delay = Math.min(this.pollIntervalMs * 2 ** failures, 30000);
      }
      if (delay > 0) await sleep(delay, this.wake.signal);
    }
  }

  // Slots are reserved BEFORE the poll is sent: concurrent per-handler loops
  // must never ask the engine for more tasks than the worker can start (P11).
  async pollOnce(handlerName, queue, wanted) {
    const limit = Math.min(wanted, this.free);
    if (limit <= 0) return { tasks: [], poll_after_ms: 0 };
    this.free -= limit;
    let res;
    try {
      const body = { handler_name: handlerName, worker_id: WORKER_ID, limit };
      if (queue) body.queue_name = queue;
      if (this.version) body.version = this.version;
      res = await request("POST", queue ? "/workers/tasks/poll/queue" : "/workers/tasks/poll", body);
    } catch (e) {
      this.free += limit;
      throw e;
    }
    const tasks = res.tasks.slice(0, limit);
    this.free += limit - tasks.length;
    if (res.heartbeat_interval_secs) {
      this.heartbeatMs = Math.min(15000, res.heartbeat_interval_secs * 1000, (res.lease_secs ?? Infinity) * 500);
    }
    for (const task of tasks) this.#execute(task);
    return res;
  }

  async #heartbeatTick() {
    await Promise.allSettled([...this.inFlight.values()].map((s) => s.lost ? null : this.#mutate(s, "heartbeat", {})));
    if (this.running || this.inFlight.size > 0) this.hbTimer = setTimeout(() => this.#heartbeatTick(), this.heartbeatMs);
  }

  async #mutate(state, kind, extra) {
    const body = { worker_id: WORKER_ID, claim_epoch: state.task.claim_epoch, ...extra };
    try {
      return await request("POST", `/workers/tasks/${encodeURIComponent(state.task.id)}/${kind}`, body);
    } catch (e) {
      if (BUG !== "ack_after_loss" && e instanceof ApiError && (e.status === 404 || e.status === 409)) {
        state.lost = true;
        state.abort.abort();
        throw new LeaseLost(e.message);
      }
      throw e;
    }
  }

  async #ack(state, kind, extra) {
    for (let i = 1; ; i += 1) {
      try {
        await this.#mutate(state, kind, extra);
        return;
      } catch (e) {
        if (e instanceof LeaseLost || !isRetryable(e) || i >= 5) return;
        await sleep(Math.min(200 * 2 ** (i - 1), 5000));
      }
    }
  }

  async #execute(task) {
    const state = { task, lost: false, abort: new AbortController(), seq: task.checkpoint_seq ?? 0 };
    this.inFlight.set(task.id, state);
    const ctx = {
      signal: state.abort.signal,
      checkpoint: async (value) => {
        const res = await this.#mutate(state, "heartbeat", { checkpoint: value, checkpoint_seq: state.seq });
        state.seq = res.checkpoint_seq;
      },
    };
    try {
      const handler = handlers[task.handler_name];
      let output;
      try {
        if (!handler) throw new PermanentError(`no handler registered for ${task.handler_name}`);
        output = await handler(task, ctx);
      } catch (e) {
        if (state.lost || e instanceof LeaseLost) return;
        const retryable = typeof e?.retryable === "boolean" ? e.retryable : BUG !== "crash_permanent";
        await this.#ack(state, "fail", { message: String(e?.message ?? e) || "error", retryable });
        return;
      }
      if (state.lost) return;
      await this.#ack(state, "complete", { output: output ?? {} });
    } finally {
      this.inFlight.delete(task.id);
      this.free += 1;
    }
  }
}

function workerFromEnv() {
  return new Worker({
    concurrency: Number(env.ORCH8_CONCURRENCY ?? 4),
    pollIntervalMs: Number(env.ORCH8_POLL_INTERVAL_MS ?? 1000),
    queue: env.ORCH8_QUEUE || null,
    version: env.ORCH8_WORKER_VERSION || null,
  });
}

async function runWorker() {
  const worker = workerFromEnv();
  worker.start();
  await new Promise((resolve) => process.once("SIGTERM", resolve));
  await worker.stop(Number(env.ORCH8_SHUTDOWN_TIMEOUT_MS ?? 10000));
  process.exit(0);
}

async function runPush() {
  const worker = workerFromEnv();
  worker.running = true;
  const secret = env.ORCH8_PUSH_SECRET;
  const tolerance = Number(env.ORCH8_PUSH_TOLERANCE_SECS ?? 300);
  const server = createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const raw = Buffer.concat(chunks);
      const ok = verify({
        secret,
        timestamp: req.headers["x-orch8-timestamp"],
        signature: req.headers["x-orch8-signature"],
        body: raw,
        toleranceSecs: tolerance,
      });
      if (!ok) { res.writeHead(401).end(); return; }
      let envelope;
      try { envelope = JSON.parse(raw.toString("utf8")); } catch { res.writeHead(400).end(); return; }
      res.writeHead(202).end();
      worker.pollOnce(envelope.handler_name, envelope.queue_name, 1).catch(() => {});
    });
  });
  server.listen(Number(env.ORCH8_PUSH_PORT), "0.0.0.0", () => process.stdout.write("READY\n"));
  await new Promise((resolve) => process.once("SIGTERM", resolve));
  server.close();
  worker.running = false;
  const deadline = Date.now() + 10000;
  while (worker.inFlight.size > 0 && Date.now() < deadline) await sleep(20);
  process.exit(0);
}

async function eachLine(fn) {
  const rl = createInterface({ input: process.stdin });
  for await (const line of rl) {
    if (!line.trim()) continue;
    process.stdout.write(`${JSON.stringify(await fn(JSON.parse(line)))}\n`);
  }
}

function runVerify() {
  return eachLine(async (v) => ({
    valid: BUG === "accept_all_signatures" || verify({
      secret: v.secret,
      timestamp: v.timestamp ?? undefined,
      signature: v.signature ?? undefined,
      body: Buffer.from(v.body, "utf8"),
      now: v.now,
      toleranceSecs: v.tolerance_secs ?? 300,
    }),
  }));
}

const KIND = { 400: "invalid_argument", 401: "unauthorized", 403: "forbidden", 404: "not_found", 409: "conflict", 413: "payload_too_large", 422: "unprocessable", 429: "rate_limited" };

function qs(query) {
  const entries = Object.entries(query ?? {}).filter(([, v]) => v !== undefined && v !== null);
  return entries.length ? `?${new URLSearchParams(entries.map(([k, v]) => [k, String(v)]))}` : "";
}

async function clientOp(op, a) {
  const attempts = Number(env.ORCH8_MAX_ATTEMPTS ?? 3);
  const get = (p) => requestWithRetry("GET", p, undefined, attempts);
  const send = (m, p, b) => (BUG === "retry_unsafe" ? requestWithRetry(m, p, b, attempts) : request(m, p, b));
  const seg = encodeURIComponent;
  switch (op) {
    case "sequences.create": return send("POST", "/sequences", a.body);
    case "sequences.get": return get(`/sequences/${seg(a.id)}`);
    case "sequences.list": return get(`/sequences${qs(a.query)}`);
    case "instances.create": return send("POST", "/instances", a.body);
    case "instances.get": return get(`/instances/${seg(a.id)}`);
    case "instances.list": return get(`/instances${qs(a.query)}`);
    case "instances.signal": return send("POST", `/instances/${seg(a.id)}/signals`, a.payload === undefined ? { signal_type: a.signal_type } : { signal_type: a.signal_type, payload: a.payload });
    case "instances.cancel": return send("POST", `/instances/${seg(a.id)}/signals`, { signal_type: "cancel" });
    case "jobs.enqueue": return send("POST", "/jobs", Object.fromEntries(Object.entries(a.body).filter(([, v]) => v !== null && v !== undefined)));
    case "jobs.get": return get(`/jobs/${seg(a.id)}`);
    case "jobs.list": return get(`/jobs${qs(a.query)}`);
    case "jobs.cancel": return send("DELETE", `/jobs/${seg(a.id)}`);
    default: throw new Error(`unknown op ${op}`);
  }
}

function runClient() {
  return eachLine(async ({ id, op, args }) => {
    try {
      return { id, ok: true, result: (await clientOp(op, args)) ?? null };
    } catch (e) {
      if (e instanceof ApiError) {
        const kind = KIND[e.status] ?? (e.status >= 500 ? "server" : "api");
        return { id, ok: false, error: { kind, status: e.status, code: e.code, message: e.message } };
      }
      return { id, ok: false, error: { kind: "transport", status: null, code: null, message: String(e.message ?? e) } };
    }
  });
}

const mode = process.argv[2];
const modes = { worker: runWorker, push: runPush, verify: runVerify, client: runClient };
if (!modes[mode]) {
  process.stderr.write(`usage: adapter.mjs <${Object.keys(modes).join("|")}>\n`);
  process.exit(2);
}
await modes[mode]();
