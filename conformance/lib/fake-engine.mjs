// Scripted fake Orch8 engine for the worker/client conformance kit.
// Implements the worker protocol described in ../../WORKER_PROTOCOL.md with
// per-scenario hooks, and records every request for later assertions.

import { createServer } from "node:http";

export const API_KEY = "conformance-key";
export const TENANT_ID = "conformance-tenant";

export function errorBody(code, message) {
  return { error: { code, message, request_id: null } };
}

let taskCounter = 0;

/** Build a claimed-shape worker task. `overrides` wins. */
export function makeTask(handler, overrides = {}) {
  taskCounter += 1;
  const n = String(taskCounter).padStart(12, "0");
  return {
    id: `00000000-0000-7000-8000-${n}`,
    instance_id: `00000000-0000-7000-9000-${n}`,
    block_id: `step_${taskCounter}`,
    handler_name: handler,
    params: {},
    context: { data: {}, config: {} },
    attempt: 0,
    timeout_ms: null,
    state: "pending",
    worker_id: null,
    claimed_at: null,
    heartbeat_at: null,
    claim_epoch: 0,
    checkpoint_seq: 0,
    completed_at: null,
    output: null,
    error_message: null,
    error_retryable: null,
    created_at: new Date().toISOString(),
    ...overrides,
  };
}

export class FakeEngine {
  /**
   * @param {object} opts
   * @param {number} [opts.leaseSecs]
   * @param {number} [opts.heartbeatIntervalSecs]
   * @param {string} [opts.bindHost]
   */
  constructor(opts = {}) {
    this.leaseSecs = opts.leaseSecs ?? 60;
    this.heartbeatIntervalSecs = opts.heartbeatIntervalSecs ?? 15;
    this.bindHost = opts.bindHost ?? "127.0.0.1";
    /** @type {Array<object>} */
    this.requests = [];
    /** Pending tasks, served FIFO. Each may carry `_queue` and `_availableAt`. */
    this.pending = [];
    /** Claimed / settled tasks by id. */
    this.tasks = new Map();
    this.hooks = {};
    this.routes = [];
    this.listeners = [];
    this.startedAt = Date.now();
  }

  /** Queue a task. `opts.queue` binds it to a named queue; `opts.availableAfterMs` delays it. */
  addTask(task, opts = {}) {
    this.pending.push({
      ...task,
      _queue: opts.queue ?? null,
      _availableAt: Date.now() + (opts.availableAfterMs ?? 0),
      _epochBase: task.claim_epoch ?? 0,
    });
    return task;
  }

  /** Register a generic route: `(req) => {status, body, headers} | undefined`. */
  route(method, pathPattern, handler) {
    this.routes.push({ method, pathPattern, handler });
  }

  /** Wait until `pred(engine)` is true, or reject after `timeoutMs`. */
  waitFor(pred, timeoutMs, label = "condition") {
    return new Promise((resolve, reject) => {
      const started = Date.now();
      const tick = () => {
        let ok = false;
        try { ok = pred(this); } catch { ok = false; }
        if (ok) return resolve();
        if (Date.now() - started > timeoutMs) {
          return reject(new Error(`timed out after ${timeoutMs}ms waiting for ${label}`));
        }
        setTimeout(tick, 20);
      };
      tick();
    });
  }

  /** Requests matching method + exact path (query stripped). */
  find(method, path) {
    return this.requests.filter((r) => r.method === method && r.path === path);
  }

  byKind(kind, taskId) {
    return this.requests.filter((r) => r.kind === kind && (taskId === undefined || r.taskId === taskId));
  }

  inFlightCount() {
    let n = 0;
    for (const t of this.tasks.values()) if (t.state === "claimed") n += 1;
    return n;
  }

  async start() {
    this.server = createServer((req, res) => this.#handle(req, res));
    await new Promise((resolve) => this.server.listen(0, this.bindHost, resolve));
    this.port = this.server.address().port;
    return this.port;
  }

  async stop() {
    if (!this.server) return;
    this.server.closeAllConnections?.();
    await new Promise((resolve) => this.server.close(resolve));
  }

  #handle(req, res) {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", async () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      const url = new URL(req.url, "http://fake");
      let body = null;
      let bodyError = null;
      if (raw.length > 0) {
        try { body = JSON.parse(raw); } catch (e) { bodyError = String(e); }
      }
      const record = {
        at: Date.now(),
        method: req.method,
        rawPath: url.pathname,
        path: url.pathname.replace(/^\/api\/v1(?=\/)/, ""),
        rawUrl: req.url,
        query: Object.fromEntries(url.searchParams.entries()),
        headers: req.headers,
        body,
        rawBody: raw,
        bodyError,
        kind: "other",
      };
      this.requests.push(record);
      let result;
      try {
        result = await this.#dispatch(record);
      } catch (e) {
        result = { status: 500, body: errorBody("internal", String(e)) };
      }
      record.status = result.status;
      record.respondedAt = Date.now();
      const headers = { ...(result.headers ?? {}) };
      if (req.headers["x-request-id"]) headers["x-request-id"] = req.headers["x-request-id"];
      if (result.status === 204 || result.body === undefined || result.body === null) {
        res.writeHead(result.status, headers);
        res.end();
      } else {
        headers["content-type"] = "application/json";
        res.writeHead(result.status, headers);
        res.end(JSON.stringify(result.body));
      }
    });
  }

  async #dispatch(r) {
    if (r.headers["x-api-key"] !== API_KEY) {
      return { status: 401, body: errorBody("unauthorized", "unauthorized") };
    }
    if (r.headers["x-tenant-id"] !== TENANT_ID) {
      return { status: 400, body: errorBody("invalid_argument", "missing required X-Tenant-Id header") };
    }
    if (!r.rawPath.startsWith("/api/v1/")) {
      return { status: 404, body: errorBody("not_found", `expected /api/v1 prefix, got ${r.rawPath}`) };
    }
    for (const route of this.routes) {
      if (route.method !== r.method) continue;
      const m = matchPath(route.pathPattern, r.path);
      if (!m) continue;
      r.kind = `${route.method} ${route.pathPattern}`;
      r.params = m;
      const out = await route.handler(r, this);
      if (out) return out;
    }
    if (r.method === "POST" && (r.path === "/workers/tasks/poll" || r.path === "/workers/tasks/poll/queue")) {
      r.kind = "poll";
      return this.#poll(r);
    }
    const m = r.path.match(/^\/workers\/tasks\/([^/]+)\/(heartbeat|complete|fail)$/);
    if (r.method === "POST" && m) {
      r.kind = m[2];
      r.taskId = decodeURIComponent(m[1]);
      return this.#mutation(r, m[2]);
    }
    return { status: 404, body: errorBody("not_found", `no route ${r.method} ${r.path}`) };
  }

  #poll(r) {
    const b = r.body ?? {};
    r.inFlightAtPoll = this.inFlightCount();
    if (this.hooks.onPoll) {
      const out = this.hooks.onPoll(r, this);
      if (out) return out;
    }
    if (typeof b.handler_name !== "string" || typeof b.worker_id !== "string") {
      return { status: 400, body: errorBody("invalid_argument", "handler_name and worker_id are required") };
    }
    const queue = r.path.endsWith("/queue") ? b.queue_name ?? null : null;
    const limit = Math.min(Number.isInteger(b.limit) ? b.limit : 1, 1000);
    const now = Date.now();
    const out = [];
    for (let i = 0; i < this.pending.length && out.length < limit; ) {
      const t = this.pending[i];
      if (t.handler_name === b.handler_name && t._queue === queue && t._availableAt <= now) {
        this.pending.splice(i, 1);
        const claimed = {
          ...t,
          state: "claimed",
          worker_id: b.worker_id,
          claimed_at: new Date().toISOString(),
          heartbeat_at: new Date().toISOString(),
          claim_epoch: t._epochBase + 1,
          queue_name: t._queue ?? undefined,
        };
        delete claimed._queue;
        delete claimed._availableAt;
        delete claimed._epochBase;
        this.tasks.set(claimed.id, { ...claimed, claimedAt: now });
        out.push(claimed);
      } else {
        i += 1;
      }
    }
    r.claimed = out.map((t) => t.id);
    const empty = out.length === 0;
    return {
      status: 200,
      headers: empty ? { "retry-after": "1" } : {},
      body: {
        tasks: out,
        lease_secs: this.leaseSecs,
        heartbeat_interval_secs: this.heartbeatIntervalSecs,
        poll_after_ms: empty ? 1000 : 0,
      },
    };
  }

  #mutation(r, kind) {
    const task = this.tasks.get(r.taskId);
    const b = r.body ?? {};
    const hook = this.hooks[`on${kind[0].toUpperCase()}${kind.slice(1)}`];
    if (hook) {
      const out = hook(r, task, this);
      if (out) {
        if (out.loseLease && task) task.state = "lost";
        return out;
      }
    }
    if (!task) return { status: 404, body: errorBody("not_found", `not found: worker_task ${r.taskId}`) };
    const owns = b.worker_id === task.worker_id && b.claim_epoch === task.claim_epoch;
    if (kind === "complete" && task.state === "completed" && owns) {
      return { status: 200, body: null };
    }
    if (task.state !== "claimed" || !owns) {
      return { status: 409, body: errorBody("conflict", "conflict: worker task lease changed") };
    }
    if (kind === "heartbeat") {
      if (b.checkpoint !== undefined && b.checkpoint !== null) {
        if (!Number.isInteger(b.checkpoint_seq)) {
          return { status: 400, body: errorBody("invalid_argument", "checkpoint_seq is required with checkpoint") };
        }
        if (b.checkpoint_seq !== task.checkpoint_seq) {
          return { status: 409, body: errorBody("conflict", "conflict: worker task ownership or checkpoint sequence changed") };
        }
        task.checkpoint_seq += 1;
        task.resume_checkpoint = b.checkpoint;
      }
      return { status: 200, body: { checkpoint_seq: task.checkpoint_seq } };
    }
    if (kind === "complete") {
      task.state = "completed";
      task.output = b.output;
      task.settledAt = Date.now();
      return { status: 200, body: null };
    }
    task.state = "failed";
    task.error_message = b.message;
    task.error_retryable = b.retryable;
    task.settledAt = Date.now();
    return { status: 200, body: null };
  }
}

function matchPath(pattern, path) {
  const p = pattern.split("/");
  const a = path.split("/");
  if (p.length !== a.length) return null;
  const params = {};
  for (let i = 0; i < p.length; i += 1) {
    if (p[i].startsWith(":")) params[p[i].slice(1)] = decodeURIComponent(a[i]);
    else if (p[i] !== a[i]) return null;
  }
  return params;
}
