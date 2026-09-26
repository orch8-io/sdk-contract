// Conformance scenarios. Each scenario gets a fresh FakeEngine and a harness
// (`h`) that knows how to launch the adapter under test. Assertions throw.
//
// Rule ids (P8, H1, L2, ...) refer to ../../WORKER_PROTOCOL.md.

import { readFileSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { API_KEY, TENANT_ID, errorBody, makeTask } from "./fake-engine.mjs";
import { signatureHeader } from "./sign.mjs";

const here = dirname(fileURLToPath(import.meta.url));
export const VECTORS_PATH = resolve(here, "../../fixtures/push_signatures.json");

export const HANDLERS = ["echo", "fail_retryable", "fail_permanent", "crash", "checkpoint", "slow"];

class AssertionError extends Error {}

function check(cond, message) {
  if (!cond) throw new AssertionError(message);
}

function deepEqual(a, b) {
  return JSON.stringify(sortKeys(a)) === JSON.stringify(sortKeys(b));
}

function sortKeys(v) {
  if (Array.isArray(v)) return v.map(sortKeys);
  if (v && typeof v === "object") {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, sortKeys(v[k])]));
  }
  return v;
}

function checkAuth(engine) {
  const bad = engine.requests.filter(
    (r) => r.headers["x-api-key"] !== API_KEY || r.headers["x-tenant-id"] !== TENANT_ID,
  );
  check(bad.length === 0, `T3/T4: ${bad.length} request(s) missing x-api-key / x-tenant-id (first: ${bad[0]?.method} ${bad[0]?.rawPath})`);
  const unversioned = engine.requests.filter((r) => !r.rawPath.startsWith("/api/v1/"));
  check(unversioned.length === 0, `T1: request outside /api/v1: ${unversioned[0]?.rawPath}`);
  const nonJson = engine.requests.filter(
    (r) => r.rawBody.length > 0 && !(r.headers["content-type"] ?? "").includes("application/json"),
  );
  check(nonJson.length === 0, `T2: JSON body sent without application/json content-type (${nonJson[0]?.method} ${nonJson[0]?.rawPath})`);
}

function acksFor(engine, taskId) {
  return engine.requests.filter((r) => (r.kind === "complete" || r.kind === "fail") && r.taskId === taskId);
}

function checkMutationClaim(engine, task) {
  const muts = engine.requests.filter((r) => r.taskId === task.id);
  const epoch = engine.tasks.get(task.id)?.claim_epoch;
  for (const m of muts) {
    check(m.body?.worker_id === engine.tasks.get(task.id)?.worker_id,
      `3.3: ${m.kind} for ${task.handler_name} sent worker_id ${JSON.stringify(m.body?.worker_id)}`);
    check(m.body?.claim_epoch === epoch,
      `3.3: ${m.kind} for ${task.handler_name} must echo claim_epoch ${epoch}, got ${JSON.stringify(m.body?.claim_epoch)}`);
  }
}

// ---------------------------------------------------------------------------
// Worker scenarios
// ---------------------------------------------------------------------------

const workerScenarios = [
  {
    name: "worker.poll_complete",
    rules: "T1-T4, P1, P11, 3.3, K1",
    async run(h) {
      const engine = await h.engine();
      const task = engine.addTask(makeTask("echo", { params: { greeting: "hi", n: 7 }, claim_epoch: 6 }));
      const w = await h.worker(engine, { ORCH8_CONCURRENCY: "3" });
      await engine.waitFor((e) => acksFor(e, task.id).length > 0, 15000, "complete of echo task");
      await engine.waitFor((e) => HANDLERS.every((n) => e.byKind("poll").some((p) => p.body?.handler_name === n)),
        8000, "P2: a poll for every registered handler").catch(() => {});
      await w.stop();
      const polls = engine.byKind("poll");
      check(polls.length > 0, "P1: no polls observed");
      for (const p of polls) {
        check(p.path === "/workers/tasks/poll", `P3: default worker must poll /workers/tasks/poll, got ${p.path}`);
        check(HANDLERS.includes(p.body?.handler_name), `P1: poll handler_name ${JSON.stringify(p.body?.handler_name)} is not a registered handler`);
        check(p.body?.worker_id === h.workerId, `P1: poll worker_id ${JSON.stringify(p.body?.worker_id)} != ORCH8_WORKER_ID`);
        check(Number.isInteger(p.body?.limit) && p.body.limit >= 1 && p.body.limit <= 3,
          `P11: poll limit must be an integer in [1, concurrency=3], got ${JSON.stringify(p.body?.limit)}`);
      }
      const handlersPolled = new Set(polls.map((p) => p.body.handler_name));
      check(HANDLERS.every((n) => handlersPolled.has(n)), `P2: every registered handler must be polled; polled ${[...handlersPolled]}`);
      const acks = acksFor(engine, task.id);
      check(acks.length === 1 && acks[0].kind === "complete", `K1: expected exactly one complete, got ${acks.map((a) => a.kind)}`);
      check(deepEqual(acks[0].body.output, { echo: { greeting: "hi", n: 7 } }),
        `K1: complete output mismatch: ${JSON.stringify(acks[0].body.output)}`);
      checkMutationClaim(engine, task);
      check(w.exitCode === 0, `7.7: worker must exit 0 after SIGTERM, got ${w.exitCode}`);
      checkAuth(engine);
    },
  },
  {
    name: "worker.empty_poll_cadence",
    rules: "P8",
    async run(h) {
      const engine = await h.engine();
      const w = await h.worker(engine, { ORCH8_POLL_INTERVAL_MS: "50" });
      const since = Date.now();
      await new Promise((r) => setTimeout(r, 3200));
      await w.stop();
      const byHandler = new Map();
      for (const p of engine.byKind("poll")) {
        const k = p.body?.handler_name;
        if (!byHandler.has(k)) byHandler.set(k, []);
        byHandler.get(k).push(p);
      }
      check(byHandler.size > 0, "no polls observed");
      for (const [handler, all] of byHandler) {
        const polls = all.filter((p) => p.at >= since - 1500);
        check(polls.length >= 2, `P7: handler ${handler} must keep polling while idle (saw ${polls.length} poll in 3.2s)`);
        for (let i = 1; i < polls.length; i += 1) {
          const gap = polls[i].at - polls[i - 1].respondedAt;
          check(gap >= 900, `P8: after an empty poll (poll_after_ms=1000) handler ${handler} polled again after ${gap}ms`);
        }
      }
      checkAuth(engine);
    },
  },
  {
    name: "worker.failures",
    rules: "F1, F4",
    async run(h) {
      const engine = await h.engine();
      const retryable = engine.addTask(makeTask("fail_retryable", { params: { message: "boom" } }));
      const permanent = engine.addTask(makeTask("fail_permanent"));
      const crash = engine.addTask(makeTask("crash"));
      const w = await h.worker(engine);
      await engine.waitFor((e) => [retryable, permanent, crash].every((t) => acksFor(e, t.id).length > 0), 15000, "fail of 3 tasks");
      await w.stop();
      const expect = [[retryable, true], [permanent, false], [crash, true]];
      for (const [task, want] of expect) {
        const acks = acksFor(engine, task.id);
        check(acks.length === 1 && acks[0].kind === "fail", `F1: ${task.handler_name} must be reported once via fail, got ${acks.map((a) => a.kind)}`);
        const b = acks[0].body;
        check(typeof b.retryable === "boolean", `F1: retryable must be sent explicitly for ${task.handler_name}`);
        check(b.retryable === want, `F4: ${task.handler_name} must be retryable=${want}, got ${b.retryable}`);
        check(typeof b.message === "string" && b.message.length > 0, `F1: ${task.handler_name} fail message must be a non-empty string`);
        checkMutationClaim(engine, task);
      }
      check(acksFor(engine, retryable.id)[0].body.message.includes("boom"), "F1: handler error message must be propagated");
      checkAuth(engine);
    },
  },
  {
    name: "worker.checkpoint_resume",
    rules: "C1-C3, C6",
    async run(h) {
      const engine = await h.engine();
      const task = engine.addTask(makeTask("checkpoint", {
        params: { steps: 3 },
        resume_checkpoint: { step: 1 },
        checkpoint_seq: 5,
      }));
      const w = await h.worker(engine);
      await engine.waitFor((e) => acksFor(e, task.id).length > 0, 15000, "complete of checkpoint task");
      await w.stop();
      const cps = engine.byKind("heartbeat", task.id).filter((r) => r.body?.checkpoint !== undefined && r.body?.checkpoint !== null);
      check(cps.length === 2, `C6: resuming from step 1 of 3 must write exactly 2 checkpoints, got ${cps.length}`);
      check(deepEqual(cps[0].body.checkpoint, { step: 2 }) && cps[0].body.checkpoint_seq === 5,
        `C3: first checkpoint must be {step:2} with checkpoint_seq 5 (from poll), got ${JSON.stringify(cps[0].body)}`);
      check(deepEqual(cps[1].body.checkpoint, { step: 3 }) && cps[1].body.checkpoint_seq === 6,
        `C2: second checkpoint must use the returned seq 6, got ${JSON.stringify(cps[1].body)}`);
      for (const c of cps) check(c.status === 200, `C4: checkpoint rejected with ${c.status}`);
      const acks = acksFor(engine, task.id);
      check(acks.length === 1 && acks[0].kind === "complete", `expected one complete, got ${acks.map((a) => a.kind)}`);
      check(deepEqual(acks[0].body.output, { resumed_from: 1, final_step: 3 }),
        `C6: output must be {resumed_from:1, final_step:3}, got ${JSON.stringify(acks[0].body.output)}`);
      checkMutationClaim(engine, task);
      checkAuth(engine);
    },
  },
  {
    name: "worker.heartbeat_cadence",
    rules: "P9, H1",
    async run(h) {
      const engine = await h.engine({ leaseSecs: 4, heartbeatIntervalSecs: 1 });
      const task = engine.addTask(makeTask("slow", { params: { sleep_ms: 3200 } }));
      const w = await h.worker(engine);
      await engine.waitFor((e) => acksFor(e, task.id).length > 0, 15000, "complete of slow task");
      await w.stop();
      const hbs = engine.byKind("heartbeat", task.id);
      check(hbs.length >= 2, `P9: a 3.2s task with heartbeat_interval_secs=1 must heartbeat at least twice, got ${hbs.length}`);
      const claimedAt = engine.tasks.get(task.id).claimedAt;
      const times = [claimedAt, ...hbs.map((r) => r.at)];
      for (let i = 1; i < times.length; i += 1) {
        check(times[i] - times[i - 1] <= 1600, `P9: heartbeat gap ${times[i] - times[i - 1]}ms exceeds the 1s server hint`);
      }
      for (const hb of hbs) {
        check(hb.body?.checkpoint === undefined || hb.body?.checkpoint === null, "H1: plain heartbeat must not carry a checkpoint");
      }
      const acks = acksFor(engine, task.id);
      check(acks.length === 1 && acks[0].kind === "complete", `expected one complete, got ${acks.map((a) => a.kind)}`);
      check(deepEqual(acks[0].body.output, { slept: 3200 }), `slow output mismatch: ${JSON.stringify(acks[0].body.output)}`);
      checkMutationClaim(engine, task);
    },
  },
  {
    name: "worker.lease_loss",
    rules: "H2, L2, L3",
    async run(h) {
      const engine = await h.engine({ leaseSecs: 4, heartbeatIntervalSecs: 1 });
      const lost409 = engine.addTask(makeTask("slow", { params: { sleep_ms: 4000 } }));
      const lost404 = engine.addTask(makeTask("slow", { params: { sleep_ms: 4000 } }));
      const after = engine.addTask(makeTask("echo", { params: { after: "lease-loss" } }), { availableAfterMs: 1800 });
      const lostAt = new Map();
      engine.hooks.onHeartbeat = (r) => {
        if (r.taskId === lost409.id) {
          if (!lostAt.has(r.taskId)) lostAt.set(r.taskId, Date.now());
          return { status: 409, loseLease: true, body: errorBody("conflict", "conflict: worker task ownership or checkpoint sequence changed") };
        }
        if (r.taskId === lost404.id) {
          if (!lostAt.has(r.taskId)) lostAt.set(r.taskId, Date.now());
          return { status: 404, loseLease: true, body: errorBody("not_found", `not found: worker_task ${r.taskId}`) };
        }
        return undefined;
      };
      const w = await h.worker(engine, { ORCH8_CONCURRENCY: "4" });
      await engine.waitFor((e) => acksFor(e, after.id).length > 0, 15000, "worker to keep processing after lease loss");
      // Give the lost handlers time to finish (4s sleep) and misbehave if they will.
      await new Promise((r) => setTimeout(r, 3500));
      await w.stop();
      for (const task of [lost409, lost404]) {
        check(lostAt.has(task.id), `H2: no heartbeat observed for ${task.id}`);
        const later = engine.byKind("heartbeat", task.id).filter((r) => r.at > lostAt.get(task.id) + 100);
        check(later.length === 0, `L2: ${later.length} heartbeat(s) sent after the lease was lost`);
        const acks = acksFor(engine, task.id);
        check(acks.length === 0, `L2: must not ${acks.map((a) => a.kind).join("/")} a task after losing its lease`);
      }
      const acks = acksFor(engine, after.id);
      check(acks.length === 1 && acks[0].kind === "complete", "L3: worker must keep processing other tasks after a lease loss");
    },
  },
  {
    name: "worker.ack_retry",
    rules: "K3, L3",
    async run(h) {
      const engine = await h.engine();
      const flaky = engine.addTask(makeTask("echo", { params: { which: "flaky" } }));
      const stolen = engine.addTask(makeTask("echo", { params: { which: "stolen" } }));
      let flakyCalls = 0;
      engine.hooks.onComplete = (r) => {
        if (r.taskId === flaky.id) {
          flakyCalls += 1;
          if (flakyCalls === 1) return { status: 503, body: errorBody("unavailable", "unavailable: storage backend temporarily unavailable") };
        }
        if (r.taskId === stolen.id) {
          return { status: 409, loseLease: true, body: errorBody("conflict", "conflict: worker task lease changed") };
        }
        return undefined;
      };
      const w = await h.worker(engine);
      await engine.waitFor((e) => acksFor(e, flaky.id).length >= 2 && acksFor(e, stolen.id).length >= 1, 15000, "complete retry");
      await new Promise((r) => setTimeout(r, 1500));
      await w.stop();
      const f = acksFor(engine, flaky.id);
      check(f.length === 2 && f.every((a) => a.kind === "complete"), `K3: complete must be retried once after 503, got ${f.map((a) => `${a.kind}:${a.status}`)}`);
      check(deepEqual(f[0].body, f[1].body), "K3: a complete retry must resend the identical body");
      const s = acksFor(engine, stolen.id);
      check(s.length === 1 && s[0].kind === "complete", `L3: a 409 on complete must not be retried or turned into fail, got ${s.map((a) => a.kind)}`);
    },
  },
  {
    name: "worker.poll_errors",
    rules: "7.2",
    async run(h) {
      const engine = await h.engine();
      let first = 0;
      engine.hooks.onPoll = () => {
        if (!first) first = Date.now();
        return Date.now() - first < 1200
          ? { status: 503, body: errorBody("unavailable", "unavailable: pool exhausted") }
          : undefined;
      };
      const task = engine.addTask(makeTask("echo", { params: { after: "outage" } }));
      const w = await h.worker(engine);
      await engine.waitFor((e) => acksFor(e, task.id).length > 0, 15000, "recovery after poll errors");
      await w.stop();
      check(w.exitCode === 0, `7.2: worker must survive poll errors and exit 0, got ${w.exitCode}`);
      const failing = engine.byKind("poll").filter((p) => p.status === 503);
      check(failing.length >= 1, "scenario did not observe any failing poll");
      check(failing.length <= 6 * HANDLERS.length,
        `7.2: ${failing.length} polls in 1.2s of outage — poll errors must back off exponentially`);
    },
  },
  {
    name: "worker.concurrency",
    rules: "P11, 7.1",
    async run(h) {
      const engine = await h.engine();
      const tasks = [];
      for (let i = 0; i < 5; i += 1) tasks.push(engine.addTask(makeTask("slow", { params: { sleep_ms: 700 } })));
      let maxInFlight = 0;
      const origPoll = engine.hooks.onPoll;
      engine.hooks.onPoll = (r, e) => {
        maxInFlight = Math.max(maxInFlight, e.inFlightCount());
        return origPoll?.(r, e);
      };
      const w = await h.worker(engine, { ORCH8_CONCURRENCY: "2" });
      await engine.waitFor((e) => tasks.every((t) => acksFor(e, t.id).length > 0), 20000, "completion of 5 slow tasks");
      await w.stop();
      // Reconstruct in-flight intervals from claim / ack timestamps.
      const intervals = tasks.map((t) => [engine.tasks.get(t.id).claimedAt, acksFor(engine, t.id)[0].at]);
      let peak = 0;
      for (const [start] of intervals) {
        const overlapping = intervals.filter(([s, e]) => s <= start && e > start).length;
        peak = Math.max(peak, overlapping);
      }
      check(peak <= 2, `7.1: ${peak} tasks were in flight at once with concurrency=2`);
      check(peak === 2, "7.1: with concurrency=2 and 5 queued tasks, two tasks must run in parallel");
      for (const p of engine.byKind("poll")) {
        check(p.body.limit <= 2 - p.inFlightAtPoll,
          `P11: polled limit=${p.body.limit} with ${p.inFlightAtPoll} in flight and concurrency=2`);
      }
    },
  },
  {
    name: "worker.graceful_shutdown",
    rules: "7.7",
    async run(h) {
      const engine = await h.engine({ leaseSecs: 4, heartbeatIntervalSecs: 1 });
      const task = engine.addTask(makeTask("slow", { params: { sleep_ms: 1800 } }));
      const w = await h.worker(engine);
      await engine.waitFor((e) => e.tasks.has(task.id), 10000, "claim of slow task");
      await new Promise((r) => setTimeout(r, 300));
      const stopAt = Date.now();
      await w.stop(12000);
      check(!w.killed, "7.7: worker did not exit within 12s of SIGTERM");
      check(w.exitCode === 0, `7.7: graceful shutdown must exit 0, got ${w.exitCode}`);
      const acks = acksFor(engine, task.id);
      check(acks.length === 1 && acks[0].kind === "complete", "7.7: in-flight task must be completed during graceful shutdown");
      check(acks[0].at <= w.exitedAt, "7.7: process exited before acknowledging its in-flight task");
      const late = engine.byKind("poll").filter((p) => p.at > stopAt + 250);
      check(late.length === 0, `7.7: ${late.length} poll(s) started after SIGTERM`);
    },
  },
  {
    name: "worker.queue_and_version",
    rules: "P3, P4",
    async run(h) {
      const engine = await h.engine();
      const task = engine.addTask(makeTask("echo", { params: { q: "gpu" } }), { queue: "gpu" });
      const w = await h.worker(engine, { ORCH8_QUEUE: "gpu", ORCH8_WORKER_VERSION: "2.3.4" });
      await engine.waitFor((e) => acksFor(e, task.id).length > 0, 15000, "completion of queued task");
      await w.stop();
      const polls = engine.byKind("poll");
      for (const p of polls) {
        check(p.path === "/workers/tasks/poll/queue", `P3: a queue-bound worker must poll /workers/tasks/poll/queue, got ${p.path}`);
        check(p.body.queue_name === "gpu", `P3: queue_name must be "gpu", got ${JSON.stringify(p.body.queue_name)}`);
        check(p.body.version === "2.3.4", `P4: version must be "2.3.4", got ${JSON.stringify(p.body.version)}`);
      }
      check(acksFor(engine, task.id)[0].kind === "complete", "queued echo task must complete");
    },
  },
];

// ---------------------------------------------------------------------------
// Push scenarios
// ---------------------------------------------------------------------------

function postRaw(port, headers, body) {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ host: "127.0.0.1", port, method: "POST", path: "/orch8/push", headers: {
      "content-type": "application/json",
      "content-length": Buffer.byteLength(body),
      ...headers,
    } }, (res) => {
      res.resume();
      res.on("end", () => resolve(res.statusCode));
    });
    req.on("error", reject);
    req.setTimeout(10000, () => req.destroy(new Error("push request timed out")));
    req.end(body);
  });
}

const pushScenarios = [
  {
    name: "push.signature_vectors",
    rules: "S1-S3",
    async run(h) {
      const vectors = JSON.parse(readFileSync(VECTORS_PATH, "utf8")).cases;
      const lines = vectors.map((v) => JSON.stringify({
        secret: v.secret, timestamp: v.timestamp, signature: v.signature,
        body: v.body, now: v.now, tolerance_secs: v.tolerance_secs,
      }));
      const out = await h.lines("verify", lines);
      check(out.length === vectors.length, `verify mode must print one line per input (${out.length}/${vectors.length})`);
      const wrong = [];
      vectors.forEach((v, i) => {
        if (out[i]?.valid !== v.valid) wrong.push(`${v.name} (want ${v.valid}, got ${JSON.stringify(out[i])})`);
      });
      check(wrong.length === 0, `S1-S3 vector mismatches:\n      - ${wrong.join("\n      - ")}`);
    },
  },
  {
    name: "push.receiver",
    rules: "S2-S4, D2",
    async run(h) {
      const engine = await h.engine();
      const secret = "whsec_receiver";
      const queued = engine.addTask(makeTask("echo", { params: { via: "push" } }), { queue: "push-q" });
      const receiver = await h.push(engine, secret);
      const envelope = JSON.stringify({
        task_id: queued.id, instance_id: queued.instance_id, block_id: queued.block_id,
        handler_name: "echo", queue_name: "push-q", params: { via: "push" }, context: {},
        attempt: 0, timeout_ms: null,
      });
      const now = Math.floor(Date.now() / 1000);
      const cases = [
        ["missing signature", { "x-orch8-timestamp": String(now) }, envelope],
        ["wrong secret", { "x-orch8-timestamp": String(now), "x-orch8-signature": signatureHeader("nope", now, envelope) }, envelope],
        ["stale timestamp", { "x-orch8-timestamp": String(now - 1000), "x-orch8-signature": signatureHeader(secret, now - 1000, envelope) }, envelope],
        ["tampered body", { "x-orch8-timestamp": String(now), "x-orch8-signature": signatureHeader(secret, now, envelope) }, envelope.replace("push", "pull")],
      ];
      for (const [label, headers, body] of cases) {
        const status = await postRaw(receiver.port, headers, body);
        check(status === 401, `S4: ${label} must be rejected with 401, got ${status}`);
      }
      await new Promise((r) => setTimeout(r, 300));
      check(engine.byKind("poll").length === 0, "S4: a rejected push must not trigger a claim");
      const ts = Math.floor(Date.now() / 1000);
      const status = await postRaw(receiver.port, {
        "x-orch8-timestamp": String(ts),
        "x-orch8-signature": signatureHeader(secret, ts, envelope),
      }, envelope);
      check(status >= 200 && status < 300, `D1: a valid push must be acknowledged with 2xx, got ${status}`);
      await engine.waitFor((e) => acksFor(e, queued.id).length > 0, 10000, "claim + complete after valid push");
      await receiver.stop();
      const polls = engine.byKind("poll");
      check(polls.length >= 1 && polls.every((p) => p.path === "/workers/tasks/poll/queue"),
        "D2: the receiver must claim via /workers/tasks/poll/queue");
      check(polls[0].body.queue_name === "push-q" && polls[0].body.handler_name === "echo",
        `D2: claim must use queue_name/handler_name from the envelope, got ${JSON.stringify(polls[0].body)}`);
      const acks = acksFor(engine, queued.id);
      check(acks[0].kind === "complete" && deepEqual(acks[0].body.output, { echo: { via: "push" } }), "D2: claimed task must be executed and completed");
      checkMutationClaim(engine, queued);
      checkAuth(engine);
    },
  },
];

// ---------------------------------------------------------------------------
// Client scenarios
// ---------------------------------------------------------------------------

const JOB = {
  id: "job_0001", instance_id: "00000000-0000-7000-9000-00000000a001", handler: "send_email",
  status: "scheduled", created_at: "2026-09-26T10:00:00Z", run_at: "2026-09-26T10:05:00Z",
};

function installClientRoutes(engine) {
  const seq = { id: "00000000-0000-7000-a000-000000000001", tenant_id: TENANT_ID, namespace: "default", name: "onboarding", version: 1, blocks: [], created_at: "2026-09-26T10:00:00Z" };
  const inst = { id: "00000000-0000-7000-b000-000000000001", sequence_id: seq.id, tenant_id: TENANT_ID, namespace: "default", state: "running", priority: "normal", timezone: "UTC", metadata: {}, context: { data: {} }, created_at: "2026-09-26T10:00:00Z", updated_at: "2026-09-26T10:00:00Z" };
  engine.route("POST", "/sequences", () => ({ status: 201, body: { id: seq.id } }));
  engine.route("GET", "/sequences/:id", (r) => (r.params.id === seq.id ? { status: 200, body: seq } : { status: 404, body: errorBody("not_found", `not found: sequence ${r.params.id}`) }));
  engine.route("GET", "/sequences", () => ({ status: 200, body: [seq] }));
  engine.route("POST", "/instances", (r) => ({ status: r.body?.idempotency_key === "dup" ? 200 : 201, body: r.body?.idempotency_key === "dup" ? { id: inst.id, deduplicated: true } : { id: inst.id } }));
  engine.route("GET", "/instances/:id", (r) => (r.params.id === inst.id ? { status: 200, body: inst } : { status: 404, body: errorBody("not_found", `not found: instance ${r.params.id}`) }));
  engine.route("GET", "/instances", () => ({ status: 200, body: [inst] }));
  engine.route("POST", "/instances/:id/signals", (r) => (r.params.id === inst.id
    ? { status: 201, body: { signal_id: "00000000-0000-7000-c000-000000000001" } }
    : { status: 404, body: errorBody("not_found", `not found: instance ${r.params.id}`) }));
  let flaky = 0;
  engine.route("POST", "/jobs", (r) => {
    if (r.body?.handler === "always_503") return { status: 503, body: errorBody("unavailable", "unavailable: pool exhausted") };
    if (r.body?.handler === "conflict") return { status: 409, body: errorBody("already_exists", "already exists: job") };
    if (r.body?.handler === "invalid") return { status: 400, body: errorBody("invalid_argument", "invalid argument: handler is required") };
    return { status: 201, body: { ...JOB, handler: r.body?.handler ?? JOB.handler } };
  });
  engine.route("GET", "/jobs/:id", (r) => {
    if (r.params.id === "job_flaky") {
      flaky += 1;
      if (flaky < 3) return { status: 503, body: errorBody("unavailable", "unavailable: pool exhausted") };
      return { status: 200, body: { ...JOB, id: "job_flaky" } };
    }
    if (r.params.id === "job_rate") return { status: 429, body: errorBody("rate_limited", "rate limit exceeded: quota") };
    if (r.params.id === JOB.id || r.params.id === "a/b c") return { status: 200, body: { ...JOB, id: r.params.id } };
    return { status: 404, body: errorBody("not_found", `not found: job ${r.params.id}`) };
  });
  engine.route("GET", "/jobs", () => ({ status: 200, body: [JOB] }));
  engine.route("DELETE", "/jobs/:id", (r) => (r.params.id === JOB.id
    ? { status: 200, body: { ...JOB, status: "cancelled" } }
    : { status: 404, body: errorBody("not_found", `not found: job ${r.params.id}`) }));
  engine.route("GET", "/unauthorized-probe", () => ({ status: 401, body: errorBody("unauthorized", "unauthorized") }));
  return { seq, inst };
}

async function runOps(h, engine, ops) {
  const lines = ops.map((op, i) => JSON.stringify({ id: i + 1, op: op.op, args: op.args ?? {} }));
  const out = await h.lines("client", lines);
  check(out.length === ops.length, `client mode must print one result line per op (${out.length}/${ops.length})`);
  out.forEach((o, i) => check(o.id === i + 1, `client result ${i} has id ${o?.id}, expected ${i + 1}`));
  return out;
}

function lastReq(engine, method, path) {
  const m = engine.find(method, path);
  check(m.length > 0, `expected a ${method} ${path} request`);
  return m[m.length - 1];
}

function checkNoNulls(body, label) {
  const nulls = Object.entries(body ?? {}).filter(([, v]) => v === null).map(([k]) => k);
  check(nulls.length === 0, `${label}: unset optional fields must be omitted, not sent as null (${nulls})`);
}

const clientScenarios = [
  {
    name: "client.sequences",
    rules: "T1-T4",
    async run(h) {
      const engine = await h.engine();
      const { seq } = installClientRoutes(engine);
      const body = { tenant_id: TENANT_ID, namespace: "default", name: "onboarding", blocks: [{ type: "step", id: "s1", handler: "echo", params: {} }] };
      const out = await runOps(h, engine, [
        { op: "sequences.create", args: { body } },
        { op: "sequences.get", args: { id: seq.id } },
        { op: "sequences.list", args: { query: { namespace: "default", limit: 10 } } },
      ]);
      out.forEach((o, i) => check(o.ok, `op ${i + 1} failed: ${JSON.stringify(o.error)}`));
      check(out[0].result?.id === seq.id, `sequences.create must return the server's id, got ${JSON.stringify(out[0].result)}`);
      const create = lastReq(engine, "POST", "/sequences");
      check(create.body?.name === "onboarding" && Array.isArray(create.body?.blocks), "sequences.create must send the definition body");
      check(out[1].result?.name === "onboarding", "sequences.get must return the sequence");
      check(Array.isArray(out[2].result) && out[2].result.length === 1, "sequences.list must return an array");
      const list = lastReq(engine, "GET", "/sequences");
      check(list.query.namespace === "default" && list.query.limit === "10", `sequences.list must pass query params, got ${list.rawUrl}`);
      checkAuth(engine);
    },
  },
  {
    name: "client.instances",
    rules: "T1-T4",
    async run(h) {
      const engine = await h.engine();
      const { seq, inst } = installClientRoutes(engine);
      const out = await runOps(h, engine, [
        { op: "instances.create", args: { body: { sequence_id: seq.id, tenant_id: TENANT_ID, namespace: "default", context: { data: { user: "u1" } } } } },
        { op: "instances.create", args: { body: { sequence_id: seq.id, tenant_id: TENANT_ID, namespace: "default", idempotency_key: "dup" } } },
        { op: "instances.get", args: { id: inst.id } },
        { op: "instances.list", args: { query: { state: "running", limit: 5 } } },
        { op: "instances.signal", args: { id: inst.id, signal_type: { custom: "approve" }, payload: { by: "alice" } } },
        { op: "instances.cancel", args: { id: inst.id } },
      ]);
      out.forEach((o, i) => check(o.ok, `op ${i + 1} failed: ${JSON.stringify(o.error)}`));
      check(out[0].result?.id === inst.id, "instances.create must return the created id");
      check(out[1].result?.id === inst.id && out[1].result?.deduplicated === true, "instances.create must surface deduplicated=true on an idempotent replay");
      check(out[2].result?.state === "running", "instances.get must return the instance");
      const list = lastReq(engine, "GET", "/instances");
      check(list.query.state === "running" && list.query.limit === "5", `instances.list must pass query params, got ${list.rawUrl}`);
      const signals = engine.find("POST", `/instances/${inst.id}/signals`);
      check(signals.length === 2, `expected 2 signal requests, got ${signals.length}`);
      check(deepEqual(signals[0].body, { signal_type: { custom: "approve" }, payload: { by: "alice" } }), `instances.signal body mismatch: ${signals[0].rawBody}`);
      check(signals[1].body?.signal_type === "cancel", `instances.cancel must send signal_type "cancel", got ${signals[1].rawBody}`);
      checkAuth(engine);
    },
  },
  {
    name: "client.jobs",
    rules: "jobs contract",
    async run(h) {
      const engine = await h.engine();
      installClientRoutes(engine);
      const full = {
        handler: "send_email", payload: { to: "a@example.com" }, queue: "emails", priority: 5,
        retry: { max_attempts: 4, initial_backoff_ms: 500, max_backoff_ms: 10000 },
        delay_ms: 250, idempotency_key: "welcome-a", metadata: { source: "signup" },
      };
      const out = await runOps(h, engine, [
        { op: "jobs.enqueue", args: { body: { handler: "send_email", payload: { to: "b@example.com" } } } },
        { op: "jobs.enqueue", args: { body: full } },
        { op: "jobs.enqueue", args: { body: { handler: "send_email", payload: {}, run_at: "2026-10-01T00:00:00Z" } } },
        { op: "jobs.get", args: { id: JOB.id } },
        { op: "jobs.get", args: { id: "a/b c" } },
        { op: "jobs.list", args: { query: { status: "scheduled", limit: 20 } } },
        { op: "jobs.cancel", args: { id: JOB.id } },
      ]);
      out.forEach((o, i) => check(o.ok, `op ${i + 1} failed: ${JSON.stringify(o.error)}`));
      const posts = engine.find("POST", "/jobs");
      check(posts.length === 3, `expected 3 POST /jobs, got ${posts.length}`);
      check(deepEqual(posts[0].body, { handler: "send_email", payload: { to: "b@example.com" } }),
        `jobs.enqueue minimal body must contain only handler + payload, got ${posts[0].rawBody}`);
      check(deepEqual(posts[1].body, full), `jobs.enqueue full body mismatch: ${posts[1].rawBody}`);
      check(posts[2].body?.run_at === "2026-10-01T00:00:00Z", `jobs.enqueue must pass run_at, got ${posts[2].rawBody}`);
      posts.forEach((p, i) => checkNoNulls(p.body, `jobs.enqueue #${i + 1}`));
      for (const k of ["id", "instance_id", "handler", "status", "created_at", "run_at"]) {
        check(out[1].result?.[k] !== undefined, `jobs.enqueue result must expose ${k}`);
      }
      check(out[3].result?.id === JOB.id && out[3].result?.status === "scheduled", "jobs.get must return the job");
      const enc = engine.requests.find((r) => r.method === "GET" && r.path.startsWith("/jobs/a"));
      check(enc && (enc.rawPath === "/api/v1/jobs/a%2Fb%20c" || enc.rawPath === "/api/v1/jobs/a%2Fb+c"),
        `path ids must be percent-encoded as one segment, got ${enc?.rawPath}`);
      check(Array.isArray(out[5].result) && out[5].result.length === 1, "jobs.list must return an array of jobs");
      const list = lastReq(engine, "GET", "/jobs");
      check(list.query.status === "scheduled" && list.query.limit === "20", `jobs.list must pass query params, got ${list.rawUrl}`);
      check(engine.find("DELETE", `/jobs/${JOB.id}`).length === 1, "jobs.cancel must send DELETE /jobs/{id}");
      checkAuth(engine);
    },
  },
  {
    name: "client.errors_and_retries",
    rules: "T7, 1.1, fixtures/transport.json",
    async run(h) {
      const engine = await h.engine();
      installClientRoutes(engine);
      const out = await runOps(h, engine, [
        { op: "jobs.get", args: { id: "missing" } },
        { op: "jobs.enqueue", args: { body: { handler: "conflict", payload: {} } } },
        { op: "jobs.enqueue", args: { body: { handler: "invalid", payload: {} } } },
        { op: "jobs.get", args: { id: "job_flaky" } },
        { op: "jobs.enqueue", args: { body: { handler: "always_503", payload: {} } } },
        { op: "jobs.get", args: { id: "job_rate" } },
      ]);
      const want = [
        ["not_found", 404, "not_found"],
        ["conflict", 409, "already_exists"],
        ["invalid_argument", 400, "invalid_argument"],
        null,
        ["server", 503, "unavailable"],
        ["rate_limited", 429, "rate_limited"],
      ];
      want.forEach((w, i) => {
        if (!w) return;
        const o = out[i];
        check(o.ok === false, `op ${i + 1} must fail`);
        check(o.error?.kind === w[0], `op ${i + 1}: error kind must be ${w[0]}, got ${JSON.stringify(o.error)}`);
        check(o.error?.status === w[1], `op ${i + 1}: status must be ${w[1]}, got ${o.error?.status}`);
        check(o.error?.code === w[2], `T7: op ${i + 1}: error code must be parsed from the envelope (${w[2]}), got ${o.error?.code}`);
        check(typeof o.error?.message === "string" && o.error.message.length > 0, `op ${i + 1}: error message missing`);
      });
      check(out[3].ok && out[3].result?.id === "job_flaky", `safe GET must recover after transient 503s: ${JSON.stringify(out[3])}`);
      check(engine.find("GET", "/jobs/job_flaky").length === 3, `safe GET must be retried (3 attempts), got ${engine.find("GET", "/jobs/job_flaky").length}`);
      const unsafe = engine.find("POST", "/jobs").filter((r) => r.body?.handler === "always_503");
      check(unsafe.length === 1, `unsafe POST must never be replayed, got ${unsafe.length} attempts`);
      checkAuth(engine);
    },
  },
];

export const scenarios = [...workerScenarios, ...pushScenarios, ...clientScenarios];
export { AssertionError };
