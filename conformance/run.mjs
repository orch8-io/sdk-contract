#!/usr/bin/env node
// Orch8 worker/client conformance runner.
//
//   node conformance/run.mjs --adapter "<command>" [--only a,b] [--skip a,b] [--list]
//                            [--bind-host 127.0.0.1] [--advertise-host 127.0.0.1] [--verbose]
//
// The adapter command is run through the shell as `<command> <mode>` where
// mode is one of: worker | push | verify | client. See conformance/README.md.

import { spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import { createServer } from "node:net";
import { createInterface } from "node:readline";

import { API_KEY, FakeEngine, TENANT_ID } from "./lib/fake-engine.mjs";
import { AssertionError, scenarios } from "./lib/scenarios.mjs";

export async function runConformance(options) {
  const {
    adapter,
    only = [],
    skip = [],
    bindHost = "127.0.0.1",
    advertiseHost = bindHost === "0.0.0.0" ? "127.0.0.1" : bindHost,
    verbose = false,
    log = (line) => process.stdout.write(`${line}\n`),
  } = options;
  const selected = scenarios.filter((s) =>
    (only.length === 0 || only.some((o) => s.name === o || s.name.startsWith(`${o}.`) || s.name.startsWith(o)))
    && !skip.some((o) => s.name === o || s.name.startsWith(o)));
  const results = [];
  for (const scenario of selected) {
    const started = Date.now();
    const harness = new Harness({ adapter, bindHost, advertiseHost, verbose });
    let error = null;
    try {
      await withTimeout(scenario.run(harness), 90000, `${scenario.name} exceeded 90s`);
    } catch (e) {
      error = e;
    } finally {
      await harness.cleanup();
    }
    const ms = Date.now() - started;
    results.push({ name: scenario.name, ok: !error, error, ms });
    if (!error) {
      log(`  ok    ${scenario.name}  (${ms}ms)`);
    } else {
      log(`  FAIL  ${scenario.name}  [${scenario.rules}]`);
      log(`        ${error instanceof AssertionError ? error.message : error.stack ?? error}`);
      const tail = harness.output.slice(-40).join("\n        | ");
      if (tail) log(`        adapter output (tail):\n        | ${tail}`);
    }
  }
  const failed = results.filter((r) => !r.ok);
  log(`\n${results.length - failed.length}/${results.length} scenarios passed`);
  return { results, passed: failed.length === 0 };
}

function withTimeout(promise, ms, message) {
  let timer;
  return Promise.race([
    promise.finally(() => clearTimeout(timer)),
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(message)), ms); timer.unref(); }),
  ]);
}

function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.on("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

class Harness {
  constructor({ adapter, bindHost, advertiseHost, verbose }) {
    this.adapter = adapter;
    this.bindHost = bindHost;
    this.advertiseHost = advertiseHost;
    this.verbose = verbose;
    this.workerId = `conformance-${randomBytes(4).toString("hex")}`;
    this.engines = [];
    this.procs = [];
    this.output = [];
  }

  async engine(opts = {}) {
    const engine = new FakeEngine({ ...opts, bindHost: this.bindHost });
    await engine.start();
    this.engines.push(engine);
    return engine;
  }

  env(engine, extra = {}) {
    return {
      ...process.env,
      ORCH8_BASE_URL: engine ? `http://${this.advertiseHost}:${engine.port}/api/v1` : "",
      ORCH8_API_KEY: API_KEY,
      ORCH8_TENANT_ID: TENANT_ID,
      ORCH8_WORKER_ID: this.workerId,
      ORCH8_CONCURRENCY: "4",
      ORCH8_POLL_INTERVAL_MS: "100",
      ORCH8_SHUTDOWN_TIMEOUT_MS: "10000",
      ORCH8_MAX_ATTEMPTS: "3",
      ORCH8_RETRY_BASE_DELAY_MS: "20",
      ORCH8_PUSH_TOLERANCE_SECS: "300",
      ...extra,
    };
  }

  spawn(mode, env) {
    // `exec` makes the adapter replace the shell, so signals and the exit
    // status are the adapter's own. dash (Ubuntu's /bin/sh) does not exec a
    // trailing simple command by itself, unlike bash and macOS sh.
    const command = process.platform === "win32"
      ? `${this.adapter} ${mode}`
      : `exec ${this.adapter} ${mode}`;
    const child = spawn(command, {
      shell: true,
      env,
      detached: process.platform !== "win32",
      stdio: ["pipe", "pipe", "pipe"],
    });
    const proc = { child, exitCode: null, exitedAt: null, killed: false, stdoutLines: [] };
    proc.exited = new Promise((resolve) => {
      child.on("exit", (code, signal) => {
        proc.exitCode = code ?? (signal ? `signal ${signal}` : null);
        proc.exitedAt = Date.now();
        resolve();
      });
    });
    const tag = (stream, name) => {
      createInterface({ input: stream }).on("line", (line) => {
        this.output.push(`${name}: ${line}`);
        if (this.output.length > 400) this.output.shift();
        if (this.verbose) process.stderr.write(`    [${mode} ${name}] ${line}\n`);
        if (name === "out") proc.stdoutLines.push(line);
      });
    };
    tag(child.stdout, "out");
    tag(child.stderr, "err");
    child.stdin.on("error", () => {});
    this.procs.push(proc);
    return proc;
  }

  signal(proc, sig) {
    if (proc.exitedAt) return;
    try {
      if (process.platform !== "win32") process.kill(-proc.child.pid, sig);
      else proc.child.kill(sig);
    } catch {
      try { proc.child.kill(sig); } catch { /* already gone */ }
    }
  }

  async stopProc(proc, timeoutMs) {
    if (proc.exitedAt) return;
    this.signal(proc, "SIGTERM");
    const timedOut = await Promise.race([
      proc.exited.then(() => false),
      new Promise((r) => setTimeout(() => r(true), timeoutMs).unref()),
    ]);
    if (timedOut) {
      proc.killed = true;
      this.signal(proc, "SIGKILL");
      await proc.exited;
    }
  }

  /** Start `<adapter> worker`; resolves after the first poll reaches the engine. */
  async worker(engine, extraEnv = {}) {
    const proc = this.spawn("worker", this.env(engine, extraEnv));
    proc.child.stdin.end();
    const firstPoll = engine.waitFor((e) => e.byKind("poll").length > 0, 60000, "the worker's first poll");
    await Promise.race([
      firstPoll,
      proc.exited.then(() => { throw new Error(`worker exited before polling (code ${proc.exitCode})`); }),
    ]);
    const handle = {
      get exitCode() { return proc.exitCode; },
      get exitedAt() { return proc.exitedAt; },
      get killed() { return proc.killed; },
      stop: async (timeoutMs = 12000) => { await this.stopProc(proc, timeoutMs); },
    };
    return handle;
  }

  /** Start `<adapter> push`; resolves once it prints READY. */
  async push(engine, secret) {
    const port = await freePort();
    const proc = this.spawn("push", this.env(engine, {
      ORCH8_PUSH_PORT: String(port),
      ORCH8_PUSH_SECRET: secret,
    }));
    proc.child.stdin.end();
    const started = Date.now();
    while (!proc.stdoutLines.some((l) => l.trim() === "READY")) {
      if (proc.exitedAt) throw new Error(`push receiver exited before READY (code ${proc.exitCode})`);
      if (Date.now() - started > 60000) throw new Error("push receiver did not print READY within 60s");
      await new Promise((r) => setTimeout(r, 25));
    }
    return { port, stop: () => this.stopProc(proc, 12000) };
  }

  /** Run `<adapter> <mode>` feeding JSON lines on stdin; returns parsed JSON stdout lines. */
  async lines(mode, inputLines) {
    const engine = this.engines[this.engines.length - 1];
    const proc = this.spawn(mode, this.env(engine));
    proc.child.stdin.end(`${inputLines.join("\n")}\n`);
    const done = await Promise.race([
      proc.exited.then(() => true),
      new Promise((r) => setTimeout(() => r(false), 120000).unref()),
    ]);
    if (!done) {
      await this.stopProc(proc, 2000);
      throw new Error(`${mode} mode did not exit within 120s`);
    }
    await new Promise((r) => setImmediate(r));
    if (proc.exitCode !== 0) throw new Error(`${mode} mode exited with ${proc.exitCode}`);
    return proc.stdoutLines
      .map((l) => l.trim())
      .filter((l) => l.startsWith("{"))
      .map((l) => JSON.parse(l));
  }

  async cleanup() {
    for (const proc of this.procs) {
      if (!proc.exitedAt) await this.stopProc(proc, 3000);
    }
    for (const engine of this.engines) await engine.stop();
  }
}

function parseArgs(argv) {
  const out = { only: [], skip: [] };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === "--adapter") out.adapter = next();
    else if (a === "--only") out.only = next().split(",").filter(Boolean);
    else if (a === "--skip") out.skip = next().split(",").filter(Boolean);
    else if (a === "--bind-host") out.bindHost = next();
    else if (a === "--advertise-host") out.advertiseHost = next();
    else if (a === "--verbose") out.verbose = true;
    else if (a === "--list") out.list = true;
    else if (a === "--help" || a === "-h") out.help = true;
    else throw new Error(`unknown argument ${a}`);
  }
  return out;
}

const isMain = import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("/run.mjs");
if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  if (args.list) {
    for (const s of scenarios) console.log(`${s.name.padEnd(28)} ${s.rules}`);
    process.exit(0);
  }
  if (args.help || !args.adapter) {
    console.log("usage: node conformance/run.mjs --adapter \"<command>\" [--only a,b] [--skip a,b] [--bind-host h] [--advertise-host h] [--verbose] [--list]");
    process.exit(args.help ? 0 : 2);
  }
  console.log(`Orch8 conformance kit — adapter: ${args.adapter}`);
  const { passed } = await runConformance(args);
  process.exit(passed ? 0 : 1);
}
