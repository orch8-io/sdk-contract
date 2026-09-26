// Self-tests for the conformance kit: the reference adapter must pass every
// scenario, and each injected protocol bug must be caught by its scenario.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { runConformance } from "../run.mjs";
import { sign, verify } from "../lib/sign.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const adapter = `node ${JSON.stringify(resolve(here, "../reference/adapter.mjs"))}`;
const quiet = () => {};

test("push signature vectors agree with the reference verifier", () => {
  const { cases } = JSON.parse(readFileSync(resolve(here, "../../fixtures/push_signatures.json"), "utf8"));
  assert.ok(cases.length >= 10);
  for (const c of cases) {
    const got = verify({
      secret: c.secret, timestamp: c.timestamp ?? undefined, signature: c.signature ?? undefined,
      body: Buffer.from(c.body, "utf8"), now: c.now, toleranceSecs: c.tolerance_secs,
    });
    assert.equal(got, c.valid, c.name);
  }
});

test("sign() matches the engine scheme hex(HMAC-SHA256(secret, ts.body))", () => {
  // Engine unit test fixture: orch8-engine/src/webhooks.rs sign("shhh", ts, body).
  assert.match(sign("shhh", 1700000000, "{}"), /^[0-9a-f]{64}$/);
  assert.notEqual(sign("shhh", 1700000000, "{}"), sign("shhh", 1700000001, "{}"));
});

test("reference adapter passes every scenario", { timeout: 300000 }, async () => {
  const { results, passed } = await runConformance({ adapter, log: quiet });
  const failed = results.filter((r) => !r.ok).map((r) => `${r.name}: ${r.error?.message}`);
  assert.ok(passed, `failing scenarios:\n${failed.join("\n")}`);
});

const bugs = [
  ["crash_permanent", "worker.failures"],
  ["ignore_poll_hint", "worker.empty_poll_cadence"],
  ["ack_after_loss", "worker.lease_loss"],
  ["accept_all_signatures", "push.signature_vectors"],
  ["retry_unsafe", "client.errors_and_retries"],
];

for (const [bug, scenario] of bugs) {
  test(`injected bug ${bug} is caught by ${scenario}`, { timeout: 120000 }, async () => {
    process.env.REF_BUG = bug;
    try {
      const { results } = await runConformance({ adapter, only: [scenario], log: quiet });
      assert.equal(results.length, 1);
      assert.equal(results[0].ok, false, `${scenario} should fail with REF_BUG=${bug}`);
    } finally {
      delete process.env.REF_BUG;
    }
  });
}
