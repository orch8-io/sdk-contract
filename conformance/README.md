# Orch8 SDK conformance kit

A language-neutral test runner that exercises an SDK's **worker**, **push
receiver**, **signature verifier** and **client** against a scripted fake
engine. The rules it checks are defined in
[`../WORKER_PROTOCOL.md`](../WORKER_PROTOCOL.md); every failure message names
the rule id (`P8`, `L2`, `K3`, ...).

Requirements: Node.js ≥ 20. No npm dependencies.

```bash
# from sdk-contract/
node conformance/run.mjs --list                                   # list scenarios
node conformance/run.mjs --adapter "node conformance/reference/adapter.mjs"
node conformance/run.mjs --adapter "../sdk-ruby/bin/conformance" --only worker
npm test                                                          # kit self-tests
```

Exit status is 0 only when every selected scenario passes. `--verbose`
streams the adapter's stdout/stderr; on failure the last 40 lines are always
printed.

## How it works

For each scenario the runner starts a fresh fake engine on a random port
(`127.0.0.1` by default), launches your adapter, drives it, and asserts on the
recorded HTTP traffic. The fake engine requires `x-api-key` and `x-tenant-id`
exactly as provided, serves everything under `/api/v1`, implements
poll/heartbeat/checkpoint/complete/fail with real claim-epoch and
checkpoint-sequence CAS semantics, and injects faults per scenario (409/404 on
heartbeat, 503 on poll and complete, 409 on complete, ...).

## The adapter contract

Your SDK ships a tiny program (the *adapter*) — a CLI that the runner invokes
through the shell as:

```
<adapter command> <mode>        mode ∈ worker | push | verify | client
```

The adapter MUST `exec` the real program (or be the real program) so that it
receives `SIGTERM` directly; the runner signals the whole process group.
Anything printed to stderr is only used for diagnostics.

### Environment (all modes)

| Variable | Meaning |
|---|---|
| `ORCH8_BASE_URL` | Engine base URL including `/api/v1` (e.g. `http://127.0.0.1:53211/api/v1`). |
| `ORCH8_API_KEY` | Send as `x-api-key`. |
| `ORCH8_TENANT_ID` | Send as `x-tenant-id`. |
| `ORCH8_WORKER_ID` | Use as the worker id. |
| `ORCH8_CONCURRENCY` | Max concurrent tasks (default `4`). |
| `ORCH8_POLL_INTERVAL_MS` | Base poll interval (default `100`). The server's `poll_after_ms` must still be honoured. |
| `ORCH8_SHUTDOWN_TIMEOUT_MS` | Drain timeout for graceful shutdown (`10000`). |
| `ORCH8_QUEUE` | Optional named queue; when set, poll `/workers/tasks/poll/queue`. |
| `ORCH8_WORKER_VERSION` | Optional worker version to send on polls. |
| `ORCH8_MAX_ATTEMPTS` | Client: max attempts for safe (GET/HEAD) requests (`3`). |
| `ORCH8_RETRY_BASE_DELAY_MS` | Client: base backoff between safe retries (`20`). |
| `ORCH8_PUSH_PORT` | Push mode: port to listen on (all interfaces). |
| `ORCH8_PUSH_SECRET` | Push mode: HMAC secret. |
| `ORCH8_PUSH_TOLERANCE_SECS` | Push mode: timestamp tolerance (`300`). |

Do not hard-code heartbeat intervals: leave the SDK default and let it honour
the server's `heartbeat_interval_secs` hint (scenarios advertise `1`).

### Standard handlers (worker and push modes)

Register exactly these handlers using the SDK's public worker API:

| Handler | Behaviour |
|---|---|
| `echo` | Return `{"echo": <task.params>}`. |
| `fail_retryable` | Raise the SDK's *retryable* error with message `params.message` (default `"boom"`). |
| `fail_permanent` | Raise the SDK's *non-retryable* error with message `"fatal"`. |
| `crash` | Raise a plain runtime exception (not an SDK error type) with message `"crash"`. |
| `checkpoint` | `start = task.resume_checkpoint.step` (0 when absent); for `i` in `start+1 ..= params.steps` (default 3) persist checkpoint `{"step": i}` via the SDK's checkpoint API; return `{"resumed_from": start, "final_step": steps}`. |
| `slow` | Sleep `params.sleep_ms` (cancellable by the SDK's cancellation signal), return `{"slept": sleep_ms}`. |

### `worker` mode

Build a worker from the environment, register the standard handlers, start it,
and block. On `SIGTERM`: stop polling, finish and acknowledge in-flight tasks
(up to `ORCH8_SHUTDOWN_TIMEOUT_MS`), then exit with status **0**. Nothing needs
to be printed. stdin is closed immediately and must be ignored.

### `push` mode

Listen on `0.0.0.0:$ORCH8_PUSH_PORT` for `POST` requests (any path). Verify
`X-Orch8-Timestamp` / `X-Orch8-Signature` against the **raw body** with the
SDK's push verifier. Invalid → respond `401` and do nothing. Valid → respond
`2xx` (preferably `202`) and then, asynchronously, claim with one
`POST /workers/tasks/poll/queue` using `queue_name`/`handler_name` from the
envelope (`limit` 1) and run the claimed task through the standard handlers
(heartbeat, complete/fail as usual). Print the line `READY` on stdout once
listening. Exit 0 on `SIGTERM`.

### `verify` mode

Read JSON lines from stdin, one per vector, and write one JSON line per input:

```
in : {"secret": "...", "timestamp": "1767225600" | null, "signature": "sha256=..." | null,
      "body": "<exact body as a string>", "now": 1767225600, "tolerance_secs": 300}
out: {"valid": true}
```

`null` means the header is absent. `now` must be injected into the verifier's
clock. The body must be verified as its UTF-8 bytes. Vectors live in
[`../fixtures/push_signatures.json`](../fixtures/push_signatures.json).

### `client` mode

Read JSON lines `{"id": n, "op": "<op>", "args": {...}}` from stdin; for each,
call the SDK's typed client and print one JSON line:

```
{"id": n, "ok": true,  "result": <response as JSON, or null>}
{"id": n, "ok": false, "error": {"kind": "...", "status": 404, "code": "not_found", "message": "..."}}
```

Configure the client from the environment (base URL, key, tenant,
`ORCH8_MAX_ATTEMPTS`, `ORCH8_RETRY_BASE_DELAY_MS`). Exit 0 at EOF.

| op | args | HTTP |
|---|---|---|
| `sequences.create` | `body` (sequence definition JSON) | `POST /sequences` |
| `sequences.get` | `id` | `GET /sequences/{id}` |
| `sequences.list` | `query` (map) | `GET /sequences?...` |
| `instances.create` | `body` (CreateInstanceRequest JSON) | `POST /instances` → `{id, deduplicated?}` |
| `instances.get` | `id` | `GET /instances/{id}` |
| `instances.list` | `query` | `GET /instances?...` |
| `instances.signal` | `id`, `signal_type` (string or `{"custom": name}`), `payload?` | `POST /instances/{id}/signals` |
| `instances.cancel` | `id` | `POST /instances/{id}/signals {"signal_type":"cancel"}` |
| `jobs.enqueue` | `body` `{handler, payload, queue?, priority?, retry?, delay_ms?, run_at?, idempotency_key?, metadata?}` | `POST /jobs` |
| `jobs.get` | `id` | `GET /jobs/{id}` |
| `jobs.list` | `query` | `GET /jobs?...` |
| `jobs.cancel` | `id` | `DELETE /jobs/{id}` |

Error `kind` is derived from the SDK's **typed error**:
`400 invalid_argument`, `401 unauthorized`, `403 forbidden`, `404 not_found`,
`409 conflict`, `413 payload_too_large`, `422 unprocessable`,
`429 rate_limited`, `5xx server`, connection failure `transport`, anything
else `api`. `code` and `message` come from the engine's error envelope
(`{"error": {"code", "message"}}`). Path ids must be percent-encoded as a
single segment; unset optional request fields must be omitted, not `null`.
Safe requests (GET/HEAD) are retried on `408/425/429/5xx`/transport errors
up to `ORCH8_MAX_ATTEMPTS`; unsafe requests are never replayed
(`fixtures/transport.json`).

## Scenarios

| Scenario | Checks |
|---|---|
| `worker.poll_complete` | auth/tenant headers, `/api/v1`, poll body, `limit ≤ concurrency`, every handler polled, `claim_epoch` echo, complete output, exit 0 on SIGTERM |
| `worker.empty_poll_cadence` | waits ≥ `poll_after_ms` after an empty poll; keeps polling while idle |
| `worker.failures` | retryable / non-retryable / generic exception → `retryable` true / false / true, explicit flag |
| `worker.checkpoint_resume` | resume from `resume_checkpoint`, CAS `checkpoint_seq` from poll then from each response |
| `worker.heartbeat_cadence` | heartbeats at the server hint (1 s) for a 3.2 s task |
| `worker.lease_loss` | after 409 / 404 on heartbeat: no more heartbeats, no complete/fail, worker keeps working |
| `worker.ack_retry` | complete retried with identical body after 503; not retried (and not turned into fail) after 409 |
| `worker.poll_errors` | survives 503 polls with exponential backoff |
| `worker.concurrency` | never more than `ORCH8_CONCURRENCY` in flight; `limit` never exceeds free slots, even across handler loops |
| `worker.graceful_shutdown` | SIGTERM: no new polls, in-flight task completed before exit, exit 0 |
| `worker.queue_and_version` | queue poll endpoint, `queue_name`, `version` |
| `push.signature_vectors` | verifier against the shared vectors (tolerance, tampering, prefix, encoding) |
| `push.receiver` | 401 on missing/wrong/stale/tampered signatures without claiming; valid push → queue claim → complete |
| `client.sequences` / `client.instances` / `client.jobs` | request shapes, query strings, encoding, response mapping |
| `client.errors_and_retries` | typed errors from the envelope; GET retried, POST not |

## Running an adapter that lives in Docker

When the SDK cannot run on the host, bind the fake engine to all interfaces
and advertise a host name the container can reach; publish the push port:

```bash
node conformance/run.mjs --bind-host 0.0.0.0 --advertise-host host.docker.internal \
  --adapter 'docker run --rm -i --add-host=host.docker.internal:host-gateway \
     -p $ORCH8_PUSH_PORT:$ORCH8_PUSH_PORT \
     -e ORCH8_BASE_URL -e ORCH8_API_KEY -e ORCH8_TENANT_ID -e ORCH8_WORKER_ID \
     -e ORCH8_CONCURRENCY -e ORCH8_POLL_INTERVAL_MS -e ORCH8_SHUTDOWN_TIMEOUT_MS \
     -e ORCH8_QUEUE -e ORCH8_WORKER_VERSION -e ORCH8_MAX_ATTEMPTS -e ORCH8_RETRY_BASE_DELAY_MS \
     -e ORCH8_PUSH_PORT -e ORCH8_PUSH_SECRET -e ORCH8_PUSH_TOLERANCE_SECS \
     -v "$PWD/../sdk-php:/app" -w /app my-php-image php conformance/adapter.php'
```

(`$ORCH8_PUSH_PORT` is empty outside push mode; use a wrapper script that adds
`-p` only when it is set.)

## Writing a new adapter

1. Start from [`reference/adapter.mjs`](reference/adapter.mjs) — it is a
   complete, dependency-free protocol implementation and shows every mode.
2. Implement the modes on top of your SDK's **public** API only; the adapter
   exists to prove the SDK, not to re-implement it.
3. Run `node conformance/run.mjs --adapter "<cmd>" --verbose --only worker.poll_complete`
   and widen from there.

The kit's own tests (`npm test`) run the reference adapter and additionally
inject known protocol bugs (`REF_BUG=...`) to prove each scenario detects the
violation it is named for.
