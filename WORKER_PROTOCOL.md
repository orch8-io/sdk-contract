# Orch8 Worker Wire Protocol

Status: normative for every Orch8 worker SDK. Contract version 1.

This document specifies the HTTP protocol between the Orch8 engine and an
external worker. It is written from the engine source, not from any one SDK;
where an existing SDK differs, this document wins. Every rule cites the file
that enforces it (paths are relative to the `engine/` repository). The
conformance kit in [`conformance/`](conformance/README.md) checks these rules
against a scripted fake engine.

Key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119.

---

## 1. Transport basics

| Rule | Source |
|---|---|
| **T1** The canonical base URL is `<origin>/api/v1`. The unversioned root is a deprecated alias. SDKs MUST default to (or document) the `/api/v1` base. | `orch8-api/src/openapi.rs` (`servers`), `sdk-contract/openapi.json` |
| **T2** Request and response bodies are JSON (`Content-Type: application/json`). The artifact upload endpoint is the only exception (`application/octet-stream`). | `orch8-api/src/workers.rs` (`upload_task_artifact`) |
| **T3** Authentication: every request carries `x-api-key: <secret>` when the server runs with an API key. With no root key configured (`--insecure`) the header is ignored. | `orch8-api/src/auth.rs` (`api_key_middleware`) |
| **T4** Tenancy: `x-tenant-id: <tenant>` scopes the request. With a per-tenant key the tenant is taken from the key; a header that disagrees returns `403 forbidden`. With the root key, or when `require_tenant` is on, a missing header returns `400 invalid_argument`; an empty header returns `400`. SDKs SHOULD always send the configured tenant. | `orch8-api/src/auth.rs` (`api_key_middleware`, `tenant_middleware`) |
| **T5** Capability scoping: a per-tenant key with the `worker` capability may call only `/workers*` and `/handlers`. A worker SDK MUST NOT need any other route for the worker loop. | `orch8-api/src/auth.rs` (`capabilities_allow`) |
| **T6** Correlation: a client MAY send `x-request-id` (ASCII alnum, `-`, `_`, max 128 chars; other chars are stripped). The server echoes it (or a generated UUID) in the response `x-request-id` header. | `orch8-api/src/request_id.rs` |
| **T7** Errors use one envelope for every 4xx/5xx the API produces: `{"error": {"code": "<code>", "message": "<text>", "request_id": null, "details": <optional>}}`. 500 bodies never contain internal detail (`"internal server error"`). | `orch8-api/src/error.rs` (`ErrorEnvelope`, `IntoResponse`) |

### 1.1 Status → error code map

| HTTP | `error.code` | Meaning for a worker |
|---|---|---|
| 400 | `invalid_argument` | Malformed request (e.g. `checkpoint` without `checkpoint_seq`). Do not retry. |
| 401 | `unauthorized` | Missing / invalid / revoked / expired key. Do not retry. |
| 403 | `forbidden` | Tenant mismatch or missing capability. Do not retry. |
| 404 | `not_found` | Task (or its instance) does not exist or is not visible to this tenant. For an owned task this means the lease is gone (§5). |
| 409 | `conflict` / `already_exists` | Lease changed, stale `claim_epoch`, stale `checkpoint_seq`, or effect-guard conflict. Lease loss (§5). |
| 413 | `payload_too_large` | Checkpoint > 256 KiB, artifact > 10 MiB, or merged context over `max_context_bytes`. Do not retry the same payload. |
| 422 | `unprocessable_entity` | Semantic validation failure. |
| 429 | `rate_limited` | Quota exceeded. Retry with backoff. |
| 500 | `internal` | Retry with backoff. |
| 502 | `bad_gateway` | Retry with backoff. |
| 503 | `unavailable` | Storage unavailable / shutting down. Retry with backoff. |

Source: `orch8-api/src/error.rs` (`ApiError::code`, `from_storage`). The
transport retry policy in `fixtures/transport.json` treats
`408, 425, 429, 500, 502, 503, 504` and connection errors as retryable.

---

## 2. Task model

A worker task (`WorkerTask`, `orch8-types/src/worker.rs`) as returned by a poll:

| Field | Type | Notes |
|---|---|---|
| `id` | UUID | Task id. Used in every mutation path. |
| `instance_id` | UUID | Owning workflow instance. |
| `block_id` | string | Step id inside the sequence. |
| `handler_name` | string | Handler the step names. |
| `queue_name` | string, optional | Absent for the default queue. |
| `requirements` | object, optional | Capability requirements (absent when empty). |
| `params` | JSON | Step parameters (templates already resolved). |
| `context` | JSON | Serialized execution context (`{data, config, ...}`). |
| `attempt` | integer ≥ 0 | 0 on first dispatch; incremented on engine-driven retry. |
| `timeout_ms` | integer or null | Deadline measured from `created_at` (§6.3). |
| `state` | `"claimed"` | Always `claimed` in a poll response. |
| `worker_id` | string | Echo of the claimer. |
| `claimed_at`, `heartbeat_at` | RFC 3339 | |
| `claim_epoch` | integer ≥ 1 | Ownership generation (§3.3). |
| `resume_checkpoint` | JSON, optional | Last durable checkpoint (§4.2). Absent when none. |
| `checkpoint_seq` | integer ≥ 0 | CAS version of the checkpoint (§4.2). Defaults to 0. |
| `completed_at`, `output`, `error_message`, `error_retryable` | null | Unset on a claimed task. |
| `created_at` | RFC 3339 | |

Unknown fields MUST be ignored by SDKs (the engine adds fields over time).

---

## 3. Poll / claim

### 3.1 Requests

```
POST /workers/tasks/poll
{"handler_name": "send_email", "worker_id": "host-1234", "limit": 4,
 "version": "1.4.2", "capabilities": {...}}

POST /workers/tasks/poll/queue
{"queue_name": "gpu", "handler_name": "render", "worker_id": "host-1234", "limit": 1}
```

| Rule | Source |
|---|---|
| **P1** `handler_name` and `worker_id` are required. `limit` defaults to **1** and is clamped to **1000** server-side. | `workers.rs` (`PollRequest`, `default_poll_limit`, `poll_tasks`) |
| **P2** A poll is per handler. A worker serving N handlers issues N polls (or N concurrent loops). | `workers.rs` (`poll_tasks`) |
| **P3** Queue routing: a worker bound to a named queue MUST use `/workers/tasks/poll/queue` with `queue_name`. The default poll only returns tasks whose `queue_name` is null. Which queue a task lands on can be overridden server-side by routing rules keyed on `(tenant, handler_name[, match_queue])`; workers never see the rules, only the resulting queue. | `workers.rs` (`poll_tasks_from_queue`), `orch8-api/src/queue_routing.rs` |
| **P4** Version: the optional `version` is recorded in the worker registry. If a pin `(tenant, handler_name) → min_version` exists and the version is absent or lower (numeric dot-compare, `v` prefix allowed, lexical fallback), the poll returns **200 with an empty `tasks` array** — not an error. SDKs SHOULD send their app/build version when configured. | `workers.rs` (`version_pin_blocks`), `orch8-types/src/worker.rs` (`version_satisfies`) |
| **P5** Capabilities: the optional `capabilities` object (`RuntimeCapabilities` in `openapi.json`) switches to capability-matched claiming. When sent, `capabilities.runtime_id` MUST equal `worker_id` and `capabilities.handlers` MUST contain `handler_name`, else `400`. Without it, only tasks with empty `requirements` are claimable. | `workers.rs` (`validate_and_record_capabilities`), `orch8-storage/src/sqlite/workers.rs` (`claim`) |
| **P6** Tenant isolation is enforced inside the claim transaction: a tenant-scoped poll never claims another tenant's row. | `workers.rs` (`poll_tasks` comment), `orch8-storage/src/lib.rs` (`claim_worker_tasks_for_tenant`) |
| **P7** Every poll (including a version-blocked one) upserts the worker registry (`worker_id`, handler, queue, version, `last_seen_at`). Liveness in `GET /workers` is "polled within 60 s" by default, so an idle worker MUST keep polling. | `workers.rs` (`record_registration`, `list_workers`) |

### 3.2 Response

```json
{"tasks": [ WorkerTask, ... ], "lease_secs": 60, "heartbeat_interval_secs": 15, "poll_after_ms": 0}
```

| Rule | Source |
|---|---|
| **P8** `poll_after_ms` is `1000` when `tasks` is empty and `0` otherwise; an empty response also carries `Retry-After: 1`. After an empty poll the worker MUST wait at least `poll_after_ms` before polling that handler again. After a non-empty poll it MAY poll again immediately if it has free capacity. | `workers.rs` (`poll_response`) |
| **P9** `lease_secs` = the server's stale threshold (`worker_reaper_stale_secs`, default 60). `heartbeat_interval_secs` = `max(1, lease_secs / 4)`. The worker MUST heartbeat each in-flight task at an interval ≤ `min(configured, heartbeat_interval_secs)` and strictly less than `lease_secs`. | `orch8-server/src/main.rs` (`worker_lease_secs`, `worker_heartbeat_interval_secs`), `orch8-types/src/config.rs` |
| **P10** Claim order is FIFO by `created_at`; claiming is atomic (`FOR UPDATE SKIP LOCKED` on Postgres, `BEGIN IMMEDIATE` on SQLite), so concurrent pollers never receive the same task. Tasks of terminal instances are never claimed. | `orch8-storage/src/sqlite/workers.rs` (`claim`), `docs/WORKERS.md` |
| **P11** A claimed task is owned by `(worker_id, claim_epoch)`. The worker MUST NOT request more tasks than it can start immediately: `limit` ≤ free concurrency slots. | `workers.rs`; SDK requirement |

### 3.3 Claim epoch

Every successful claim increments `claim_epoch` (`claim_epoch = claim_epoch + 1`).
Every mutation (heartbeat, checkpoint, complete, fail, artifact upload) MUST
echo the `claim_epoch` from the poll. A matching `worker_id` alone is not
proof of ownership: a restarted process reusing a stable `worker_id` is
rejected with `409` for tasks claimed by its predecessor.
Source: `orch8-types/src/worker.rs` (`WorkerTask::claim_epoch`, `WorkerClaim`),
`orch8-storage/src/sqlite/workers.rs` (`claim`), `docs/WORKERS.md` (Worker identity).

`worker_id` MUST be unique per process (e.g. `hostname-pid`, pod name).

---

## 4. Heartbeat and resumable checkpoints

### 4.1 Plain heartbeat

```
POST /workers/tasks/{id}/heartbeat
{"worker_id": "host-1234", "claim_epoch": 3}
→ 200 {"checkpoint_seq": 5}
```

| Rule | Source |
|---|---|
| **H1** A heartbeat succeeds only when the task is `claimed` by exactly `(worker_id, claim_epoch)`; it refreshes `heartbeat_at`. The response returns the task's current `checkpoint_seq`. | `workers.rs` (`heartbeat_task`), `orch8-storage/src/lib.rs` (`heartbeat_worker_task`) |
| **H2** Failure modes: `404 not_found` (task deleted — e.g. instance cancelled, race branch cancelled, retry replaced the row) or `409 conflict` ("worker task ownership or checkpoint sequence changed"). Both mean the lease is lost (§5). | `workers.rs` (`heartbeat_task`) |
| **H3** Without a heartbeat, the reaper (tick `worker_reaper_tick_secs`, default 30 s) returns tasks whose `COALESCE(heartbeat_at, claimed_at)` is older than `lease_secs` to `pending` (clearing `worker_id`); the next claim increments `claim_epoch`. The effective worst-case reclaim delay is `lease_secs + tick`. | `orch8-storage/src/sqlite/workers.rs` (`reap_stale`), `orch8-types/src/config.rs` |

### 4.2 Checkpoint heartbeat

```
POST /workers/tasks/{id}/heartbeat
{"worker_id": "host-1234", "claim_epoch": 3,
 "checkpoint_seq": 5, "checkpoint": {"cursor": "page-7"}}
→ 200 {"checkpoint_seq": 6}
```

| Rule | Source |
|---|---|
| **C1** `checkpoint_seq` is REQUIRED whenever `checkpoint` is present (`400` otherwise). It is the *expected* current sequence (compare-and-swap). | `workers.rs` (`heartbeat_task`) |
| **C2** On success the checkpoint is stored, the lease is refreshed, and the response returns `expected + 1`. The SDK MUST use the returned value as the expected sequence of the next checkpoint. | `orch8-storage/src/sqlite/workers.rs` (`checkpoint`) |
| **C3** The first expected sequence for a claimed task is the task's `checkpoint_seq` from the poll response (0 when never checkpointed). | `workers.rs`, `docs/WORKERS.md` |
| **C4** A stale sequence or a former owner gets `409`; a checkpoint never overwrites newer progress. Treat as lease loss (§5). | `orch8-storage/src/sqlite/workers.rs` (`checkpoint` `WHERE ... checkpoint_seq=?`) |
| **C5** Checkpoints are capped at **256 KiB** of serialized JSON (`413`). They are encrypted at rest when storage encryption is on. | `workers.rs` (`MAX_ACTIVITY_CHECKPOINT_BYTES`) |
| **C6** Checkpoints survive reaping (stale-lease reclaim) and engine-driven retry: the next claimer receives `resume_checkpoint` and `checkpoint_seq`. A handler MUST start from `resume_checkpoint` when present instead of repeating completed work. | `workers.rs` (`fail_task` copies `resume_checkpoint`/`checkpoint_seq` into the retry task), `orch8-storage/src/sqlite/workers.rs` (`reap_stale` leaves them intact) |

---

## 5. Lease loss

A lease is lost when any mutation for `(task_id, worker_id, claim_epoch)`
returns **404** or **409**, when the local `timeout_ms` deadline passes, or when
the worker has not heartbeated for `lease_secs`.

| Rule | Source |
|---|---|
| **L1** Every rejected stale mutation is recorded as a `stale_mutation_rejected` attempt event, inspectable via `GET /workers/tasks/{id}/attempts`. | `workers.rs` (`record_stale_rejection`, `list_task_attempts`) |
| **L2** After a 404/409 on heartbeat or checkpoint the SDK MUST stop heartbeating that task, MUST NOT send `complete` or `fail` for that claim (it would be rejected with 409 and cannot win), and SHOULD signal cancellation to the running handler (context / token / flag). | `workers.rs` (all mutations check `state = claimed && worker_id && claim_epoch`) |
| **L3** A 404/409 on `complete`/`fail` MUST NOT be retried and MUST NOT be reported to the user as a handler failure; the task belongs to someone else (or no longer exists). | `workers.rs` (`complete_task`, `fail_task`) |
| **L4** An SDK MUST NOT report success for work whose acknowledgement failed ambiguously; leave it for lease recovery. | `docs/WORKERS.md`, `workers.rs` |

---

## 6. Complete and fail

### 6.1 Complete

```
POST /workers/tasks/{id}/complete
{"worker_id": "host-1234", "claim_epoch": 3, "output": {"message_id": "m-1"},
 "logs": [ StepLogEntry, ... ]}
→ 200 (empty body)
```

| Rule | Source |
|---|---|
| **K1** `worker_id`, `claim_epoch`, `output` are required; `logs` is optional. `output` SHOULD be a JSON object: object keys are merged into the instance `context.data` and the whole value is stored as the block output. A handler that returns nothing MUST be sent as `{}`. | `workers.rs` (`CompleteRequest`, `complete_task`) |
| **K2** The merged context is size-checked against `max_context_bytes` before commit (`413`). | `workers.rs` (`complete_task`, `check_size`) |
| **K3** Idempotency: a retry by the **same** `(worker_id, claim_epoch)` of an already-completed task returns `200`, and the originally committed output wins. SDKs therefore MUST retry `complete` on transport errors and retryable statuses (§1.1) with backoff and the identical body, and MUST NOT retry on other 4xx. | `workers.rs` (`completion_retry`) |
| **K4** Completion for a cancelled/failed/completed or paused instance is accepted with `200` but does not transition the instance. | `workers.rs` (`complete_task`) |

### 6.2 Fail

```
POST /workers/tasks/{id}/fail
{"worker_id": "host-1234", "claim_epoch": 3, "message": "SMTP refused", "retryable": true,
 "logs": [ ... ]}
→ 200 (empty body)
```

| Rule | Source |
|---|---|
| **F1** `message` is required; `retryable` defaults to **false** when omitted. SDKs MUST always send `retryable` explicitly. | `workers.rs` (`FailRequest`) |
| **F2** `retryable: true` → if the step has a `retry` policy with attempts remaining (`attempt + 1 < max_attempts`) the task row is replaced by a new pending task (`attempt + 1`, `claim_epoch` reset to 0, checkpoint preserved) and the engine re-dispatches it; otherwise the step fails as permanent. The worker does not implement backoff — the engine owns retry scheduling. | `workers.rs` (`fail_task` retry branches) |
| **F3** `retryable: false` → the execution node is marked failed (a surrounding `try_catch` can recover) or, for flat step-only sequences, the instance goes to `failed` (DLQ). | `workers.rs` (`fail_task`), `docs/API.md` |
| **F4** Error classification in SDKs: a handler error explicitly typed/flagged non-retryable → `retryable: false`; an explicitly retryable error → `true`; **any other uncaught exception → `true`** (transient by default). A local timeout (`timeout_ms`) → `retryable: true`. No handler registered for a claimed task → `retryable: false`. | `docs/WORKERS.md` (Error handling table) |
| **F5** A `fail` for the same claim is NOT idempotent: after a retryable fail the old row is deleted (`404`), after a permanent fail it is `failed` (`409`). SDKs MAY retry `fail` on transport errors / 5xx, and MUST treat a subsequent 404/409 as "already settled" (L3). | `workers.rs` (`fail_task`) |

### 6.3 Timeouts

`timeout_ms` is measured from the task's `created_at`. The engine's expiry
sweep marks overdue `pending`/`claimed` tasks `failed` with
`error_retryable = false` and message `task timed out (timeout_ms exceeded)`;
later mutations get `409`. SDKs SHOULD also enforce `timeout_ms` locally
(cancel the handler and report `fail` with `retryable: true` if the lease is
still held). Source: `orch8-storage/src/sqlite/workers.rs` (`expire_timed_out`).

---

## 7. Worker loop requirements (SDK-level)

These follow from the rules above and are what the conformance kit checks.

1. **Concurrency** — at most `max_concurrency` tasks execute at once across all
   handlers; `limit` in each poll ≤ free slots (P11); a full worker does not poll.
2. **Cadence** — honour `poll_after_ms` after an empty poll (P8); on poll
   errors back off exponentially (base = poll interval, cap 30 s) and keep
   running; reset after a success.
3. **Heartbeats** — every in-flight task is heartbeated at
   `≤ min(configured, heartbeat_interval_secs)` (P9); plain heartbeats and
   checkpoint heartbeats share the same endpoint.
4. **Checkpoint API** — handlers receive the task (including
   `resume_checkpoint`) and a way to persist a checkpoint that tracks the
   CAS sequence automatically (C2/C3).
5. **Lease loss** — stop heartbeating, cancel the handler, no ack (L2/L3).
6. **Acks** — `complete` retried per K3; `fail` per F5; `retryable` always explicit (F1).
7. **Graceful shutdown** — on stop (SIGTERM / `stop()` / disposal): stop
   polling immediately, keep heartbeating in-flight tasks, let them finish and
   ack, then return. A drain timeout MAY abandon remaining tasks without
   acknowledging them (they are reclaimed via H3).
8. **Registry hygiene** — send `version` when configured (P4); poll the
   queue endpoint when a queue is configured (P3).

---

## 8. Push dispatch

A queue can be switched from pull to push:
`POST /queues/dispatch {"tenant_id", "queue_name", "mode": "push", "push_url", "secret"}`.
The secret is write-only (never echoed); omitting it keeps the stored
secret, `"secret": null` clears it. Push mode requires a public `push_url`.
Source: `orch8-api/src/queue_dispatch.rs`, `orch8-types/src/queue_dispatch.rs`.

### 8.1 Request (engine → worker)

When a task is enqueued on a push queue, the engine still writes the durable
`pending` `worker_tasks` row and then POSTs:

```
POST <push_url>
Content-Type: application/json
X-Orch8-Timestamp: 1767225600
X-Orch8-Signature: sha256=5d0f...        (only when a secret is configured)

{"task_id": "…", "instance_id": "…", "block_id": "…", "handler_name": "…",
 "queue_name": "…", "params": {…}, "context": {…}, "attempt": 0, "timeout_ms": null}
```

Source: `orch8-engine/src/push.rs` (`maybe_push_task`, `post_once`).

### 8.2 Signature

| Rule | Source |
|---|---|
| **S1** `signature = lowercase_hex(HMAC-SHA256(key = secret (UTF-8), message = "<X-Orch8-Timestamp>" + "." + <raw body bytes>))`, sent as `X-Orch8-Signature: sha256=<hex>`. The timestamp is Unix seconds (decimal). This is the same scheme as outbound webhooks. | `orch8-engine/src/webhooks.rs` (`sign`, `send_request`), `orch8-engine/src/push.rs` (`post_once`) |
| **S2** Verifiers MUST compute the MAC over the **exact raw body bytes** (never a re-serialized JSON value) and MUST compare in constant time. The header value MUST be `sha256=` followed by exactly 64 hex digits (hex is compared case-insensitively); anything else — including bare hex, a missing header, or a non-integer timestamp — is rejected when a secret is configured. An empty secret is a configuration error. Test vectors: `fixtures/push_signatures.json`. | S1 |
| **S3** Verifiers MUST reject timestamps outside a tolerance window (default **300 s**, both directions) to bound replay. The engine does not send a nonce or delivery id for push; replay protection is the timestamp window plus the fact that acting on a push only triggers a claim (§8.3). | `orch8-engine/src/push.rs` |
| **S4** A failed verification SHOULD be answered with `401` and MUST NOT trigger any work. | receiver requirement |

### 8.3 Response and semantics

| Rule | Source |
|---|---|
| **D1** Any status `< 400` is delivery success. Any `≥ 400` status or a transport error is retried up to **3** more times with `500 ms × (attempt + 1)` sleeps (4 requests total, 10 s request timeout). After that the task simply stays `pending`. | `orch8-engine/src/push.rs` (`send_push`) |
| **D2** The envelope carries **no `claim_epoch`**: the task is not claimed by the push. A push is a durable-row wake-up. The receiver MUST claim via `POST /workers/tasks/poll/queue` (`queue_name` and `handler_name` from the envelope) and then heartbeat/complete/fail the claimed task exactly as in §3–§6. The claimed task may be a different (older) task of the same queue; that is correct. | `orch8-engine/src/push.rs` (module docs), `orch8-types/src/queue_dispatch.rs` |
| **D3** The receiver SHOULD respond quickly (`202 Accepted`) and claim/execute asynchronously; slow responses risk the 10 s timeout and a duplicate push. Duplicate pushes are harmless because claiming is atomic (P10). | `orch8-engine/src/push.rs` |
| **D4** The engine re-checks the push URL against its SSRF guard at send time; internal addresses are never pushed to (the task stays pending for pollers). | `orch8-engine/src/push.rs` (`send_push`) |

---

## 9. Idempotency summary

| Operation | Idempotent? | Mechanism |
|---|---|---|
| Poll | No (each call may claim) | Bound `limit` by free capacity. |
| Heartbeat (plain) | Yes, per claim | Refreshes `heartbeat_at`. |
| Heartbeat (checkpoint) | CAS | Replaying the same `checkpoint_seq` after success returns 409 — re-read nothing, treat the returned seq as authoritative. On an ambiguous transport failure, retry once with the same seq; a 409 then means either lease loss or that the first write landed (the SDK SHOULD treat it as lease loss for safety). |
| Complete | Yes, per claim | `completion_retry` (K3). |
| Fail | No | F5. |
| Artifact upload | Yes, per `upload_id` | Client-chosen `upload_id` UUID; same id + same bytes is safe to retry, different bytes → 409. `sha256` query param verified if sent. Also refreshes the lease. Source: `workers.rs` (`upload_task_artifact`). |
| Push delivery | At-least-once | D1/D3. |
| Job enqueue / instance create | Yes, with `idempotency_key` | Server-side dedupe (`instances/lifecycle.rs` returns `{"id", "deduplicated": true}`). |

---

## 10. Worker control channel (optional)

Admin-only endpoints (`require_admin`): `POST /workers/commands`
(`drain | reload | ping | place`), `GET /workers/{worker_id}/commands`,
`DELETE /workers/commands/{id}`. A per-tenant `worker` key cannot call them,
so SDKs MUST NOT depend on them for correctness. An SDK MAY poll its command
mailbox when running with an admin key and treat `drain` as graceful shutdown
(§7.7). Source: `workers.rs` (`enqueue_command`, `list_commands`, `ack_command`).

---

## Appendix A — Known divergences in existing SDKs

- `sdk-node` (`src/webhook.ts`, `verifyWebhookSignature`) verifies the
  *trigger* webhook scheme (HMAC over the body only, `x-trigger-*` headers),
  which is not the push scheme of §8.2. Push delivery is verified by
  `@orch8.io/sdk/push` instead.
- Fixed on `sdk-node` branch `feat/adoption` (822be69): generic exceptions now
  fail with `retryable: true` (F4), and a 404/409 heartbeat stops heartbeating
  and acknowledging that claim (L2).
