# Orch8 SDK contract

This directory is the language-neutral source used to keep the Node, Python,
Go, and Expo SDKs aligned with the engine.

- `openapi.json` is exported from `orch8-api::openapi::ApiDoc`.
- `fixtures/transport.json` defines behavior every SDK transport must satisfy.
- `routes.json` is the normalized operation manifest consumed by SDK tooling.
- `WORKER_PROTOCOL.md` is the normative, language-neutral worker wire protocol
  (poll/claim, leases, heartbeats + checkpoints, complete/fail, push dispatch
  signatures), with engine source citations for every rule.
- `fixtures/push_signatures.json` holds push-dispatch signature test vectors.
- `conformance/` is the conformance kit: a scripted fake engine plus a runner
  that exercises an SDK's worker, push receiver, verifier and client through a
  small adapter CLI. See `conformance/README.md`.

Run the kit's self-tests (Node >= 20, no dependencies): `npm test`.
Check an SDK: `node conformance/run.mjs --adapter "<adapter command>"`.

Regenerate the contract and language manifests:

```bash
cargo run -p orch8-api --example export_openapi -- sdk-contract/openapi.json
node scripts/generate-sdk-contract.mjs
```

Run the portfolio release gate:

```bash
node scripts/release-sdks.mjs --check
```

Publishing is a two-phase operation. The check phase regenerates contracts and
runs every SDK test/build. The publish phase additionally requires clean SDK
repositories and an exact version confirmation:

```bash
ORCH8_RELEASE_CONFIRM=0.3.0 node scripts/release-sdks.mjs --publish
```
