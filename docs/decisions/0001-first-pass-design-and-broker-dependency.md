# 0001: First pass — design, judgment calls, and what's pending on the credential broker

Status: Ratified (ADR 0084, 2026-09-28); asks routed to booth-core, booth-storage and booth-notebooks.

Everything here was built and tested against real Lakekeeper 0.13.6, real Postgres and real MinIO
(`hack/docker-compose.yml`, `hack/kind-integration.sh`). booth-core's ADR 0080 broker does not exist
yet (latest booth-core commit `fa11da0` has no broker code), so the broker used in every test is a
stand-in (`tests/integration/fakecore.py`) at a **provisional** wire shape — but the credentials it
issues are real MinIO credentials with real, enforced scope.

## Shape of the module

```
engine (notebook kernel, pipeline task) ──Bearer, X-Workspace──▶ booth-core gateway /modules/lakehouse
    │                                                                     │
    │                                                    ┌────────────────▼─────────────────┐
    │                                                    │ API pod (this repo, Python)       │
    │                                                    │ re-verifies token (ADR 0041),     │
    │                                                    │ pins caller to its workspace's    │
    │                                                    │ warehouse, viewer=read/editor=write│
    │                                                    └────────────────┬─────────────────┘
    │                                                                     │ (NetworkPolicy: only path in)
    │                                                    ┌────────────────▼─────────────────┐
    │                                                    │ Lakekeeper (upstream, no auth)    │──▶ Postgres (ADR 0053)
    │                                                    │ writes table metadata with its     │
    │                                                    │ warehouse's broker credential      │──▶ object store
    │                                                    └──────────────────────────────────┘
    └──(caller's own token)──▶ ADR 0080 broker ──▶ booth-storage s3 provider
         s3 grant scoped to ONE table's {backendId, path}, read or readwrite ──▶ PyIceberg reads/writes data files
```

- **One Lakekeeper warehouse per workspace** (`booth-ws-<slug>`), created by a workspace owner by
  choosing a `{backendId, path}` in booth-storage (ADR 0045). The warehouse's storage profile comes
  from the broker grant's resolved location; nothing in this module maps a backend id to a bucket.
- **Writes go through PyIceberg** (ADR 0079); reads use PyIceberg, with `table.duckdb()` for SQL.
- **Client library** `booth_lakehouse` (`client/`): `create_table`, `table`, `append`, `read`
  (incl. `snapshot_id=`), `tables`, `drop_table`, plus `.catalog`/`.iceberg` escape hatches.

## Judgment calls — please ratify or redirect

1. **An authorizing proxy in front of an unauthenticated Lakekeeper**, rather than configuring
   Lakekeeper's own OIDC + OpenFGA. Lakekeeper can't derive a role from ADR 0025's groups grammar, reject a
   forged `X-Booth-Role` (ADR 0041), or trust core's workload issuer alongside the IdP (ADR 0059), and
   OpenFGA would be a second authorization system holding a copy of workspace membership. The cost:
   **the NetworkPolicy "only the API pod reaches Lakekeeper" is load-bearing**, and on a non-enforcing
   CNI (kind, k3s default) it is inert — the same cross-cutting gap as ARCHITECTURE.md §7 item 37(b).
   Every route is fail-closed: unknown Iceberg endpoints are refused until reviewed.

2. **The catalog never vends storage credentials.** ADR 0079 anticipated "Lakekeeper's own
   vended-credentials flow". I switched it (and remote signing) off: each engine asks the broker
   itself, **as the caller**, for a grant scoped to **one table** (read for reads, readwrite for writes).
   Why: the broker's audit then names the real requester (vending would show only this module); scope
   is enforced by the provider, not re-derived by Lakekeeper; and it avoids STS role-chaining from an
   already-temporary credential, which MinIO/AWS support unevenly. Consequence: an engine that isn't
   this client library (Spark, Trino, DuckDB `ATTACH`) must get its own broker credential too.
   Remote signing (credentials never leave Lakekeeper) is the natural later alternative.

3. **`/register` and `/credentials` are refused for everyone.** Registering an existing metadata file
   would let a table point at storage outside its warehouse. Not needed for v0.

4. **Lakekeeper stores its warehouse credential encrypted in Postgres** (its default secret store),
   which ADR 0039 says a module shouldn't do with credentials. The difference: this is a short-lived,
   warehouse-scoped broker grant, not a backend's real credential, and it is replaced every renewal.
   Alternative if unacceptable: Lakekeeper's Vault KV2 store (needs a Vault).

5. **Warehouse credential renewal uses a workload token** minted for this module with
   `owner` = the warehouse's creator (ADR 0056/0058). **Real limit:** core refuses to mint once that
   owner hasn't signed in within its recency window (default 7 days), after which renewal fails and
   the warehouse goes dark when the credential expires. See ask (d) below.
   **Update (2026-09-29, ADR 0088):** ruled "no non-person identity; name a current owner instead".
   Built: renewal tries the creator, then up to 9 other editors/owners this module has seen
   (people only, most recently seen first, from the tokens it already verifies), moving on only when
   core refuses the mint (403) or the broker refuses the resulting token's live role (403). The
   warehouse now goes dark only if *no* editor/owner has used it within core's recency window.

6. **One warehouse per workspace, not movable in v0** (a second `PUT` is 409). Moving a warehouse is
   a data migration. Lakekeeper itself refuses overlapping warehouse locations, so two workspaces can
   never share one prefix.

7. **No UI in v0** (`hasOwnUi: false`), although the brief files the module under Manage — what a UI
   should expose is open question 2 below. **Closed (ADR 0093, 2026-09-30):** a read-only admin view,
   see 0005.

8. **Notebook token discovery** reaches into `booth._default._http.token` (private) when running in a
   booth-notebooks kernel. booth-notebooks should expose a public token accessor; until then this is
   guarded and falls back to `BOOTH_TOKEN`.

## The provisional broker shape, and asks for booth-core / booth-storage

What `booth_lakehouse.broker.HttpBroker` sends (one file to change when core's real shape lands):

```
POST <BOOTH_CREDENTIAL_BROKER_URL>              (default <core.url>/api/credentials)
Authorization: Bearer <caller's own token>      X-Workspace: <slug>
{"kind": "s3", "ttlSeconds": 900,
 "scope": {"backendId": "lake", "path": "lakehouse/<table-uuid>", "access": "read" | "readwrite"},
 "options": {"sessionToken": "allowed" | "forbidden"}}

201 {"leaseId", "kind": "s3", "expiresAt", "scope": <echo>,
     "credential": {"accessKeyId", "secretAccessKey", "sessionToken"?,
                    "endpoint", "region", "bucket", "keyPrefix", "pathStyle"}}
422 {"error": "scope_not_supported", ...}   # the contract's "refuse rather than widen"
```

The client refuses any grant whose echoed scope differs from the request, or whose location doesn't
cover the table. Asks, each found by building against real dependencies, not assumed:

- **(a) A static-key form.** Lakekeeper 0.13.6's `access-key` warehouse credential has **no
  session-token field** (checked in its own `management-openapi`). A warehouse grant must be a key
  pair without a session token, or Lakekeeper can't use it. MinIO can do this (expiring service
  account with an inline policy — what the stand-in does). **AWS IAM access keys can't expire**, so an
  AWS-backed provider would have to refuse, rotate IAM users itself, or the design moves to
  `assume-role-arn` + system identity. This is booth-storage's call, but it decides whether
  lakehouse works on AWS S3 at all.
- **(b) The grant carries its resolved location** (endpoint, bucket, key prefix, region, path style).
  Only the provider knows how `{backendId, path}` maps to a bucket; this keeps ADR 0045 intact.
- **(c) The broker must accept workload tokens.** The callers are notebook kernels and pipeline
  tasks. Core's own `/api/*` routes reject workload tokens today (ADR 0059) — if the broker lives
  there, that rule needs an exception, or the broker lives elsewhere.
- **(d) A non-person identity for unattended renewal** — or a ruling that owner-bound workload tokens
  (judgment call 5) are acceptable with their 7-day limit.
- **(e) Provider facts for booth-storage (MinIO):** an expiring service account can't be shorter than
  15 minutes (so a static-key TTL has a floor above what ADR 0080 might want as a ceiling); and MinIO
  rejects an `s3:prefix` condition on `s3:GetBucketLocation` (it must be a separate statement).

## What is tested against what

| Claim | Evidence | Real dependency? |
|---|---|---|
| Create/append/read, time travel, schema evolution, DuckDB | `tests/integration/test_real_stack.py` | Real Lakekeeper, Postgres, MinIO, API image |
| Data lands under the warehouse's `{backendId, path}` | same, listed with MinIO root creds | Real MinIO |
| A table grant can't read another table or write | same, raw `S3FileSystem` with the grant | Real MinIO policy enforcement |
| Viewer reads, can't write; forged role 401/403; cross-workspace 404 | same + `tests/unit/test_app.py` | Real Lakekeeper behind the real proxy |
| Warehouse credential renewed as the module's workload identity; Lakekeeper outlives the original | same (`zz`, `zzz` with `BOOTH_SLOW=1`) | Real MinIO expiry |
| Chart deploys, CRD validates, NetworkPolicies enforced, state survives restarts | `hack/kind-integration.sh` | Real kind cluster, core's real CRD |
| **The broker's real authorization, audit trail and booth-storage minting** | — | **Pending booth-core + booth-storage (ADR 0080)** |
| **Through booth-core's real gateway** | — | **Not yet run** — fakecore stands in |

## Smaller findings from running against real things

- Lakekeeper's `/v1/config` returns `overrides.uri` = its own base URI; an Iceberg client honours it
  and would silently bypass the proxy. The proxy strips it (tested).
- Lakekeeper's load-table response injects its storage profile (`s3.endpoint` etc.), which overrides a
  client's own settings. The proxy strips it and the client builds FileIO from the broker grant only.
- The upstream Lakekeeper image sets **no USER** — it runs as root by default. The chart pins UID
  65532 (its distroless `nonroot`), verified on kind.
- `quay.io/minio/minio:latest` now answers anonymous pulls with **401**. Resolved by ADR 0087/0091: a
  source-built, digest-pinned mirror at `ghcr.io/projectbooth/minio-test`, shared with booth-storage
  (docs/decisions/0004).
- Current kind (kindnet) **does enforce NetworkPolicy**. `hack/kind-integration.sh` asserts it: an
  unlabelled pod reaches neither the API nor Lakekeeper; a "core" pod reaches the API but not Lakekeeper.

## Open questions for the user (not resolved here)

1. **How tables surface in booth-catalog** — a concrete proposal is in `0002`.
2. **How much of Iceberg is user-facing in v0.** Available today through the client: listing
   snapshots, reading at a snapshot (`read(snapshot_id=...)`), schema evolution via the `.iceberg`
   escape hatch. Not exposed at all: rollback / set-current-snapshot, snapshot expiry, branches/tags,
   a UI. Rollback and expiry change what other readers see, so they're a permissions question as well
   as an API one (editor? owner only?).
