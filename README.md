# booth-lakehouse

Project Booth's lakehouse module (nav group **Manage**, ADR 0017): **Apache Iceberg tables on top of
booth-storage's bytes** (ADR 0079). A self-hosted [Lakekeeper](https://github.com/lakekeeper/lakekeeper)
REST catalog (Rust, no JVM) behind a workspace-scoped, role-enforcing API, plus a small Python client
so a notebook or pipeline task works with versioned, schema-evolving tables instead of hand-managed
Parquet files.

> **Status: second pass (`table.*` events for booth-catalog, ADR 0085), still pending booth-core's
> credential broker (ADR 0080).** Everything works end to
> end against real Lakekeeper, Postgres and MinIO, and the chart deploys on kind. The broker used in
> tests is a stand-in at a provisional wire shape, issuing *real* scoped MinIO credentials. See
> [docs/decisions/0001](docs/decisions/0001-first-pass-design-and-broker-dependency.md) for what's
> pending and the judgment calls to ratify.

```python
from booth_lakehouse import Lakehouse

lh = Lakehouse.from_env()                        # BOOTH_GATEWAY_URL, BOOTH_WORKSPACE, broker URL, token
lh.create_table("sales.daily", df)               # schema from the DataFrame; creates the namespace
lh.append("sales.daily", more_rows)              # pandas, pyarrow, or a list of dicts
lh.read("sales.daily").to_pandas()
lh.read("sales.daily", where="amount > 10", columns=["day", "amount"])
lh.read("sales.daily", snapshot_id=...)          # time travel
lh.table("sales.daily").duckdb().sql("select ...")
lh.tables()
```

A workspace owner creates the workspace's warehouse once, by choosing where its tables live in
booth-storage: `lh.create_warehouse("lake", "lakehouse")` (a `{backendId, path}` pair, ADR 0045).

## The admin view

A single read-only native view under **Manage** (`/lakehouse`, ADR 0093), from
`@projectbooth/lakehouse-ui` (`web/`): each warehouse's location, creator, storage-credential state and
table count. Editors/owners see their own workspace; a platform operator (`/platform/operator` in their
token's groups claim, ADR 0094) sees every workspace. No create/delete actions.

```sh
cd web && npm ci && npm run dev      # dev harness against BOOTH_LAKEHOUSE_DEV_BACKEND (default :8080)
npm run typecheck && npm run lint && npm test -- --run && npm run build
```

Published to GitHub Packages by pushing a `lakehouse-ui-v<version>` tag (`.github/workflows/publish-ui.yml`).

## How it fits together

```
client ──Bearer, X-Workspace──▶ core gateway ──▶ API pod ──(only path in)──▶ Lakekeeper ──▶ Postgres
   │                                             re-verifies token,          writes table metadata with
   │                                             pins to the workspace's     its warehouse's broker grant
   │                                             warehouse, viewer/editor
   └──as the caller──▶ ADR 0080 broker ──▶ s3 grant for ONE table, read or readwrite ──▶ data files
```

| Component | Holds | Reachable from |
|---|---|---|
| **API** (`src/booth_lakehouse_server`, this repo's image) | DB DSN; workload-minting credential (renewals); event-bus credential (publish `table.*` only) | booth-core only |
| **Lakekeeper** (upstream image, pinned by digest) | DB DSN; encryption key; each warehouse's current short-lived grant | the API pod only |
| **client** (`client/`, package `booth_lakehouse`) | the caller's own token and per-table grants, in memory | — |

- `proxy.py`: which Iceberg REST calls a role may make; everything else is refused (fail closed).
  Catalog-vended credentials and `/register` are off for everyone; `/v1/config` is always answered for
  the caller's own warehouse; responses are scrubbed of Lakekeeper's address and storage settings.
- `warehouses.py`: owner creates a warehouse from a static-key broker grant (Lakekeeper can't hold a
  session token); the API renews it before expiry as its own workload identity (ADR 0056).
- `identity.py`: same verification and role derivation as the rest of the fleet (ADR 0041), plus
  core's workload issuer, since kernels and pipeline tasks are the expected callers.
- `events.py`: publishes `table.created`/`updated`/`deleted` for booth-catalog (ADR 0085), which
  registers each table as an `iceberg`-format dataset. A reconciler against Lakekeeper, at-least-once,
  `updated` debounced to once a minute per table — [docs/decisions/0003](docs/decisions/0003-table-events-for-booth-catalog.md).

## Installing

Requires booth-core (BoothModule CRD, `booth-database-credentials`, `booth-workload-minting-credentials`,
`booth-event-bus-credentials`)
and, to do anything useful, booth-core's credential broker with booth-storage's `s3` provider (ADR 0080).

```sh
helm install lakehouse charts/booth-lakehouse -n booth-lakehouse \
  --set identity.oidcIssuerUrl=https://keycloak.example/realms/booth
```

Key values: `identity.*` (trusted issuers), `broker.url` (default `<core.url>/api/credentials`),
`workloadIdentity.enabled`, `tableEvents.*`, `warehouseCredential.*`, `core.namespaceSelector`/`podSelector`
(must match your core install; they gate the API's ingress). **The NetworkPolicies need a CNI that
enforces them** — Lakekeeper has no authentication of its own, so "only the API pod reaches it" is a
real security boundary, not decoration.

## Development

```sh
python -m venv .venv && . .venv/bin/activate          # .venv\Scripts\activate on Windows
pip install -e client -e ".[dev]"
ruff check src client tests
pytest tests/unit tests/contract                       # layers 1-2 (contract tests need helm)

docker compose -f hack/docker-compose.yml up -d --build --wait    # layer 3, real dependencies
docker compose -f hack/docker-compose.yml build tests             # (a profile service; `up --build` skips it)
docker compose -f hack/docker-compose.yml run --rm tests          # BOOTH_SLOW=1 adds the ~16-min expiry test
docker compose -f hack/docker-compose.yml down -v

sh hack/kind-integration.sh                            # layer 3 on kind: docker + kind + kubectl + helm
```

| Suite | What it proves |
|---|---|
| `tests/unit` | the `table.*` publisher's decisions (created/updated/deleted, rename, drop-recreate, debounce, never-delete-on-doubt, ack-before-record), token verification + role derivation, every proxy rule, response scrubbing, broker adapter (scope echo, refusals, no secret in repr/logs), warehouse create/renew, the HTTP app with a fake Lakekeeper, client location→`{backendId, path}` mapping and grant caching |
| `tests/contract` | the BoothModule manifest vs. module-manifest.md; who holds which Secret; no static storage credential anywhere; NetworkPolicies; digest-pinned Lakekeeper |
| `tests/integration` (compose) | **real Lakekeeper + Postgres + MinIO + NATS JetStream (core's JWT mode) + this API image**, through the real client: create/append/read, time travel, schema evolution, DuckDB, roles, cross-workspace isolation, per-table grants refused by MinIO outside their table, credential renewal, `table.*` events read back as booth-catalog would, the bus refusing any other event type |
| `hack/kind-integration.sh` | the chart on kind with booth-core's real CRD; the same suite in-cluster; state survives restarting both Deployments |

CI: `.github/workflows/ci.yml` (layers 1–2 + image build, every push/PR, required on `main` via branch
protection), `.github/workflows/integration.yml` (layer 3, merge to `main` and nightly).

## Decisions

- [0001](docs/decisions/0001-first-pass-design-and-broker-dependency.md): design, judgment calls for
  ratification, the provisional broker shape and the asks for booth-core/booth-storage.
- [0002](docs/decisions/0002-proposal-iceberg-tables-in-booth-catalog.md): the booth-catalog proposal —
  ruled Option A (ADR 0085).
- [0003](docs/decisions/0003-table-events-for-booth-catalog.md): the `table.*` publisher.
- [0005](docs/decisions/0005-read-only-admin-view.md): the read-only admin view (ADR 0093) and
  `@projectbooth/lakehouse-ui`.
- [0004](docs/decisions/0004-minio-test-image-built-from-source.md): the shared MinIO test image
  (`ghcr.io/projectbooth/minio-test`, built from source, pinned by digest).

Ratified upstream: ADR 0084 (the first pass's judgment calls), ADR 0085 (tables as `iceberg`-format datasets).
