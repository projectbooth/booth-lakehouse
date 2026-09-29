# 0003: `table.*` events for booth-catalog (ADR 0085)

Status: Built and tested (2026-09-29). Implementation choices below are this module's own, per
ADR 0085 ("debouncing ... left as an implementation detail for whoever builds the publisher").

## What's published

| Subject | When | `data` |
|---|---|---|
| `booth.<ws>.table.created` | a table this module hasn't announced | table summary minus `snapshots` (as `GET /api/tables/{ns}/{t}`) |
| `booth.<ws>.table.updated` | schema, snapshot, location, partitioning, format version or name changed | same |
| `booth.<ws>.table.deleted` | an announced table is gone | `{tableUuid, namespace, name}` (tombstone) |

Envelope per ADR 0026 (`workspace`, `eventType`, `publishedAt`, `publishedBy: "lakehouse"`, `data`),
built from the same binding as the subject, since booth-catalog drops an envelope that disagrees with
its subject. Manifest: `events: {publish: [table.created, table.updated, table.deleted]}`.

## Judgment calls (flagging, not asking)

1. **A reconciler, not publish-on-write.** Every pass (default 10 s, nudged sooner by any write
   through the proxy) lists each warehouse's tables from Lakekeeper and compares them with a record
   of what was last acknowledged by JetStream (`booth_lakehouse.published_tables`). This can't lose an
   event to a crash between a commit and a publish, it sees changes that never pass the proxy
   (Lakekeeper's own expiry tasks), and it debounces by construction. Delivery is at-least-once; a
   republish of the same state carries the same `Nats-Msg-Id`, so JetStream's duplicate window drops it.
2. **Identity is the table UUID.** A rename is `table.updated` with the new name, not delete + create.
   A drop-and-recreate under the same name is `deleted` (old UUID) then `created` (new UUID) —
   deletes are always sent first in a pass. **booth-catalog should key Iceberg datasets on
   `tableUuid`**, not name — worth confirming on its side.
3. **Debounce: `updated` at most once per table per 60 s** (`tableEvents.updateMinGapSeconds`), always
   carrying the latest state when it goes. `created`/`deleted` aren't delayed. A burst of commits right
   after a create can be absorbed into the `created` event itself (observed in the integration test).
4. **Never delete on doubt.** A failed warehouse listing skips that workspace for the pass; a table
   whose metadata can't be read this pass is left exactly as last published.
5. **`lastUpdatedMs` is in the payload but not in the change test** — Lakekeeper bumps it for property
   changes nothing in the catalog reflects.
6. **The bus credential is optional in the chart.** The API starts without `booth-event-bus-credentials`
   and reports `tableEvents: "disabled"` on `/health` (informational; status stays 200). Once core
   provisions it the pod needs a restart to pick it up (env from a Secret) — acceptable, same as any
   ADR 0020 Secret.

## Tested against

- **Real NATS JetStream in booth-core's own JWT operator mode.** `hack/gen-nats-fixtures.sh` compiles a
  small generator inside a throwaway `git archive` of booth-core (commit recorded in
  `tests/integration/natsauth/GENERATED_FROM`) and uses core's `internal/natsauth` to produce the
  server trust config and this module's credential from its manifest via core's own `GrantsFor`. The
  stream is created with core's exact config. `tests/integration/test_table_events.py` reads back with
  a subscriber credential derived the same way (as booth-catalog's would be), and checks the server
  refuses this module's credential for any other event type.
- Not tested: booth-catalog actually consuming these (its build item under ADR 0085), and core's real
  controller provisioning the Secret (the kind run has no bus).

## Found while building

- `nats-py` needs its `nkeys` extra to sign in with a `.creds` file; without it the image would have
  failed on first connect against core's bus. Fixed in `pyproject.toml`, caught by the integration run.
