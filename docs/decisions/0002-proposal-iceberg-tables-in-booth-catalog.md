# 0002: Proposal — how an Iceberg table surfaces in booth-catalog

Status: **Ruled — Option A, event-bus push (ADR 0085, 2026-09-28).** Publisher built: see 0003. Original proposal below.

Was: **Proposal for the coordinator — not built.** ARCHITECTURE.md §7 item 12 and ADR 0079 both
leave this open and say not to resolve it unilaterally. Nothing in this repo registers anything with
booth-catalog.

## The real schema there is to register

`GET /modules/lakehouse/api/tables/{namespace}/{table}` returns this today (built from real Lakekeeper
metadata, exercised in `tests/integration/test_real_stack.py::test_table_summary_is_the_catalog_proposal_shape`):

```json
{
  "namespace": "sales", "name": "daily",
  "tableUuid": "0199…", "formatVersion": 2,
  "location": {"backendId": "lake", "path": "lakehouse/0199…"},
  "schema": [{"name": "day", "type": "string", "required": false, "doc": ""},
             {"name": "amount", "type": "double", "required": false, "doc": ""}],
  "schemaId": 0, "partitionSpec": [],
  "currentSnapshotId": 4417…,
  "snapshots": [{"snapshotId": 4417…, "timestampMs": 1790…, "operation": "append"}],
  "lastUpdatedMs": 1790…
}
```

Against booth-catalog's current `Dataset` (`internal/data/model.go`: `name`, `description`,
`location {backendId, path}`, `schema [{name, type, description}]`, `tags`, `owner`), the overlap is
nearly total: name, location (same ADR 0045 pair), and schema columns map directly. What an Iceberg
table adds: a stable `tableUuid`, a format marker, a location that is a *directory* (not one object),
and a snapshot history.

## Option A — a `dataset` with a format discriminator (recommended)

Add to `Dataset`: `format: "file" | "iceberg"` (default `"file"`, so existing records don't change)
and, for Iceberg only, `table: {namespace, name, uuid, currentSnapshotId}`.

- Search (ADR 0044), lineage (ADR 0046's `dataset` source kind) and permissions (ADR 0048) apply
  unchanged — an Iceberg table *is* a dataset to anyone browsing.
- Readers must branch on `format`: booth-notebooks' `read_dataset` today refuses a directory
  location; with `format: iceberg` it would open the table through `booth_lakehouse` instead of
  reading bytes. That's a small, contained change in each reader.
- `schema` stays the current-schema snapshot; history lives in the lakehouse, not duplicated.

## Option B — a new `table` asset type

A fourth asset type alongside dataset/code/dashboard.

- Cleaner separation, room for table-specific fields later.
- But it duplicates dataset's fields and every consumer (search, lineage source kinds, notebooks'
  `find`) needs a second code path, for something users will think of as "a dataset".

## How registration would happen (either option)

booth-lakehouse would publish `table.created` / `table.updated` (schema change or new snapshot) /
`table.deleted` on the event bus — the same push model dashboards use (ADR 0018/0046), which means
adding `events: {publish: [table.*]}` to this module's manifest (ADR 0050) and booth-catalog
subscribing. Payload = the summary above minus `snapshots`, plus `workspace` from the envelope.
Updated on every commit would be chatty; debouncing (e.g. at most once a minute per table) is an
implementation detail for whoever builds it.

## What I need from the coordinator

A ruling on A vs. B, and whether event-bus push (vs. booth-catalog pulling `/api/tables`) is the
right registration path. Once ruled, the lakehouse side is small: a publisher plus the manifest field.
