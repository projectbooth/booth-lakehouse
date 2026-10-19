# Design note: what dropping a lakehouse table does, and the undo window (ADR 0122 part B)

Status: **for the user's decision.** Nothing here is built. The homelab is frozen until the NFL gate
(Wed 21 Oct 09:00) and the cutover are done (ADR 0121). Source: ADR 0122, booth-spark
`docs/findings-iceberg-drop-purge.md` (b780965), Lakekeeper v0.13.6 source (`ff42848`), PyIceberg
0.12.0 source, and a local reproduction (below). Written 2026-10-19.

## 1. Reproduced, with one correction

Local run against Lakekeeper v0.13.6 (this chart's digest), Postgres and SeaweedFS 4.48 (the
`hack/seaweedfs-gate` stack, task poll 1 s). Warehouse `home` with this module's storage profile and
the default `hard` delete profile; each case creates a table with 3 rows (5 objects), drops it, waits
12 s, counts objects under the table's location. **13 of 13 checks passed.**

| Drop request | Who sends it | Hard profile (the homelab's) |
|---|---|---|
| `DELETE …/tables/t`, no `purgeRequested` | Iceberg Java 1.x `dropTable` (Spark plain `DROP TABLE`, and Spark `DROP … PURGE` without `rest-catalog-purge`), Trino, most engines | catalog entry gone, **all 5 objects deleted** |
| `DELETE …?purgeRequested=false` | **PyIceberg 0.12 `catalog.drop_table()`**, so also this module's `booth_lakehouse` client `Lakehouse.drop_table()` | catalog entry gone, **files kept** (orphans) |
| `DELETE …?purgeRequested=true` | PyIceberg `purge_table()`, Spark `DROP … PURGE` with `rest-catalog-purge=true` | catalog entry gone, all objects deleted |

Why: `DropParams.purge_requested` has `#[serde(default = "true_fn")]`
(`crates/lakekeeper/src/api/iceberg/types.rs:396`). Under `hard`, `drop_table` schedules a
`tabular_purge` task when it is true (`server/tables.rs:793`). **This is deliberate and documented
upstream** (`docs/docs/concepts.md`, "Dropping Tables": managed tables default `purgeRequested` to
true unless explicitly false).

**Correction to the Spark findings:** "PyIceberg behaves the same" is not right for PyIceberg 0.12.
Its `drop_table` sends `purgeRequested=False` explicitly (`pyiceberg/catalog/rest/__init__.py:1131`),
so a PyIceberg drop keeps the files. Those files can't be brought back through the platform either,
because this module refuses `/register` for everyone (decision 0001). So today:

- a Spark/SQL drop **deletes** the data within seconds, with no undo;
- a PyIceberg or `booth_lakehouse` drop **leaves orphaned files** that nothing references or cleans
  up (this module's own `test_table_events` Integration test leaves one each run).

This module's proxy forwards the drop request's query string unchanged (`app.py:214`), so the client
alone decides.

**Which tests would show a regression: none today.** `tests/integration/test_table_events.py` drops
a table but asserts only the `table.deleted` event. `tests/unit/test_proxy.py` checks only that
`DELETE` on a table needs the editor role. Nothing counts objects after a drop. Whichever option is
chosen, add to Integration (real Lakekeeper + MinIO, `hack/docker-compose.yml`, with
`LAKEKEEPER__TASK_POLL_INTERVAL=1s` so the drop tasks run promptly):

- through the proxy, one drop each with no flag, `false` and `true`;
- assert the object count afterwards matches the chosen semantics;
- under soft, list the dropped table, undrop it and read the rows back.

Add a contract test for the chart's default profile.

## 2. Options

### (i) Keep `hard`; make every plain drop "do not purge"

- **How:** the proxy adds `purgeRequested=false` when a table `DELETE` has no flag. This covers every
  engine at once; changing only our client would not, since PyIceberg already sends false.
  - Spark then does `PURGE` itself, client-side, and nothing races (Lakekeeper no longer purges).
  - With ADR 0122 A (`rest-catalog-purge=true`), Lakekeeper does the purge instead.
- **Cost:** about 10 lines in `proxy.py`/`app.py`, a unit test and the Integration test above. One
  release.
- **Risk:**
  - **There is still no undo.** A dropped table's files remain, but nothing points at them, and
    users can't re-register them (`/register` is refused).
  - Recovery is an operator's job: find the last `*.metadata.json` under the old location and
    register it with Lakekeeper from inside the API pod.
  - Orphans pile up with no owner and no expiry, so someone has to delete them by hand.
  - It also departs from Lakekeeper's documented behaviour for managed tables.
- **Homelab:** nothing changes on the warehouse. It ships with the next lakehouse release after the
  freeze.

### (ii) Lakekeeper's `soft` delete profile with an undrop window

- **What it does (verified locally):**
  - A drop marks the table deleted: it's unlisted and its name is free to reuse.
  - At drop time Lakekeeper schedules an expiration for `expiration-seconds` later. The window is
    fixed when the table is dropped, so later changes to the setting don't move it.
  - Inside the window the files are untouched, and an undrop brings the table back intact (3 rows,
    sum 42).
  - At expiry the catalog entry is removed for good. Its files are purged only if the drop said
    `purgeRequested` true or said nothing.
  - So after expiry a PyIceberg `drop_table` (explicit `false`) leaves orphans: 5 objects kept,
    verified.
  - `force=true` bypasses soft entirely (`server/tables.rs:785`). Our clients don't send it.
  - Undropping fails with 409 (`AlreadyExistsException`) if a live table now has the same name.
    That table must be renamed or dropped first.
- **Window:** proposed **7 days** (`604800` s). It is per warehouse.
- **Storage cost:** a dropped table keeps all its bytes for the window, on SeaweedFS's 50 Gi volume.
  The `nfl` tables are tens to hundreds of MiB, not measured. A drop-and-rebuild loop holds one
  extra copy per drop for 7 days. Watch free space if anyone rebuilds large tables daily.
- **How to undrop:** only through Lakekeeper's management API:
  - `GET /management/v1/warehouse/{id}/deleted-tabulars` lists dropped tables, with `deleted-at` and
    `expiration-date`;
  - `POST /management/v1/warehouse/{id}/deleted-tabulars/undrop` with
    `{"targets":[{"type":"table","id":"<uuid>"}]}` restores one.

  Neither is reachable by users. The proxy doesn't expose `/management`, and Lakekeeper's own UI is
  behind the NetworkPolicy (only the API pod reaches Lakekeeper). So this needs, in booth-lakehouse:
  - `GET /api/warehouse/dropped`, for editors and up;
  - `POST /api/warehouse/dropped/{uuid}/restore`, for owners;
  - a "Recently dropped" panel in the UI.

  Until those ship, an undrop is an operator runbook: `kubectl exec` into the API pod and send the
  POST to `http://<release>-lakekeeper:8181`.
- **Config:** no Lakekeeper or chart restart is needed. It is one per-warehouse call,
  `POST /management/v1/warehouse/{id}/delete-profile` with
  `{"delete-profile":{"type":"soft","expiration-seconds":604800}}`.
  - That call updates one warehouse row. It changes no table and touches no file.
  - Only drops made after the change are soft.
  - **Safe on live data, verified:** I switched a warehouse holding a live table from hard to soft,
    then read it, appended to it and read 4 rows back.
  - Best shipped as code, not a hand call: a chart value `warehouse.deleteProfile`, applied by the
    API when it creates a warehouse and reconciled for existing warehouses at startup (idempotent).
    The homelab then gets it through an ordinary release.
- **Must go with ADR 0122 A** (Spark `rest-catalog-purge=true`). Without it, Spark's `DROP … PURGE`
  deletes the files client-side straight away. An undrop would then restore a table whose files are
  gone.
- **Two consequences to check before shipping:**
  - **The `table.*` publisher:** a soft drop unlists the table, so `table.deleted` goes out (expected
    but not tested). An undrop makes the table reappear with the same uuid, which would publish
    `table.created` again. booth-catalog should then re-show the dataset; confirm with its owner.
  - **Credential renewal:** the expiration purge runs with the warehouse's current credential. If
    renewal is broken at that moment, the purge task fails and the files stay as orphans. That fails
    safe.
- **Cost:**
  - the chart value plus the startup reconcile;
  - the two API routes and the UI panel;
  - Integration tests;
  - the proxy rule below.

  About one to two days, in one lakehouse release after the freeze.
- **Homelab:** one metadata change to the `home` warehouse, applied by that release.

### (iii) Leave as is and rely on backups

- **What exists today for the `lake` bucket: nothing for the data.** `deployment/homelab.md`
  (SeaweedFS, 2026-10-16): "Object data and filer metadata have no off-cluster backup in v0 (NFL data
  is re-ingestible)".
- Lakekeeper's catalog database (`booth_mod_lakehouse`) is core-provisioned, so core's nightly
  `pg_dump` to Garage should include it. Check today's MANIFEST.
- After a purge, though, restoring the catalog only brings back pointers to deleted files. It would
  also roll back every other table in the warehouse.
- **Recovery today means re-ingesting from source.** For `nfl.plays` that was 15 Spark runs
  (2011–2025) on 2026-10-18.
- **Cost of a real backup:** a new component, such as a scheduled bucket mirror to Garage on server
  2. Garage has a 20 Gi volume, shared with the Postgres dumps. It also needs a restore drill, and
  is a separate ADR 0109-style decision.
- **Risk:** the status quo. Any drop, from any engine, is permanent within seconds.

## 3. Recommendation (default)

**(ii) soft, 7 days, for every warehouse (the chart default), with three parts:**

1. **ADR 0122 A** in booth-spark, as already ruled.
2. **One proxy rule:** a table `DELETE` is forwarded with `purgeRequested=true` whatever the client
   sent.
   - In a Booth warehouse, "drop and keep the files" has no use: `/register` is refused, so the
     files could never be reached again.
   - It turns PyIceberg's and our client's drops from permanent orphans into "recoverable for 7
     days, then cleaned up".
   - This overrides an explicit client flag. It is a judgment call; the alternative is a periodic
     orphan sweeper.
3. **The undrop API and UI panel.**

(i) is cheaper but gives no real undo. (iii) gives none either until a data backup exists, and that
is a bigger project. Off-site data backup is still worth doing separately, because soft delete does
not protect against losing the volume.

**Can the `home` warehouse be switched safely? Yes.**
- The profile change rewrites nothing in `nfl.plays` (723,445 rows), `nfl.team_game_pbp` (4,190) or
  the other two of the 4 tables listed.
- It only changes what future drops do. The local run shows a live table still reads and takes
  appends after the switch.

**Order:**
1. After the gate and the cutover: the booth-spark A release and the lakehouse release, in either
   order, both before anyone is allowed to drop `nfl` tables again.
2. Check: `GET …/warehouse/{id}` shows `soft`.
3. Drill: drop and undrop a scratch table, then confirm the 4 tables and the row counts are
   unchanged.

ADR 0122's interim rules stay in force until then.

## 4. The Lakehouse UI

The UI is read-only and never drops anything. Drops come from SQL and engines, so there is no
"are you sure?" to add. It should instead:

- **Say what a drop does**, from the warehouse's real profile.
  - Under hard: "Dropping a table deletes its data within seconds. There is no undo."
  - Under soft: "Dropped tables can be restored for 7 days."
- **List recently dropped tables** under soft: name, dropped at, recoverable until. Owners get a
  Restore button, and the 409 is shown as "a table with this name exists now; rename or drop it
  first".
- Not a confirmation in engines: Spark and PyIceberg can't be made to ask.

## 5. Upstream

**Not a bug report.** Lakekeeper documents "missing `purgeRequested` = purge" for managed tables as
intended, and recommends soft delete for protection. That is the same conclusion as (ii).

A feature request is reasonable: a per-warehouse option to honour the Iceberg REST spec's default
(`purgeRequested` false when absent), or a note in the API reference that the default differs from
the spec. Minimal reproduction to attach:
- Lakekeeper v0.13.6, any S3, a warehouse with the default delete profile;
- a table created and appended with any client;
- then:

```
curl -X DELETE "$LK/catalog/v1/$WH/namespaces/ns/tables/t"                      # no flag
# ~2 s later (task poll 1 s): Lakekeeper log "Data at location s3://…/<uuid> deleted"; prefix empty
curl -X DELETE "$LK/catalog/v1/$WH/namespaces/ns/tables/t2?purgeRequested=false"
# files kept
```

The full script: the scratchpad's `drop/drop.py`, run in the `hack/seaweedfs-gate` stack's gate
image. It is not committed (no code in this change); commit it under `hack/` if the Integration test
is wanted from it.
