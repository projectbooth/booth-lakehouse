# Operating booth-lakehouse

Notes for whoever runs the module. Design background: [ADR 0122](https://github.com/projectbooth/booth-architecture)
part B and [design-drop-semantics.md](design-drop-semantics.md).

## What dropping a table does

Every warehouse has Lakekeeper's **soft** delete profile with a **7-day** window (chart value
`warehouse.deleteProfile`, default `{type: soft, expirationSeconds: 604800}`).

- **Any drop is a soft delete.** That covers `DROP TABLE` and `DROP TABLE … PURGE` from Spark, and
  `drop_table`/`purge_table` from PyIceberg or `booth_lakehouse`.
  - The table disappears from listings, and its name can be reused straight away.
  - Its data and metadata files stay where they are.
- **During the 7 days**, a workspace owner can restore it from the Lakehouse view (Manage →
  Lakehouse, "Dropped tables") or through the API (below). The restored table has the same UUID,
  data and history, and can be written to again.
- **After the 7 days**, Lakekeeper removes the catalog entry and deletes the table's files. Nothing
  brings it back after that.

Why every drop behaves the same: the API's proxy forwards each table `DELETE` with
`purgeRequested=true`, overriding the client's flag.
- Under soft, that flag only decides what happens when the window ends: files deleted (`true`), or
  left behind with nothing pointing at them (`false`).
- Left-behind files could never be reached again, because `/register` is refused.
- So forcing `true` means every drop is restorable for the window and is then cleaned up, with no
  orphaned files.

The proxy also strips `force`. In Lakekeeper `force` skips the soft profile, and on a namespace with
`recursive=true` it would delete every table in that namespace at once.

Spark must run with `rest-catalog-purge=true` (booth-spark, ADR 0122 part A). Without it, Spark's
`DROP … PURGE` deletes the files itself, client-side, before Lakekeeper is asked. A restore would then
bring back a table whose files are gone.

**This is not a backup.** Soft delete protects against a mistaken drop. It does not protect against
losing the object store, its volume or its disk. Nothing copies the lake bucket anywhere today
(booth-architecture `ARCHITECTURE.md` item 76). Postgres's nightly dump covers the catalog, but a
catalog without its files restores nothing.

## Restoring a table

From the Lakehouse view, owners see a **Restore** button on each dropped table, with the date it stops
being restorable. Through the API, via core's gateway, as the workspace owner:

```sh
curl -H "Authorization: Bearer $TOKEN" -H "X-Workspace: home" \
  https://<host>/modules/lakehouse/api/dropped-tables
# {"deleteProfile": {...}, "items": [{"id": "...", "namespace": "nfl", "name": "plays",
#                                     "droppedAt": 1792..., "expiresAt": 1792...}]}
curl -X POST -H "Authorization: Bearer $TOKEN" -H "X-Workspace: home" \
  https://<host>/modules/lakehouse/api/dropped-tables/<id>/restore
```

- **409** "a table with this name exists now; rename or drop it first": a new table took the name
  after the drop. Rename or drop the new one, then restore.
- **404**: there is no such dropped table in this workspace, most likely because its window has
  passed.
- **403**: only a workspace owner can restore. Viewers and editors can see the list.

booth-catalog hears about a drop (`table.deleted`) and a restore (`table.created`, same UUID) on the
next events pass, a few seconds later. Each drop and each restore is its own event, however quickly
they follow each other: JetStream discards a repeated message id inside its 2-minute window, so a
drop's id carries the deletion time Lakekeeper recorded for it, and a restore's id the deletion time
of the drop it undoes (ADR 0122 addendum 2026-10-19). A retried publish of the same drop or restore
keeps its id and is still collapsed.

One narrow case keeps the plain id: a restore made through this API whose event hadn't gone out when
the API restarted. If its table hasn't changed since its first `table.created` and that was under
2 minutes earlier, booth-catalog can miss the restore until the table next changes.

## The delete profile and its reconcile

The API creates each warehouse with the configured profile. It also checks every existing warehouse
**at startup and every `warehouse.reconcileSeconds` (300 s)**, and changes any that differ. That is
how a warehouse created before this setting existed (Lakekeeper's default is `hard`) is switched.

- **Safe on live data.** The change is one warehouse setting in Lakekeeper. No table, snapshot or
  file is touched; this is proven in Integration on a hard warehouse holding tables. It affects only
  later drops: a table dropped earlier keeps the window it was dropped with.
- **Logged.** `warehouse delete profile changed workspace=<ws> warehouse=<id> from=hard to=soft(604800s)`
  when it changes something. Nothing is logged when the warehouse already matches.
- **A failure never stops the API.** It logs
  `warehouse delete profile: couldn't check or apply …` and retries after 2 s, 5 s, 10 s and then
  every 30 s for about five minutes, before falling back to the normal interval. The API and
  Lakekeeper often start together, so this is what keeps a fresh install or restart from leaving a
  warehouse on `hard` for minutes. The Lakehouse
  view shows it on the warehouse ("couldn't be applied: …"), as does `GET /api/admin/warehouses`
  (`deleteProfile.status: "error"` with `error`).
- **Checking it:** `GET /api/admin/warehouses` → `items[].deleteProfile` (`type`, `expirationSeconds`,
  `status`, `changedAt`).

To make drops immediate again, set `warehouse.deleteProfile.type=hard`; the next pass applies it. A
different window: `warehouse.deleteProfile.expirationSeconds`.

## After installing a release with this (the homelab's `home` warehouse)

Install it only **after** booth-spark's `rest-catalog-purge=true` release (ADR 0122 B item 5). Then:

1. In the Lakehouse view, check that the warehouse says "keeps its data for 7 days". Or check
   `deleteProfile` = `soft`, `604800`, `ok` in `GET /api/admin/warehouses`. The API log has the
   `changed … from=hard to=soft(604800s)` line once.
2. Drill, on a scratch table (for example `scratch.drill_<date>`), checking booth-catalog after
   every step. Run it within about two minutes from first drop to last restore, since that window is
   what it exercises. "Listed" means the dataset `scratch.drill_<date>` shows in the Catalog's search,
   or, through core's gateway, in
   `GET /modules/catalog/api/datasets?q=scratch.drill_<date>` (an item with `format: iceberg` and
   `table.uuid` equal to the table's UUID). booth-catalog hides a dropped table's dataset, so
   "not listed" is what a drop should lead to.
   1. Create it and write one row. booth-catalog lists it within a few seconds.
   2. Drop it. booth-catalog no longer lists it.
   3. Restore it from the Lakehouse view. booth-catalog lists it again, and it reads back its row.
   4. Drop it again. booth-catalog no longer lists it.
   5. Restore it again. booth-catalog lists it again.
   6. Drop it a last time and leave it to expire.

   If booth-catalog misses a step, the API log has `published table.deleted` or
   `published table.created` for the table's UUID; a step with no such line is this module's fault.
3. Confirm the existing tables and their row counts are unchanged (`nfl.plays` 723,445;
   `nfl.team_game_pbp` 4,190).
