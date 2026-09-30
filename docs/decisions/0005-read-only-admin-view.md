# 0005: The read-only admin view (ADR 0093)

Status: Built (2026-09-30). Closes docs/decisions/0001 judgment call 7 ("no UI in v0").

## What shipped

- **`GET /api/admin/warehouses`** (`src/booth_lakehouse_server/admin.py`): warehouses with cheap status —
  location `{backendId, path}`, created-at/by, the storage credential's state (`ok` / `renewing` /
  `expired`, from this module's own store), and table/view counts from **one** Lakekeeper statistics
  call per warehouse (Lakekeeper maintains them on every change; verified against real Lakekeeper that
  page 1 is the newest entry and matches the tables that exist). A failed statistics read degrades
  that field to `null`, never the view. No credential, lease id or secret is in the response.
- **`@projectbooth/lakehouse-ui`** (`web/`): `LakehouseApp`, the `NativeModuleProps` mount contract
  (`workspace`, `role`, `theme`, `getAccessToken` — ADR 0031/0033), no outer padding (ADR 0072),
  requests through `/modules/lakehouse/api`, published to GitHub Packages on a `lakehouse-ui-v*` tag.
- **Manifest**: `hasOwnUi: true`, `uiIntegrationMode: native`, `navGroup: manage`, `navPath: /lakehouse`,
  no `adminNavPath` (one view, per ADR 0093).
- **Read-only by construction**: the route is GET-only (other methods 405, tested); the UI has no
  create/delete/drop control. Deleting a warehouse stays undecided (ADR 0093: a future ADR).

## Correction to the ask

ADR 0093 and the brief say this module had "no user-facing `/api/*` route at all". It did —
`GET/PUT /api/warehouse` and `GET /api/tables*`, with real token verification and ADR 0041 role
derivation since the first pass. The new route reuses exactly that path (`app.py`'s `identity`
dependency), so it inherits the same tested enforcement; its own tests use real RS256-signed tokens.

## Judgment calls (flagging)

1. **Who can view: editors and owners, of their own workspace.** Viewers are refused (they can
   already read their warehouse's binding via `GET /api/warehouse`; this is a Manage view).
2. **Cross-workspace visibility is opt-in and owner-only.** "List the workspaces with a provisioned
   warehouse" can't mean "every tenant's" for an ordinary workspace owner: ADR 0025 roles are per
   workspace and ADR 0052 forbids cross-tenant enumeration for the user directory. So by default a
   caller sees only their own workspace. An operator view of every workspace is available to owners
   acting in a workspace listed in `access.operatorWorkspaces` — **booth-logging's ADR 0067
   `access.workspaces` mechanism, reused rather than reinvented, but fail-closed** (unset means nobody
   sees another tenant; ADR 0067 chose open-by-default for logs, which I don't think fits here).
   ADR 0067 said a real operator role should wait for "a second module" needing the distinction —
   this is that second consumer (and booth-database's identical view will likely be the third), so
   it's worth the coordinator deciding whether it's time for that ADR.

   **Update (2026-09-30, ADR 0094): ruled — a real role.** A platform operator is now whoever has
   `/platform/operator` in their verified token's groups claim (exact match), and `access.operatorWorkspaces`
   is gone. The scoping above is unchanged (own workspace for editors/owners, everything for an operator,
   fail-closed when nobody holds the claim). One consequence of "operator status is orthogonal to
   workspace role": an operator gets the all-workspaces view whatever their role where they're acting,
   including viewer; the old allowlist required *owner* only because that was part of how an operator was
   identified. An operator still needs a verified membership in the workspace they're acting in, as every
   route does — the claim widens what they see, not whether they're authenticated.
3. **Creator names** are resolved through core's user directory (best-effort, own workspace only,
   ADR 0052), falling back to the raw `sub`.
