import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ApiError,
  getAdminView,
  getDroppedTables,
  lookupUser,
  restoreDroppedTable,
  type AdminView,
  type ApiContext,
  type DeleteProfile,
  type DroppedTable,
  type GetAccessToken,
  type WarehouseStatus,
} from "./api";

export type WorkspaceRole = "owner" | "editor" | "viewer";

/**
 * The NativeModuleProps contract (ADR 0031 workspace/role/theme, ADR 0033 getAccessToken): plain
 * React props, so this package never depends on anything booth-design exports (ADR 0030).
 */
export interface LakehouseAppProps {
  /** Active workspace slug (ADR 0025). */
  workspace: string;
  /** Part of the mount contract, but deliberately NOT used to decide whether to load: who may see
   *  the view isn't a function of workspace role alone — a platform operator (ADR 0094) may be a
   *  viewer here. The API decides (it re-verifies the token, ADR 0041); a 403 means "not for you". */
  role: WorkspaceRole;
  theme: "dark" | "light";
  /** booth-design's current bearer token (ADR 0032), or null. Called fresh before every request. */
  getAccessToken: GetAccessToken;
}

type State = { kind: "loading" } | { kind: "ready"; view: AdminView } | { kind: "error"; message: string; status: number };

/**
 * booth-lakehouse's native view (ADR 0093), published as @projectbooth/lakehouse-ui: the warehouses
 * and their cheap status, plus (ADR 0122 B) what dropping a table does here and the workspace's
 * recently dropped tables, which an owner can restore. Still no create/delete/drop action — creating
 * a warehouse stays in the client library, and tables are dropped from engines (SQL, PyIceberg).
 */
export function LakehouseApp({ workspace, role, theme, getAccessToken }: LakehouseAppProps) {
  const ctx = useMemo<ApiContext>(() => ({ workspace, getAccessToken }), [workspace, getAccessToken]);
  const [state, setState] = useState<State>({ kind: "loading" });
  const [names, setNames] = useState<Record<string, string>>({});

  const load = useCallback(() => {
    let live = true;
    setState({ kind: "loading" });
    getAdminView(ctx).then(
      (view) => live && setState({ kind: "ready", view }),
      (e: unknown) => live && setState({ kind: "error", status: e instanceof ApiError ? e.status : 0, message: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [ctx]);

  // Always ask: the server answers "workspace", "all" (an operator, whatever their role here), or 403.
  useEffect(() => load(), [load]);
  const forbidden = state.kind === "error" && state.status === 403;

  // Best-effort display names for creators in the caller's own workspace (the directory is
  // workspace-scoped, ADR 0052); anything else keeps showing the raw subject.
  useEffect(() => {
    if (state.kind !== "ready") return;
    let live = true;
    const subs = [...new Set(state.view.items.filter((i) => i.workspace === workspace).map((i) => i.createdBy))];
    Promise.all(subs.map(async (s) => [s, await lookupUser(ctx, s)] as const)).then((pairs) => {
      if (!live) return;
      setNames(Object.fromEntries(pairs.filter(([, n]) => n).map(([s, n]) => [s, n as string])));
    });
    return () => {
      live = false;
    };
  }, [state, ctx, workspace]);

  return (
    <div data-theme={theme} className="flex flex-col gap-4 text-slate-900 dark:text-slate-100">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 pb-3 dark:border-slate-800">
        <div>
          <h1 className="text-lg font-semibold">Lakehouse warehouses</h1>
          <p className="text-sm text-slate-500 dark:text-slate-400">
            {state.kind === "ready" && state.view.scope === "all"
              ? "Every workspace (operator view). Read-only."
              : `Workspace ${workspace}. Read-only.`}
          </p>
        </div>
        {!forbidden && (
          <button
            type="button"
            onClick={() => load()}
            disabled={state.kind === "loading"}
            className="rounded-md border border-slate-300 px-3 py-1.5 text-sm hover:bg-slate-100 disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
          >
            Refresh
          </button>
        )}
      </header>

      {forbidden ? (
        <Notice>The lakehouse admin view is available to a workspace&apos;s editors and owners, and to platform operators.</Notice>
      ) : state.kind === "loading" ? (
        <p className="text-sm text-slate-500" role="status">
          Loading…
        </p>
      ) : state.kind === "error" ? (
        <Notice tone="error">
          {state.status === 503 || state.status === 502
            ? "The lakehouse module isn't reachable right now."
            : `Couldn't load warehouses: ${state.message}`}
        </Notice>
      ) : state.view.items.length === 0 ? (
        <Notice>
          This workspace has no warehouse yet. A workspace owner creates one by choosing a storage location, with the{" "}
          <code className="font-mono text-xs">booth_lakehouse</code> client&apos;s{" "}
          <code className="font-mono text-xs">create_warehouse(backend_id, path)</code>.
        </Notice>
      ) : (
        <>
          <WarehouseTable items={state.view.items} names={names} />
          {state.view.items.some((i) => i.workspace === workspace) && (
            <DroppedTablesPanel
              ctx={ctx}
              isOwner={role === "owner"}
              profile={state.view.items.find((i) => i.workspace === workspace)?.deleteProfile ?? null}
            />
          )}
        </>
      )}
    </div>
  );
}

type Dropped = { kind: "loading" } | { kind: "ready"; items: DroppedTable[]; profile: DeleteProfile | null } | { kind: "error"; message: string };

/**
 * ADR 0122 B: says what a drop does under this warehouse's current delete profile, and lists the
 * tables dropped in this workspace that can still be restored. Restore is owner-only (the API
 * enforces it; the button is only hidden for others).
 */
function DroppedTablesPanel({ ctx, isOwner, profile }: { ctx: ApiContext; isOwner: boolean; profile: DeleteProfile | null }) {
  const [state, setState] = useState<Dropped>({ kind: "loading" });
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState<{ tone: "info" | "error"; text: string } | null>(null);

  const load = useCallback(() => {
    let live = true;
    getDroppedTables(ctx).then(
      (d) => live && setState({ kind: "ready", items: d.items, profile: d.deleteProfile }),
      (e: unknown) => live && setState({ kind: "error", message: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      live = false;
    };
  }, [ctx]);
  useEffect(() => load(), [load]);

  const restore = async (t: DroppedTable) => {
    setBusy(t.id);
    setMessage(null);
    try {
      await restoreDroppedTable(ctx, t.id);
      setMessage({ tone: "info", text: `Restored ${t.namespace}.${t.name}.` });
      load();
    } catch (e: unknown) {
      setMessage({ tone: "error", text: `Couldn't restore ${t.namespace}.${t.name}: ${e instanceof Error ? e.message : String(e)}` });
    } finally {
      setBusy(null);
    }
  };

  const shown = profile ?? (state.kind === "ready" ? state.profile : null);
  return (
    <section aria-labelledby="dropped-heading" className="flex flex-col gap-3">
      <h2 id="dropped-heading" className="text-base font-semibold">
        Dropped tables
      </h2>
      <DropSemantics profile={shown} />
      {message && <Notice tone={message.tone}>{message.text}</Notice>}
      {state.kind === "loading" ? (
        <p className="text-sm text-slate-500" role="status">
          Loading dropped tables…
        </p>
      ) : state.kind === "error" ? (
        <Notice tone="error">Couldn&apos;t load dropped tables: {state.message}</Notice>
      ) : state.items.length === 0 ? (
        <p className="text-sm text-slate-500 dark:text-slate-400">No dropped tables to restore.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-sm" aria-label="Dropped tables">
            <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
              <tr>
                <th className="py-2 pr-4 font-medium">Table</th>
                <th className="py-2 pr-4 font-medium">Dropped</th>
                <th className="py-2 pr-4 font-medium">Restorable until</th>
                <th className="py-2 font-medium">
                  <span className="sr-only">Action</span>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-200 dark:divide-slate-800">
              {state.items.map((t) => (
                <tr key={t.id}>
                  <td className="py-2 pr-4 font-mono text-xs">
                    {t.namespace}.{t.name}
                  </td>
                  <td className="py-2 pr-4">{formatDate(t.droppedAt)}</td>
                  <td className="py-2 pr-4">{formatDate(t.expiresAt)}</td>
                  <td className="py-2">
                    {isOwner ? (
                      <button
                        type="button"
                        onClick={() => restore(t)}
                        disabled={busy !== null}
                        className="rounded-md border border-slate-300 px-3 py-1 text-sm hover:bg-slate-100 disabled:opacity-50 dark:border-slate-700 dark:hover:bg-slate-800"
                      >
                        {busy === t.id ? "Restoring…" : "Restore"}
                      </button>
                    ) : (
                      <span className="text-xs text-slate-500 dark:text-slate-400">A workspace owner can restore it</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

/** What a drop does under the warehouse's current profile, said before anyone drops anything. */
function DropSemantics({ profile }: { profile: DeleteProfile | null }) {
  if (!profile || profile.status === "pending" || !profile.type) {
    return <Notice>The lakehouse hasn&apos;t confirmed what a drop does in this warehouse yet. Treat a drop as permanent until it does.</Notice>;
  }
  const failed = profile.status === "error" && (
    <p className="mt-1">
      The configured setting ({profile.desired.type === "soft" ? `restorable for ${duration(profile.desired.expirationSeconds)}` : "no undo"}) couldn&apos;t be
      applied: {profile.error}. It is retried automatically.
    </p>
  );
  if (profile.type === "soft") {
    return (
      <Notice tone={profile.status === "error" ? "error" : "info"}>
        Dropping a table (<code className="font-mono text-xs">DROP TABLE</code>, with or without <code className="font-mono text-xs">PURGE</code>, from Spark,
        PyIceberg or any engine) keeps its data for {duration(profile.expirationSeconds)}, and an owner can restore it here. After that the table and its files
        are deleted for good. This is not a backup of the lake bucket.
        {failed}
      </Notice>
    );
  }
  return (
    <Notice tone="error">
      Dropping a table deletes its data within seconds. There is no undo.
      {failed}
    </Notice>
  );
}

function duration(seconds: number | null): string {
  if (!seconds) return "a short while";
  const days = seconds / 86400;
  if (Number.isInteger(days)) return days === 1 ? "1 day" : `${days} days`;
  const hours = Math.round(seconds / 3600);
  return hours >= 1 ? `${hours} hours` : `${Math.round(seconds / 60)} minutes`;
}

function WarehouseTable({ items, names }: { items: WarehouseStatus[]; names: Record<string, string> }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm">
        <thead className="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">
          <tr>
            <th className="py-2 pr-4 font-medium">Workspace</th>
            <th className="py-2 pr-4 font-medium">Location</th>
            <th className="py-2 pr-4 font-medium">Created</th>
            <th className="py-2 pr-4 font-medium">Tables</th>
            <th className="py-2 font-medium">Storage credential</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-200 dark:divide-slate-800">
          {[...items]
            .sort((a, b) => a.workspace.localeCompare(b.workspace))
            .map((w) => (
              <tr key={w.workspace}>
                <td className="py-2 pr-4 font-medium">{w.workspace}</td>
                <td className="py-2 pr-4">
                  <span className="font-mono text-xs" title={w.storageRoot}>
                    {w.location.backendId}:{w.location.path}
                  </span>
                </td>
                <td className="py-2 pr-4">
                  <div>{formatDate(w.createdAt)}</div>
                  <div className="text-xs text-slate-500 dark:text-slate-400" title={w.createdBy}>
                    by {names[w.createdBy] ?? w.createdBy}
                  </div>
                </td>
                <td className="py-2 pr-4">
                  {w.tables ? (
                    <span title={w.tables.asOf ? `as of ${w.tables.asOf}` : undefined}>
                      {w.tables.tables}
                      {w.tables.views > 0 && <span className="text-slate-500"> (+{w.tables.views} views)</span>}
                      {!!w.tables.dropped && <span className="text-slate-500"> ({w.tables.dropped} dropped, restorable)</span>}
                    </span>
                  ) : (
                    <span className="text-slate-500" title="Couldn't read Lakekeeper's statistics just now">
                      —
                    </span>
                  )}
                </td>
                <td className="py-2">
                  <CredentialBadge status={w.credential.status} expiresAt={w.credential.expiresAt} />
                </td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
}

const BADGE: Record<WarehouseStatus["credential"]["status"], { label: string; cls: string; hint: string }> = {
  ok: { label: "OK", cls: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300", hint: "Renewed automatically before it expires." },
  renewing: { label: "Renewing", cls: "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300", hint: "Inside its renewal window; the next pass replaces it." },
  expired: {
    label: "Expired",
    cls: "bg-rose-100 text-rose-800 dark:bg-rose-900/40 dark:text-rose-300",
    hint: "Renewal has been failing, so tables can't be created or written. It renews on behalf of the workspace's editors/owners who've used the lakehouse recently.",
  },
};

function CredentialBadge({ status, expiresAt }: { status: WarehouseStatus["credential"]["status"]; expiresAt: number }) {
  const b = BADGE[status];
  return (
    <span className="inline-flex items-center gap-2">
      <span className={`rounded px-2 py-0.5 text-xs font-medium ${b.cls}`} title={b.hint}>
        {b.label}
      </span>
      <span className="text-xs text-slate-500 dark:text-slate-400">
        {status === "expired" ? "expired" : "expires"} {formatDate(expiresAt)}
      </span>
    </span>
  );
}

function Notice({ children, tone = "info" }: { children: React.ReactNode; tone?: "info" | "error" }) {
  const cls =
    tone === "error"
      ? "border-rose-300 bg-rose-50 text-rose-900 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-200"
      : "border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300";
  return (
    <div role={tone === "error" ? "alert" : undefined} className={`rounded-md border p-3 text-sm ${cls}`}>
      {children}
    </div>
  );
}

function formatDate(unixSeconds: number): string {
  return new Date(unixSeconds * 1000).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}
