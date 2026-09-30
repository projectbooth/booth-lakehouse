import { useCallback, useEffect, useMemo, useState } from "react";
import { ApiError, getAdminView, lookupUser, type AdminView, type ApiContext, type GetAccessToken, type WarehouseStatus } from "./api";

export type WorkspaceRole = "owner" | "editor" | "viewer";

/**
 * The NativeModuleProps contract (ADR 0031 workspace/role/theme, ADR 0033 getAccessToken): plain
 * React props, so this package never depends on anything booth-design exports (ADR 0030).
 */
export interface LakehouseAppProps {
  /** Active workspace slug (ADR 0025). */
  workspace: string;
  /** Gates what this UI offers. The API enforces the same rule itself (ADR 0041) — this is UX, not
   *  the security boundary. */
  role: WorkspaceRole;
  theme: "dark" | "light";
  /** booth-design's current bearer token (ADR 0032), or null. Called fresh before every request. */
  getAccessToken: GetAccessToken;
}

type State = { kind: "loading" } | { kind: "ready"; view: AdminView } | { kind: "error"; message: string; status: number };

/**
 * booth-lakehouse's native view (ADR 0093), published as @projectbooth/lakehouse-ui: a read-only list
 * of warehouses and their cheap status. Deliberately no create/delete/drop action — creating a
 * warehouse stays in the client library, and deleting one is undecided (a future ADR).
 */
export function LakehouseApp({ workspace, role, theme, getAccessToken }: LakehouseAppProps) {
  const ctx = useMemo<ApiContext>(() => ({ workspace, getAccessToken }), [workspace, getAccessToken]);
  const allowed = role === "owner" || role === "editor";
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

  useEffect(() => (allowed ? load() : undefined), [allowed, load]);

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
        {allowed && (
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

      {!allowed ? (
        <Notice>The lakehouse admin view is available to a workspace&apos;s editors and owners.</Notice>
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
        <WarehouseTable items={state.view.items} names={names} />
      )}
    </div>
  );
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
