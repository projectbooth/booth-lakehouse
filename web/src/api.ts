// Requests go through booth-core's gateway at /modules/lakehouse/*, which strips that prefix before
// forwarding to this repo's API (src/booth_lakehouse_server/app.py). A bare "/api/..." would hit
// booth-core's own API instead. The API re-verifies the token and derives the role itself (ADR 0041).
const LAKEHOUSE = "/modules/lakehouse/api";
// booth-core's own API (user directory, ADR 0047/0052): shell origin, no /modules prefix.
const CORE = "/api";

export type GetAccessToken = () => string | null;

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export interface ApiContext {
  workspace: string;
  getAccessToken: GetAccessToken;
}

export interface WarehouseStatus {
  workspace: string;
  warehouseName: string;
  /** Where the warehouse's tables live in booth-storage (ADR 0045). */
  location: { backendId: string; path: string };
  storageRoot: string;
  /** Unix seconds. */
  createdAt: number;
  /** The creator's token `sub`. */
  createdBy: string;
  credential: { status: "ok" | "renewing" | "expired"; expiresAt: number };
  /** From Lakekeeper's own statistics; null when they couldn't be read just now. */
  /** `tables` counts live tables; `dropped` (when known) the restorable dropped ones (ADR 0122 B). */
  tables: { tables: number; views: number; asOf: string | null; dropped?: number } | null;
  /** What dropping a table does here (ADR 0122 B), as the API last saw it; null on an older API. */
  deleteProfile?: DeleteProfile | null;
}

export interface DeleteProfile {
  /** "soft": a dropped table is restorable for expirationSeconds. "hard": gone at once. null: never read. */
  type: "soft" | "hard" | null;
  expirationSeconds: number | null;
  /** "error": the API couldn't check or apply the configured profile (see error); "pending": not checked yet. */
  status: "ok" | "error" | "pending";
  desired: { type: "soft" | "hard"; expirationSeconds: number | null };
  error?: string;
  changedAt?: number;
}

export interface DroppedTable {
  id: string;
  namespace: string;
  name: string;
  /** Unix seconds. */
  droppedAt: number;
  /** Unix seconds: after this the table and its files are removed for good. */
  expiresAt: number;
}

export interface DroppedTables {
  deleteProfile: DeleteProfile | null;
  items: DroppedTable[];
}

export interface AdminView {
  /** "workspace": the caller's own workspace only. "all": an operator's view of every workspace. */
  scope: "workspace" | "all";
  items: WarehouseStatus[];
}

async function request<T>(ctx: ApiContext, url: string, method = "GET"): Promise<T> {
  const token = ctx.getAccessToken(); // fresh every call, never cached (ADR 0033)
  const headers: Record<string, string> = { "X-Workspace": ctx.workspace, Accept: "application/json" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const resp = await fetch(url, { method, headers });
  if (!resp.ok) {
    let detail = "";
    try {
      const body = await resp.json();
      detail = typeof body?.detail === "string" ? body.detail : "";
    } catch {
      // not JSON: keep the status line
    }
    throw new ApiError(resp.status, detail || `${resp.status} ${resp.statusText}`);
  }
  return (await resp.json()) as T;
}

export function getAdminView(ctx: ApiContext): Promise<AdminView> {
  return request<AdminView>(ctx, `${LAKEHOUSE}/admin/warehouses`);
}

/** This workspace's dropped tables that can still be restored (ADR 0122 B). */
export function getDroppedTables(ctx: ApiContext): Promise<DroppedTables> {
  return request<DroppedTables>(ctx, `${LAKEHOUSE}/dropped-tables`);
}

/** Owner only. 409 (ApiError) when a table of the same name exists now. */
export function restoreDroppedTable(ctx: ApiContext, id: string): Promise<{ restored: DroppedTable }> {
  return request(ctx, `${LAKEHOUSE}/dropped-tables/${encodeURIComponent(id)}/restore`, "POST");
}

/** A person's display name from core's user directory, or null (unknown, other workspace, or core
 *  unreachable) — callers fall back to showing the raw subject. */
export async function lookupUser(ctx: ApiContext, sub: string): Promise<string | null> {
  try {
    const u = await request<{ displayName?: string; name?: string; preferredUsername?: string }>(ctx, `${CORE}/users/${encodeURIComponent(sub)}`);
    return u.displayName || u.name || u.preferredUsername || null;
  } catch {
    return null;
  }
}
