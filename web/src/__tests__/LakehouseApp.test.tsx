import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LakehouseApp } from "../LakehouseApp";

type Call = { url: string; headers: Record<string, string> };

function mockFetch(routes: Record<string, () => Response>) {
  const calls: Call[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url, headers: (init?.headers ?? {}) as Record<string, string> });
      const hit = Object.entries(routes).find(([prefix]) => url.startsWith(prefix));
      return hit ? hit[1]() : new Response("{}", { status: 404 });
    }),
  );
  return calls;
}

const json = (body: unknown, status = 200) => () => new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

const row = (ws: string, over: Record<string, unknown> = {}) => ({
  workspace: ws,
  warehouseName: `booth-ws-${ws}`,
  location: { backendId: "lake", path: "lakehouse" },
  storageRoot: `s3://lake/${ws}-data/lakehouse`,
  createdAt: 1790000000,
  createdBy: "sub-alice",
  credential: { status: "ok", expiresAt: 1790000900 },
  tables: { tables: 4, views: 1, asOf: "2026-09-30T10:00:00Z" },
  ...over,
});

const props = { workspace: "acme", role: "owner" as const, theme: "light" as const, getAccessToken: () => "tok-1" };

beforeEach(() => vi.unstubAllGlobals());
afterEach(() => vi.unstubAllGlobals());

describe("LakehouseApp", () => {
  it("lists the workspace's warehouse through the gateway with a fresh token", async () => {
    const calls = mockFetch({
      "/modules/lakehouse/api/admin/warehouses": json({ scope: "workspace", items: [row("acme")] }),
      "/api/users/": json({ displayName: "Alice A." }),
    });
    render(<LakehouseApp {...props} />);
    expect(await screen.findByText("lake:lakehouse")).toBeInTheDocument();
    expect(screen.getByText("4")).toBeInTheDocument();
    expect(screen.getByText("OK")).toBeInTheDocument();
    expect(await screen.findByText("by Alice A.")).toBeInTheDocument();
    expect(screen.getByText("Workspace acme. Warehouses are read-only here; a workspace owner can restore dropped tables.")).toBeInTheDocument();
    expect(screen.queryByText(/Read-only\./)).not.toBeInTheDocument();
    const call = calls.find((c) => c.url.startsWith("/modules/lakehouse"))!;
    expect(call.headers.Authorization).toBe("Bearer tok-1");
    expect(call.headers["X-Workspace"]).toBe("acme");
  });

  it("shows every workspace in the operator view, with a missing stat as a dash", async () => {
    mockFetch({
      "/modules/lakehouse/api/admin/warehouses": json({ scope: "all", items: [row("beta", { tables: null }), row("acme", { credential: { status: "expired", expiresAt: 1 } })] }),
      "/api/users/": json({}, 404),
    });
    render(<LakehouseApp {...props} />);
    expect(await screen.findByText("Every workspace (operator view). Warehouses are read-only here; a workspace owner can restore dropped tables.")).toBeInTheDocument();
    const cells = screen.getAllByRole("row").slice(1).map((r) => r.firstElementChild?.textContent);
    expect(cells).toEqual(["acme", "beta"]); // sorted
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(screen.getByText("Expired")).toBeInTheDocument();
    expect(screen.getAllByText("by sub-alice").length).toBe(2); // directory miss falls back to the subject
  });

  it("explains an empty workspace without offering to create anything", async () => {
    mockFetch({ "/modules/lakehouse/api/admin/warehouses": json({ scope: "workspace", items: [] }) });
    render(<LakehouseApp {...props} />);
    expect(await screen.findByText(/has no warehouse yet/)).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /create|delete|drop/i })).not.toBeInTheDocument();
  });

  it("asks the server even for a viewer, and shows a 403 as the not-available notice", async () => {
    const calls = mockFetch({
      "/modules/lakehouse/api/admin/warehouses": json({ detail: "the lakehouse admin view needs the editor or owner role in this workspace" }, 403),
    });
    render(<LakehouseApp {...props} role="viewer" />);
    expect(await screen.findByText(/available to a workspace's editors and owners, and to platform operators/)).toBeInTheDocument();
    expect(calls.filter((c) => c.url.startsWith("/modules/lakehouse"))).toHaveLength(1);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument(); // not an error, just not for this person
    expect(screen.queryByRole("button", { name: "Refresh" })).not.toBeInTheDocument();
  });

  it("shows the operator view to a platform operator who is only a viewer here (ADR 0094)", async () => {
    mockFetch({
      "/modules/lakehouse/api/admin/warehouses": json({ scope: "all", items: [row("acme"), row("beta")] }),
      "/api/users/": json({}, 404),
    });
    render(<LakehouseApp {...props} role="viewer" />);
    expect(await screen.findByText(/Every workspace \(operator view\)/)).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(3); // header + two workspaces
    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
  });

  it("reports errors and refreshes on demand", async () => {
    let fail = true;
    mockFetch({
      "/modules/lakehouse/api/admin/warehouses": () =>
        fail ? json({ detail: "Lakekeeper returned 500" }, 500)() : json({ scope: "workspace", items: [row("acme")] })(),
      "/api/users/": json({}, 404),
    });
    render(<LakehouseApp {...props} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("Lakekeeper returned 500");
    fail = false;
    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(screen.getByText("lake:lakehouse")).toBeInTheDocument());
  });

  it("follows the shell's theme", async () => {
    mockFetch({ "/modules/lakehouse/api/admin/warehouses": json({ scope: "workspace", items: [] }) });
    const { container } = render(<LakehouseApp {...props} theme="dark" />);
    expect(container.firstElementChild).toHaveAttribute("data-theme", "dark");
    await screen.findByText(/has no warehouse yet/); // let the load settle inside the test
  });

  describe("dropped tables (ADR 0122 B)", () => {
    const soft = { type: "soft", expirationSeconds: 604800, status: "ok", desired: { type: "soft", expirationSeconds: 604800 } };
    const plays = { id: "t-1", namespace: "nfl", name: "plays", droppedAt: 1790000000, expiresAt: 1790604800 };

    it("says what a drop does before anything is dropped, and lets an owner restore", async () => {
      let restored = false;
      const calls = mockFetch({
        "/modules/lakehouse/api/admin/warehouses": json({ scope: "workspace", items: [row("acme", { deleteProfile: soft })] }),
        "/modules/lakehouse/api/dropped-tables/t-1/restore": () => {
          restored = true;
          return json({ restored: plays })();
        },
        "/modules/lakehouse/api/dropped-tables": () => json({ deleteProfile: soft, items: restored ? [] : [plays] })(),
        "/api/users/": json({}, 404),
      });
      render(<LakehouseApp {...props} />);
      expect(await screen.findByText(/keeps its data for 7 days/)).toBeInTheDocument();
      expect(screen.getByText(/not a backup of the lake bucket/)).toBeInTheDocument();
      expect(await screen.findByText("nfl.plays")).toBeInTheDocument();
      await userEvent.click(screen.getByRole("button", { name: "Restore" }));
      expect(await screen.findByText("Restored nfl.plays.")).toBeInTheDocument();
      expect(await screen.findByText("No dropped tables to restore.")).toBeInTheDocument();
      const post = calls.find((c) => c.url.endsWith("/restore"))!;
      expect(post.headers.Authorization).toBe("Bearer tok-1");
    });

    it("refreshes the warehouse row's table counts after a restore, keeping the panel's message", async () => {
      let restored = false;
      const calls = mockFetch({
        "/modules/lakehouse/api/admin/warehouses": () =>
          json({
            scope: "workspace",
            items: [row("acme", { deleteProfile: soft, tables: restored ? { tables: 5, views: 1, asOf: "x" } : { tables: 4, views: 1, dropped: 1, asOf: "x" } })],
          })(),
        "/modules/lakehouse/api/dropped-tables/t-1/restore": () => {
          restored = true;
          return json({ restored: plays })();
        },
        "/modules/lakehouse/api/dropped-tables": () => json({ deleteProfile: soft, items: restored ? [] : [plays] })(),
        "/api/users/": json({}, 404),
      });
      render(<LakehouseApp {...props} />);
      expect(await screen.findByText(/1 dropped, restorable/)).toBeInTheDocument();
      expect(screen.getByText("4")).toBeInTheDocument();
      await userEvent.click(await screen.findByRole("button", { name: "Restore" }));
      await waitFor(() => expect(screen.queryByText(/dropped, restorable/)).not.toBeInTheDocument());
      expect(screen.getByText("5")).toBeInTheDocument();
      expect(screen.getByText("Restored nfl.plays.")).toBeInTheDocument();
      expect(await screen.findByText("No dropped tables to restore.")).toBeInTheDocument();
      expect(calls.filter((c) => c.url.startsWith("/modules/lakehouse/api/admin/warehouses"))).toHaveLength(2);
    });

    it("shows the 409 when the name is in use again, and hides Restore from non-owners", async () => {
      mockFetch({
        "/modules/lakehouse/api/admin/warehouses": json({ scope: "workspace", items: [row("acme", { deleteProfile: soft })] }),
        "/modules/lakehouse/api/dropped-tables/t-1/restore": json({ detail: "a table with this name exists now; rename or drop it first" }, 409),
        "/modules/lakehouse/api/dropped-tables": json({ deleteProfile: soft, items: [plays] }),
        "/api/users/": json({}, 404),
      });
      const { unmount } = render(<LakehouseApp {...props} />);
      await userEvent.click(await screen.findByRole("button", { name: "Restore" }));
      expect(await screen.findByText(/a table with this name exists now; rename or drop it first/)).toBeInTheDocument();
      unmount();
      render(<LakehouseApp {...props} role="editor" />);
      expect(await screen.findByText("A workspace owner can restore it")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Restore" })).not.toBeInTheDocument();
    });

    it("warns when drops are permanent or the profile couldn't be applied", async () => {
      mockFetch({
        "/modules/lakehouse/api/admin/warehouses": json({
          scope: "workspace",
          items: [row("acme", { deleteProfile: { type: "hard", expirationSeconds: null, status: "error", error: "Lakekeeper refused", desired: soft.desired } })],
        }),
        "/modules/lakehouse/api/dropped-tables": json({ deleteProfile: null, items: [] }),
        "/api/users/": json({}, 404),
      });
      render(<LakehouseApp {...props} />);
      expect(await screen.findByText(/deletes its data within seconds. There is no undo./)).toBeInTheDocument();
      expect(screen.getByText(/restorable for 7 days\) couldn't be applied: Lakekeeper refused/)).toBeInTheDocument();
    });
  });
});
