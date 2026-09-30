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
    expect(await screen.findByText(/Every workspace \(operator view\)/)).toBeInTheDocument();
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

  it("doesn't call the API for a viewer", async () => {
    const calls = mockFetch({});
    render(<LakehouseApp {...props} role="viewer" />);
    expect(screen.getByText(/available to a workspace's editors and owners/)).toBeInTheDocument();
    expect(calls).toHaveLength(0);
  });

  it("reports errors and refreshes on demand", async () => {
    let fail = true;
    mockFetch({
      "/modules/lakehouse/api/admin/warehouses": () =>
        fail ? json({ detail: "the lakehouse admin view needs the editor or owner role" }, 403)() : json({ scope: "workspace", items: [row("acme")] })(),
      "/api/users/": json({}, 404),
    });
    render(<LakehouseApp {...props} />);
    expect(await screen.findByRole("alert")).toHaveTextContent("needs the editor or owner role");
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
});
