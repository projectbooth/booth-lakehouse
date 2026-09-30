import { useState } from "react";
import { LakehouseApp, type WorkspaceRole } from "../LakehouseApp";

/**
 * Dev harness only (`npm run dev`): stands in for booth-design's shell so the view can be worked on
 * against a locally running API (vite.config.ts proxies /modules/lakehouse). Paste a real token.
 */
export function DevShell() {
  const [workspace, setWorkspace] = useState("acme");
  const [role, setRole] = useState<WorkspaceRole>("owner");
  const [theme, setTheme] = useState<"light" | "dark">("light");
  const [token, setToken] = useState("");
  return (
    <div className={theme === "dark" ? "min-h-screen bg-slate-950 p-6" : "min-h-screen bg-white p-6"}>
      <div className="mb-6 flex flex-wrap gap-3 text-sm">
        <input className="rounded border px-2 py-1" value={workspace} onChange={(e) => setWorkspace(e.target.value)} placeholder="workspace" />
        <select className="rounded border px-2 py-1" value={role} onChange={(e) => setRole(e.target.value as WorkspaceRole)}>
          <option>owner</option>
          <option>editor</option>
          <option>viewer</option>
        </select>
        <button className="rounded border px-2 py-1" onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>
          theme: {theme}
        </button>
        <input className="w-96 rounded border px-2 py-1" value={token} onChange={(e) => setToken(e.target.value)} placeholder="bearer token" />
      </div>
      <LakehouseApp key={`${workspace}/${role}/${token}`} workspace={workspace} role={role} theme={theme} getAccessToken={() => token || null} />
    </div>
  );
}
