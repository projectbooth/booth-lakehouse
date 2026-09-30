// Public entry point for @projectbooth/lakehouse-ui (ADR 0030, ADR 0093). Consumers also import the
// stylesheet once: `@projectbooth/lakehouse-ui/dist/style.css` (not auto-injected, same as every other
// native module's package).
import "./library.css";

export { LakehouseApp } from "./LakehouseApp";
export type { LakehouseAppProps, WorkspaceRole } from "./LakehouseApp";
