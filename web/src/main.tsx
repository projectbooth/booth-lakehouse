import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { DevShell } from "./devshell/DevShell";
import "./styles.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <DevShell />
  </StrictMode>,
);
