import "./styles/base.css"
import "./styles/ui.css"
import "./styles/timeline.css"
import "./styles/blocks.css"
import "./styles/shell.css"
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import { App } from "./App"
import { paint } from "./theme/store"

paint()

const root = createRoot(document.getElementById("root")!)
root.render(
  <StrictMode>
    <App />
  </StrictMode>,
)

// Development only: leave notes on the UI (see dev-notes.tsx). The condition
// is a build-time constant, so none of it reaches the bundle.
if (import.meta.env.DEV) {
  void import("./dev-notes").then(({ mountNotes }) => mountNotes())
}
