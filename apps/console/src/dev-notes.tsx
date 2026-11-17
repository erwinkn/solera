import { Agentation } from "agentation"
import { createRoot } from "react-dom/client"

/* Development only (main.tsx loads this under import.meta.env.DEV): a toolbar
 * for leaving notes on the UI. Each note is posted to the dev server, which
 * appends it to apps/console/.ui-notes.jsonl (vite.config.ts). */

function send(event: string, note: unknown) {
  void fetch("/__ui-notes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ event, page: window.location.pathname + window.location.search, note }),
  })
}

export function mountNotes() {
  const host = document.createElement("div")
  document.body.append(host)
  createRoot(host).render(
    <Agentation
      appName="Solera console"
      onAnnotationAdd={(note) => send("add", note)}
      onAnnotationUpdate={(note) => send("update", note)}
      onAnnotationDelete={(note) => send("delete", note)}
      onAnnotationsClear={(notes) => send("clear", notes)}
    />,
  )
}
