// UI notes, in development only (main.tsx imports this behind `import.meta.env.DEV`).
// Agentation is the toolbar: click an element, select text or drag an area, and
// write what is wrong. Every note it adds, edits or deletes is posted to the dev
// server, which appends it to apps/console/.ui-notes.jsonl (vite.config.ts) for
// whoever fixes the console to read.
import { Agentation } from "agentation";

const record = (event: string) => (note: unknown) =>
  void fetch("/__ui-notes", {
    method: "POST",
    body: JSON.stringify({ event, page: location.pathname + location.search, note }),
  });

export default function Notes() {
  return (
    <Agentation
      appName="Solera console"
      onAnnotationAdd={record("add")}
      onAnnotationUpdate={record("update")}
      onAnnotationDelete={record("delete")}
      onAnnotationsClear={record("clear")}
    />
  );
}
