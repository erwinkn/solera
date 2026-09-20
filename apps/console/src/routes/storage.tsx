import { createFileRoute } from "@tanstack/react-router";
import { Database, FolderTree } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { useWorkspace } from "@/lib/workspace";

export const Route = createFileRoute("/storage")({
  component: StoragePage,
});

function StoragePage() {
  const { diagnostics } = useWorkspace();
  if (!diagnostics) return null;
  return (
    <section className="flex flex-col gap-4">
      <div>
        <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
          State
        </div>
        <h1 className="font-heading text-xl font-medium">Storage</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Durable state and object storage backing this deployment.
        </p>
      </div>
      <div className="grid gap-4 md:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 font-mono text-sm">
              <Database className="size-4" />
              Durable state
            </CardTitle>
            <CardDescription>Heads, cursors, tasks, leases</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 text-sm">
            <div className="flex justify-between gap-4">
              <span className="text-muted-foreground">Backend</span>
              <span className="font-mono text-xs">{diagnostics.backend}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-muted-foreground">URL</span>
              <span className="truncate font-mono text-xs">
                {diagnostics.state}
              </span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-muted-foreground">Postgres store</span>
              <span className="font-mono text-xs">
                {diagnostics.postgres ? "available" : "not configured"}
              </span>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 font-mono text-sm">
              <FolderTree className="size-4" />
              Objects
            </CardTitle>
            <CardDescription>Attempt specs, results, and logs</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 text-sm">
            <div className="flex justify-between gap-4">
              <span className="text-muted-foreground">URL</span>
              <span className="truncate font-mono text-xs">
                {diagnostics.objects}
              </span>
            </div>
            <div className="flex justify-between gap-4">
              <span className="text-muted-foreground">Project</span>
              <span className="font-mono text-xs">{diagnostics.project}</span>
            </div>
          </CardContent>
        </Card>
      </div>
    </section>
  );
}
