import { useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { GitCommitVertical, Inbox } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Empty, ErrorNotice } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import type { Manifest } from "@/lib/types";

export const Route = createFileRoute("/sources")({
  component: SourcesPage,
});

function KeyPreview({ base, name }: { base: string; name: string }) {
  const { data } = useQuery<{ total: number; keys: Record<string, string> }>(
    `${base}/outputs/${name}/keys`,
    4000,
  );
  if (!data) return null;
  const entries = Object.entries(data.keys).slice(0, 8);
  return (
    <div className="rounded-lg bg-muted p-2 font-mono text-xs">
      {entries.map(([key, revision]) => (
        <div key={key} className="flex justify-between gap-4">
          <span className="truncate">{key}</span>
          <span className="text-muted-foreground">{revision.slice(0, 12)}</span>
        </div>
      ))}
      {data.total > entries.length && (
        <div className="text-muted-foreground">… {data.total} keys total</div>
      )}
      {!entries.length && <div className="text-muted-foreground">empty</div>}
    </div>
  );
}

function CommitForm({
  base,
  name,
  keyed,
}: {
  base: string;
  name: string;
  keyed: boolean;
}) {
  const action = useAction();
  const { refresh } = useWorkspace();
  const [version, setVersion] = useState("");
  const [keys, setKeys] = useState("");
  const [upsert, setUpsert] = useState("");
  const [remove, setRemove] = useState("");
  const [formError, setFormError] = useState<string | null>(null);
  async function commit() {
    setFormError(null);
    const body: Record<string, unknown> = {};
    try {
      if (version.trim()) body.version = version.trim();
      if (keys.trim()) body.keys = JSON.parse(keys);
      if (upsert.trim()) body.upsert = JSON.parse(upsert);
      if (remove.trim())
        body.remove = remove
          .split(",")
          .map((k) => k.trim())
          .filter(Boolean);
    } catch {
      setFormError("keys/upsert must be valid JSON");
      return;
    }
    const result = await action.run(() =>
      request(`${base}/sources/${name}/commit`, { body }),
    );
    if (result) {
      setVersion("");
      setKeys("");
      setUpsert("");
      setRemove("");
      refresh();
    }
  }
  return (
    <div className="flex flex-col gap-2">
      {keyed ? (
        <>
          <Label>Full key map (JSON object or list)</Label>
          <Textarea
            rows={2}
            className="font-mono text-xs"
            placeholder='{"u-1": "v1"}'
            aria-label={`${name} keys`}
            value={keys}
            onChange={(e) => setKeys(e.target.value)}
          />
          <Label>Patch — upsert (JSON)</Label>
          <Textarea
            rows={2}
            className="font-mono text-xs"
            placeholder='{"u-2": "v4"} or ["u-3"]'
            aria-label={`${name} upsert`}
            value={upsert}
            onChange={(e) => setUpsert(e.target.value)}
          />
          <Label>Remove keys (comma-separated)</Label>
          <Input
            className="font-mono text-xs"
            placeholder="u-0"
            aria-label={`${name} remove`}
            value={remove}
            onChange={(e) => setRemove(e.target.value)}
          />
        </>
      ) : (
        <>
          <Label>Version</Label>
          <Input
            className="font-mono text-xs"
            placeholder="2026-09-20T00:00Z"
            aria-label={`${name} version`}
            value={version}
            onChange={(e) => setVersion(e.target.value)}
          />
        </>
      )}
      {(formError || action.error) && (
        <ErrorNotice message={formError ?? action.error!} />
      )}
      <Button
        size="sm"
        className="self-start"
        onClick={commit}
        disabled={action.pending}
      >
        <GitCommitVertical /> Commit
      </Button>
    </div>
  );
}

function SourcesPage() {
  const { base, diagnostics } = useWorkspace();
  const manifest = useQuery<Manifest>(base ? `${base}/manifest` : null, 30000);
  if (!diagnostics) return null;
  const sources = Object.values(manifest.data?.sources ?? {});
  return (
    <section className="flex flex-col gap-4">
      <div>
        <div className="text-[0.65rem] font-medium tracking-wider text-muted-foreground uppercase">
          Ingress
        </div>
        <h1 className="font-heading text-xl font-medium">Sources</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Externally advanced outputs — pushed through the commit API.
        </p>
      </div>
      {!sources.length ? (
        <Empty title="No sources declared" />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {sources.map((source) => (
            <Card key={source.name} data-source={source.name}>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 font-mono text-sm">
                  <Inbox className="size-4" />
                  {source.name}
                </CardTitle>
                <CardDescription>
                  {source.store} ·{" "}
                  {source.key === "<elements>"
                    ? "partition set"
                    : source.key
                      ? `keyed by ${source.key}`
                      : "unkeyed"}{" "}
                  · v
                  <span className="font-mono">
                    {source.head.version.slice(0, 10)}
                  </span>
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col gap-3">
                {source.key && base && (
                  <KeyPreview base={base} name={source.name} />
                )}
                {base && (
                  <CommitForm
                    base={base}
                    name={source.name}
                    keyed={!!source.key}
                  />
                )}
              </CardContent>
            </Card>
          ))}
        </div>
      )}
    </section>
  );
}
