import { useState } from "react";
import { createFileRoute, useLocation } from "@tanstack/react-router";
import { GitCommitVertical, Inbox } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Empty, ErrorNotice, PageHeader } from "@/components/common";
import { request, useAction, useQuery } from "@/lib/api";
import { useWorkspace } from "@/lib/workspace";
import { cn } from "cn";
import type { Manifest, SourceDecl } from "@/lib/types";

export const Route = createFileRoute("/sources")({
  component: SourcesPage,
});

function sourceKind(source: SourceDecl) {
  if (source.key === "<elements>") return "partition set";
  return source.key ? `keyed by ${source.key}` : "unkeyed value";
}

function KeyPreview({ base, name }: { base: string; name: string }) {
  const { data } = useQuery<{ total: number; keys: Record<string, string> }>(
    `${base}/outputs/${name}/keys`,
    4000,
  );
  if (!data) return null;
  const entries = Object.entries(data.keys).slice(0, 6);
  return (
    <div className="flex flex-col gap-1 rounded-lg border bg-muted/30 p-2.5 font-mono text-xs">
      {entries.map(([key, revision]) => (
        <div key={key} className="flex justify-between gap-4">
          <span className="truncate">{key}</span>
          <span className="text-muted-foreground">{revision.slice(0, 12)}</span>
        </div>
      ))}
      {data.total > entries.length && (
        <div className="text-muted-foreground tabular-nums">
          … {data.total} keys total
        </div>
      )}
      {!entries.length && <div className="text-muted-foreground">empty</div>}
    </div>
  );
}

function CommitForm({
  base,
  name,
  keyed,
  partitionSet,
}: {
  base: string;
  name: string;
  keyed: boolean;
  partitionSet: boolean;
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
    <div className="flex flex-col gap-2.5">
      {keyed ? (
        <>
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs">
              {partitionSet ? "Full key set" : "Full key map"} (JSON)
            </Label>
            <Textarea
              rows={2}
              className="font-mono text-xs"
              placeholder={partitionSet ? '["u-1", "u-2"]' : '{"f1": "v1"}'}
              aria-label={`${name} keys`}
              value={keys}
              onChange={(e) => setKeys(e.target.value)}
            />
          </div>
          <div className="grid grid-cols-2 gap-2.5">
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs">Patch — upsert (JSON)</Label>
              <Textarea
                rows={2}
                className="font-mono text-xs"
                placeholder={partitionSet ? '["u-3"]' : '{"f2": "v4"}'}
                aria-label={`${name} upsert`}
                value={upsert}
                onChange={(e) => setUpsert(e.target.value)}
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs">Remove keys (comma-sep)</Label>
              <Input
                className="font-mono text-xs"
                placeholder="u-0"
                aria-label={`${name} remove`}
                value={remove}
                onChange={(e) => setRemove(e.target.value)}
              />
            </div>
          </div>
        </>
      ) : (
        <div className="flex flex-col gap-1.5">
          <Label className="text-xs">New version</Label>
          <Input
            className="font-mono text-xs"
            placeholder="2026-09-20T00:00Z"
            aria-label={`${name} version`}
            value={version}
            onChange={(e) => setVersion(e.target.value)}
          />
        </div>
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
  // Lineage-graph source nodes link here with a `#source-<name>` hash; the
  // router scrolls the matching card into view, so highlight it as well.
  const hash = useLocation({ select: (location) => location.hash });
  if (!diagnostics) return null;
  const sources = Object.values(manifest.data?.sources ?? {});
  return (
    <section className="flex flex-col gap-4">
      <PageHeader
        eyebrow="Ingress"
        title="Sources"
        description="Externally advanced outputs — pushed through the commit API."
      />
      {!sources.length ? (
        <Empty title="No sources declared" />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {sources.map((source) => (
            <div
              key={source.name}
              id={`source-${source.name}`}
              data-source={source.name}
              className={cn(
                "flex flex-col gap-3 rounded-xl border bg-card p-4",
                hash === `source-${source.name}` &&
                  "border-primary ring-1 ring-primary",
              )}
            >
              <div className="flex items-center gap-2">
                <Inbox className="size-4 text-primary" />
                <span className="font-mono text-sm font-semibold">
                  {source.name}
                </span>
                <span className="ml-auto flex items-center gap-1 text-xs text-muted-foreground">
                  {source.store} · {sourceKind(source)} · v
                  <span className="font-mono">
                    {source.head.version.slice(0, 8)}
                  </span>
                </span>
              </div>
              {source.key && base && (
                <KeyPreview base={base} name={source.name} />
              )}
              {base && (
                <CommitForm
                  base={base}
                  name={source.name}
                  keyed={!!source.key}
                  partitionSet={source.key === "<elements>"}
                />
              )}
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
