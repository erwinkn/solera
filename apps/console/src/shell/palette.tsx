import { useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, type NavigateOptions } from "@tanstack/react-router";
import { Dialog as BaseDialog } from "@base-ui/react/dialog";
import {
  Activity,
  Boxes,
  CalendarClock,
  CornerDownLeft,
  Database,
  LayoutDashboard,
  Radar,
  Search,
} from "lucide-react";
import { q } from "@/api/queries";
import { cn } from "@/lib/cn";
import { palette, usePalette } from "@/lib/palette";
import { Kbd } from "@/ui/data";

interface Item {
  id: string;
  label: string;
  hint: string;
  icon: ReactNode;
  go: NavigateOptions;
}

const PAGES: Item[] = [
  { id: "p:overview", label: "Overview", hint: "page", icon: <LayoutDashboard />, go: { to: "/" } },
  { id: "p:assets", label: "Assets", hint: "page", icon: <Boxes />, go: { to: "/assets" } },
  { id: "p:runs", label: "Runs", hint: "page", icon: <Activity />, go: { to: "/runs" } },
  {
    id: "p:failed",
    label: "Failed runs",
    hint: "runs",
    icon: <Activity />,
    go: { to: "/runs", search: { status: "failed" } },
  },
  {
    id: "p:automations",
    label: "Automations",
    hint: "page",
    icon: <CalendarClock />,
    go: { to: "/automations" },
  },
  { id: "p:sensors", label: "Sensors", hint: "page", icon: <Radar />, go: { to: "/sensors" } },
  { id: "p:sources", label: "Sources", hint: "page", icon: <Database />, go: { to: "/sources" } },
  { id: "p:executors", label: "Executors", hint: "page", icon: <Boxes />, go: { to: "/executors" } },
  { id: "p:health", label: "Health", hint: "page", icon: <LayoutDashboard />, go: { to: "/health" } },
];

const ULID = /^[0-9A-HJKMNP-TV-Z]{26}$/i;

/** Jump anywhere by name: pages, assets, sources, sensors, automations, or a run id. */
export function Palette() {
  const open = usePalette();
  return (
    <BaseDialog.Root open={open} onOpenChange={palette.set}>
      <BaseDialog.Portal>
        <BaseDialog.Backdrop className="fixed inset-0 z-40 bg-overlay transition-opacity motion-2 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
        <BaseDialog.Popup
          aria-label="Jump to"
          className={cn(
            "fixed top-[14vh] left-1/2 z-50 flex w-[min(560px,calc(100vw-2rem))] -translate-x-1/2 flex-col overflow-hidden",
            "rounded-lg border-theme border-line-strong bg-surface shadow-3 outline-none transition-[transform,opacity] motion-2",
            "data-[starting-style]:scale-95 data-[starting-style]:opacity-0 data-[ending-style]:opacity-0",
          )}
        >
          {open && <Finder />}
        </BaseDialog.Popup>
      </BaseDialog.Portal>
    </BaseDialog.Root>
  );
}

function Finder() {
  const navigate = useNavigate();
  const [text, setText] = useState("");
  const [active, setActive] = useState(0);
  const diagnostics = useQuery(q.diagnostics()).data;
  const manifest = useQuery({
    ...q.manifest(diagnostics?.project ?? "", diagnostics?.deploy ?? ""),
    enabled: !!diagnostics,
  }).data;

  const named: Item[] = manifest
    ? [
        ...Object.keys(manifest.assets).map((a) => ({
          id: `a:${a}`,
          label: a,
          hint: "asset",
          icon: <Boxes />,
          go: { to: "/assets/$asset", params: { asset: a } } as NavigateOptions,
        })),
        ...Object.keys(manifest.sources).map((s) => ({
          id: `s:${s}`,
          label: s,
          hint: "source",
          icon: <Database />,
          go: { to: "/sources/$source", params: { source: s } } as NavigateOptions,
        })),
        ...Object.keys(manifest.sensors).map((s) => ({
          id: `x:${s}`,
          label: s,
          hint: "sensor",
          icon: <Radar />,
          go: { to: "/sensors/$sensor", params: { sensor: s } } as NavigateOptions,
        })),
        ...Object.keys(manifest.automations).map((a) => ({
          id: `t:${a}`,
          label: a,
          hint: "automation",
          icon: <CalendarClock />,
          go: { to: "/automations", search: { q: a } } as NavigateOptions,
        })),
      ]
    : [];
  const needle = text.trim().toLowerCase();
  const run: Item[] = ULID.test(text.trim())
    ? [
        {
          id: "r",
          label: text.trim().toUpperCase(),
          hint: "run",
          icon: <Activity />,
          go: { to: "/runs/$run", params: { run: text.trim().toUpperCase() } },
        },
      ]
    : [];
  const items = [
    ...run,
    ...(needle ? [...PAGES, ...named].filter((i) => i.label.toLowerCase().includes(needle)) : PAGES),
  ]
    .sort(
      (a, b) =>
        Number(!a.label.toLowerCase().startsWith(needle)) - Number(!b.label.toLowerCase().startsWith(needle)),
    )
    .slice(0, 12);
  const current = Math.min(active, items.length - 1);

  const go = (item: Item | undefined) => {
    if (!item) return;
    palette.set(false);
    navigate(item.go);
  };

  return (
    <>
      <div className="flex items-center gap-2.5 border-b border-line px-4">
        <Search aria-hidden className="size-4 text-fg-subtle" />
        <input
          autoFocus
          role="combobox"
          aria-expanded
          aria-controls="palette-list"
          aria-activedescendant={items[current] ? `palette-${items[current].id}` : undefined}
          aria-label="Jump to"
          placeholder="Jump to an asset, source, sensor, page or run id…"
          value={text}
          onChange={(e) => {
            setText(e.target.value);
            setActive(0);
          }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") setActive((current + 1) % Math.max(items.length, 1));
            else if (e.key === "ArrowUp") setActive((current - 1 + items.length) % Math.max(items.length, 1));
            else if (e.key === "Enter") go(items[current]);
            else return;
            e.preventDefault();
          }}
          className="h-12 flex-1 bg-transparent text-base text-fg outline-none placeholder:text-fg-subtle"
        />
        <Kbd>esc</Kbd>
      </div>
      <ul
        id="palette-list"
        role="listbox"
        aria-label="Destinations"
        className="max-h-80 overflow-y-auto p-1.5"
      >
        {items.length === 0 && (
          <li className="px-3 py-6 text-center text-sm text-fg-subtle">Nothing by that name</li>
        )}
        {items.map((item, i) => (
          <li
            key={item.id}
            id={`palette-${item.id}`}
            role="option"
            aria-selected={i === current}
            onPointerMove={() => setActive(i)}
            onClick={() => go(item)}
            className={cn(
              "flex h-9 cursor-pointer items-center gap-3 rounded-sm px-3 text-sm [&>svg]:size-4 [&>svg]:shrink-0",
              i === current ? "bg-accent-soft text-fg" : "text-fg-muted",
            )}
          >
            {item.icon}
            <span
              className={cn(
                "flex-1 truncate",
                item.hint !== "page" && item.hint !== "runs" && "font-mono text-xs",
              )}
            >
              {item.label}
            </span>
            <span className="text-2xs text-fg-subtle">{item.hint}</span>
            {i === current && <CornerDownLeft aria-hidden className="size-3.5 text-fg-subtle" />}
          </li>
        ))}
      </ul>
    </>
  );
}
