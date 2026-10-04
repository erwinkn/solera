import type { PatternSpec, Patterns } from "@/api/types";
import { cn } from "@/lib/cn";

const text = (spec: PatternSpec) => (spec.regex ? `/${spec.regex}/` : (spec.glob ?? JSON.stringify(spec)));

/**
 * An input's key patterns (docs/per-key-processing.md §11): includes as they
 * are, excludes with the name `explain` cites. Globs match key segments:
 * `**` crosses `/`, `*` does not.
 */
export function PatternList({ patterns, className }: { patterns: Patterns; className?: string }) {
  const include = patterns.include ?? [];
  const exclude = patterns.exclude ?? [];
  if (!include.length && !exclude.length) return null;
  return (
    <span className={cn("inline-flex flex-wrap items-center gap-1.5 text-xs", className)}>
      {include.map((spec, i) => (
        <code key={`i${i}`} className="rounded-xs bg-ok-soft px-1.5 py-0.5 text-ok-fg" title="include">
          + {text(spec)}
        </code>
      ))}
      {exclude.map(([name, spec], i) => (
        <code
          key={`e${i}`}
          className="rounded-xs bg-idle-soft px-1.5 py-0.5 text-idle-fg"
          title={name ? `exclude "${name}"` : "exclude"}
        >
          − {name ? `${name}: ` : ""}
          {text(spec)}
        </code>
      ))}
    </span>
  );
}
