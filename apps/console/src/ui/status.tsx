import { Ban, CheckCircle2, CircleDashed, Clock3, LoaderCircle, TriangleAlert, XCircle } from "lucide-react";
import { cn } from "@/lib/cn";
import { label, tone, toneSoft, toneSolid, toneText, type Tone } from "@/lib/status";

const ICONS = {
  ok: CheckCircle2,
  run: LoaderCircle,
  wait: Clock3,
  warn: TriangleAlert,
  fail: XCircle,
  idle: CircleDashed,
} as const;

export function StatusIcon({
  status,
  tone: t,
  className,
}: {
  status?: string | null;
  tone?: Tone;
  className?: string;
}) {
  const resolved = t ?? tone(status);
  const Icon = status === "canceled" ? Ban : ICONS[resolved];
  return (
    <Icon
      aria-hidden
      className={cn(
        "size-3.5 shrink-0",
        toneText[resolved],
        resolved === "run" && "animate-spin [animation-duration:1.6s]",
        className,
      )}
    />
  );
}

/** A status as icon + word on a soft fill. Never color alone. */
export function StatusBadge({
  status,
  text,
  className,
}: {
  status: string | null | undefined;
  text?: string;
  className?: string;
}) {
  const t = tone(status);
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center gap-1 rounded-full pr-2 pl-1.5 text-xs font-medium whitespace-nowrap",
        toneSoft[t],
        className,
      )}
    >
      <StatusIcon status={status} tone={t} className="size-3" />
      {text ?? label(status)}
    </span>
  );
}

export function StatusDot({
  tone: t,
  pulse,
  className,
  title,
}: {
  tone: Tone;
  pulse?: boolean;
  className?: string;
  title?: string;
}) {
  return (
    <span
      title={title}
      aria-hidden={title ? undefined : true}
      className={cn(
        "inline-block size-2 shrink-0 rounded-full",
        toneSolid[t],
        pulse && "animate-[pulse-dot_1.6s_ease-in-out_infinite]",
        className,
      )}
    />
  );
}
