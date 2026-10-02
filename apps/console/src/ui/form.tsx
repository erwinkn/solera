import type { ComponentProps, KeyboardEvent, ReactNode } from "react";
import { Switch as BaseSwitch } from "@base-ui/react/switch";
import { Search } from "lucide-react";
import { cn } from "@/lib/cn";

const control =
  "h-8 min-w-0 rounded-sm border-theme border-line-strong bg-surface px-2.5 text-sm text-fg " +
  "placeholder:text-fg-subtle motion-1 transition-colors hover:border-fg-subtle focus-visible:outline-2 focus-visible:outline-focus";

export function Input({ className, ...props }: ComponentProps<"input">) {
  return <input className={cn(control, className ?? "w-full")} {...props} />;
}

export function SearchInput({ className, ...props }: ComponentProps<"input">) {
  return (
    <div className={cn("relative min-w-0", className)}>
      <Search
        aria-hidden
        className="pointer-events-none absolute top-1/2 left-2.5 size-3.5 -translate-y-1/2 text-fg-subtle"
      />
      <input type="search" className={cn(control, "w-full pl-8")} {...props} />
    </div>
  );
}

export function Textarea({ className, ...props }: ComponentProps<"textarea">) {
  return (
    <textarea
      className={cn(control, "w-full h-auto min-h-20 py-2 font-mono text-xs leading-relaxed", className)}
      {...props}
    />
  );
}

export function Select({ className, children, ...props }: ComponentProps<"select">) {
  return (
    <select
      className={cn(
        control,
        "appearance-none bg-[length:12px] bg-[right_0.5rem_center] bg-no-repeat pr-7",
        className,
      )}
      style={{ backgroundImage: CHEVRON }}
      {...props}
    >
      {children}
    </select>
  );
}
const CHEVRON =
  "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath d='M3 4.5 6 7.5 9 4.5' fill='none' stroke='%23888' stroke-width='1.5'/%3E%3C/svg%3E\")";

export function Field({
  label,
  hint,
  children,
  className,
}: {
  label: ReactNode;
  hint?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <label className={cn("flex flex-col gap-1.5", className)}>
      <span className="text-xs font-medium text-fg-muted">{label}</span>
      {children}
      {hint && <span className="text-xs text-fg-subtle">{hint}</span>}
    </label>
  );
}

export function Switch({
  checked,
  onCheckedChange,
  label,
  disabled,
}: {
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  label: string;
  disabled?: boolean;
}) {
  return (
    <BaseSwitch.Root
      checked={checked}
      onCheckedChange={(value) => onCheckedChange(value)}
      aria-label={label}
      disabled={disabled}
      className={cn(
        "relative inline-flex h-5 w-9 shrink-0 items-center rounded-full border-theme border-line-strong bg-idle-soft p-0.5",
        "motion-2 transition-colors data-[checked]:bg-accent data-[disabled]:opacity-50",
      )}
    >
      <BaseSwitch.Thumb
        className={cn(
          "block size-3.5 rounded-full bg-surface shadow-1 transition-transform motion-3",
          "data-[checked]:translate-x-4",
        )}
      />
    </BaseSwitch.Root>
  );
}

/**
 * Arrow keys (and Home/End) move between the options of a radio group or a
 * tab list, selecting as they go: one tab stop for the whole group.
 */
export function rove(event: KeyboardEvent<HTMLElement>, role: "radio" | "tab") {
  const step: Record<string, number> = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 };
  if (!(event.key in step) && event.key !== "Home" && event.key !== "End") return;
  const items = [...event.currentTarget.querySelectorAll<HTMLElement>(`[role="${role}"]`)];
  const at = items.indexOf(document.activeElement as HTMLElement);
  if (at < 0 || items.length === 0) return;
  const next =
    event.key === "Home"
      ? 0
      : event.key === "End"
        ? items.length - 1
        : (at + step[event.key]! + items.length) % items.length;
  event.preventDefault();
  items[next]!.focus();
  items[next]!.click();
}

/** A row of mutually exclusive options. Buttons, so it works without the URL too. */
export function Segmented<T extends string>({
  value,
  options,
  onChange,
  label,
  size = "md",
}: {
  value: T;
  options: { value: T; label: ReactNode; title?: string }[];
  onChange: (value: T) => void;
  label: string;
  size?: "sm" | "md";
}) {
  return (
    <div
      role="radiogroup"
      aria-label={label}
      onKeyDown={(e) => rove(e, "radio")}
      className="inline-flex shrink-0 rounded-sm border-theme border-line-strong bg-sunken p-0.5"
    >
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          role="radio"
          aria-checked={option.value === value}
          tabIndex={option.value === value ? 0 : -1}
          title={option.title}
          onClick={() => onChange(option.value)}
          className={cn(
            "inline-flex items-center gap-1.5 rounded-xs font-medium whitespace-nowrap text-fg-muted motion-1 transition-colors [&_svg]:size-3.5",
            size === "sm" ? "h-6 px-2 text-xs" : "h-7 px-2.5 text-xs",
            option.value === value ? "bg-surface text-fg shadow-1" : "hover:text-fg",
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

/** A filter chip that toggles; shows a count when given one. */
export function Chip({
  active,
  onClick,
  children,
  count,
  className,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
  count?: number;
  className?: string;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={cn(
        "inline-flex h-7 items-center gap-1.5 rounded-full border-theme px-2.5 text-xs font-medium whitespace-nowrap motion-1 transition-colors",
        active
          ? "border-fg bg-fg text-fg-inverse"
          : "border-line-strong bg-surface text-fg-muted hover:text-fg",
        className,
      )}
    >
      {children}
      {count !== undefined && (
        <span className={cn("tabular", active ? "opacity-80" : "text-fg-subtle")}>{count}</span>
      )}
    </button>
  );
}
