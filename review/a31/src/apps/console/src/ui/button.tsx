import type { ComponentProps, ReactNode } from "react";
import { cn } from "@/lib/cn";

type Variant = "primary" | "secondary" | "ghost" | "danger";
type Size = "sm" | "md";

const base =
  "inline-flex shrink-0 items-center justify-center gap-1.5 rounded-sm font-medium whitespace-nowrap select-none " +
  "disabled:opacity-50 disabled:pointer-events-none [&_svg]:size-3.5 [&_svg]:shrink-0";

const variants: Record<Variant, string> = {
  primary: "pressable bg-accent text-accent-fg hover:bg-accent-hover border-theme border-line-strong",
  secondary: "pressable bg-surface text-fg hover:bg-surface-2 border-theme border-line-strong",
  ghost: "text-fg-muted hover:text-fg hover:bg-accent-soft motion-1 transition-colors",
  danger: "pressable bg-surface text-fail-fg hover:bg-fail-soft border-theme border-line-strong",
};

const sizes: Record<Size, string> = {
  sm: "h-7 px-2.5 text-xs",
  md: "h-8 px-3 text-sm",
};

export function buttonClass(variant: Variant = "secondary", size: Size = "md", className?: string) {
  return cn(base, variants[variant], sizes[size], className);
}

export interface ButtonProps extends ComponentProps<"button"> {
  variant?: Variant;
  size?: Size;
  icon?: ReactNode;
}

export function Button({ variant, size, icon, className, children, type = "button", ...props }: ButtonProps) {
  return (
    <button type={type} className={buttonClass(variant, size, className)} {...props}>
      {icon}
      {children}
    </button>
  );
}

export function IconButton({
  label,
  className,
  children,
  type = "button",
  ...props
}: ComponentProps<"button"> & { label: string }) {
  return (
    <button
      type={type}
      aria-label={label}
      title={label}
      className={cn(
        "inline-grid size-7 shrink-0 place-items-center rounded-sm text-fg-muted motion-1 transition-colors",
        "hover:bg-accent-soft hover:text-fg [&_svg]:size-4",
        className,
      )}
      {...props}
    >
      {children}
    </button>
  );
}
