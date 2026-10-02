import type { ComponentProps } from "react";
import { cn } from "@/lib/cn";

/**
 * Dense tables: one line per thing, tabular numerals, hairline dividers, a
 * row hover that never shifts layout. Wrap in <TableScroll> so a narrow
 * screen scrolls the table, not the page.
 */
export function TableScroll({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("min-w-0 overflow-x-auto", className)} {...props} />;
}

export function Table({ className, ...props }: ComponentProps<"table">) {
  return <table className={cn("w-full border-collapse text-sm tabular", className)} {...props} />;
}

export function Th({ className, ...props }: ComponentProps<"th">) {
  return (
    <th
      scope="col"
      className={cn(
        "h-8 border-b border-line px-3 text-left text-2xs font-medium tracking-wide whitespace-nowrap text-fg-subtle uppercase first:pl-4 last:pr-4",
        className,
      )}
      {...props}
    />
  );
}

export function Td({ className, ...props }: ComponentProps<"td">) {
  return (
    <td
      className={cn("h-9 border-b border-line px-3 align-middle first:pl-4 last:pr-4", className)}
      {...props}
    />
  );
}

export function Tr({ className, ...props }: ComponentProps<"tr">) {
  return (
    <tr
      className={cn("motion-1 transition-colors hover:bg-surface-2 [&:last-child>td]:border-b-0", className)}
      {...props}
    />
  );
}
