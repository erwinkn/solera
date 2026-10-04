import { cn } from "@/lib/cn";

/**
 * A solera: casks stacked in tiers, the youngest on top, wine drawn from the
 * bottom row and refilled from the one above. Drawn once with theme tokens;
 * the theme decides how loud it is (--art-opacity, --art-saturate).
 */
export function Barrels({ className, title }: { className?: string; title?: string }) {
  const cask = (x: number, y: number, fill: string, key: string) => (
    <g key={key} transform={`translate(${x} ${y})`}>
      <ellipse cx="0" cy="0" rx="17" ry="17" fill={fill} stroke="var(--art-1)" strokeWidth="2" />
      <ellipse
        cx="0"
        cy="0"
        rx="11"
        ry="11"
        fill="none"
        stroke="var(--art-1)"
        strokeWidth="1.2"
        opacity="0.45"
      />
      <path d="M-17 0h34M0-17v34" stroke="var(--art-1)" strokeWidth="1" opacity="0.25" />
      <circle cx="0" cy="0" r="2.4" fill="var(--art-1)" />
    </g>
  );
  return (
    <svg
      viewBox="0 0 160 110"
      role={title ? "img" : undefined}
      aria-hidden={title ? undefined : true}
      className={cn("art", className)}
    >
      {title && <title>{title}</title>}
      <path d="M8 102h144" stroke="var(--art-1)" strokeWidth="2" strokeLinecap="round" />
      {[44, 80, 116].map((x, i) => cask(x, 83, "var(--art-2)", `b${i}`))}
      {[62, 98].map((x, i) => cask(x, 50, "var(--art-3)", `m${i}`))}
      {cask(80, 17, "var(--surface)", "t")}
      <path
        d="M131 92c3 4 4 7 4 9"
        stroke="var(--art-1)"
        strokeWidth="1.5"
        fill="none"
        strokeLinecap="round"
        opacity="0.6"
      />
    </svg>
  );
}
