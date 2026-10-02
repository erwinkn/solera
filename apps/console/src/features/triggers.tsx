import type { Trigger } from "@/api/types";
import { interval } from "@/lib/format";

const DAYS = ["Sundays", "Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays"];

/** A cron expression in words, for the shapes people actually write; else the expression. */
export function describeCron(expression: string, timezone = "UTC"): string {
  const parts = expression.trim().split(/\s+/);
  if (parts.length !== 5) return expression;
  const [min, hour, dom, mon, dow] = parts as [string, string, string, string, string];
  const tz = timezone === "UTC" ? " UTC" : ` ${timezone}`;
  const at = (h: string, m: string) => `${h.padStart(2, "0")}:${m.padStart(2, "0")}${tz}`;
  const num = /^\d+$/;
  if (dom === "*" && mon === "*" && dow === "*") {
    if (min === "*" && hour === "*") return "every minute";
    if (min.startsWith("*/") && hour === "*") return `every ${min.slice(2)} minutes`;
    if (num.test(min) && hour === "*") return `hourly at :${min.padStart(2, "0")}`;
    if (num.test(min) && num.test(hour)) return `daily at ${at(hour, min)}`;
  }
  if (dom === "*" && mon === "*" && num.test(dow) && num.test(min) && num.test(hour)) {
    return `${DAYS[Number(dow) % 7]} at ${at(hour, min)}`;
  }
  return `cron ${expression}`;
}

export function describeTrigger(trigger: Trigger, watched: string[] = []): string {
  switch (trigger.kind) {
    case "every":
      return `every ${interval(trigger.seconds)}`;
    case "cron":
      return describeCron(trigger.expression, trigger.timezone);
    case "onchange": {
      const outputs = trigger.outputs.length ? trigger.outputs : watched;
      return outputs.length ? `on change of ${outputs.join(", ")}` : "on change of its inputs";
    }
    case "ondeploy":
      return "on deploy";
  }
}
