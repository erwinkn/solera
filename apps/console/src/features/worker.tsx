import type { Attempt, Manifest } from "@/api/types";

/**
 * Where an attempt ran, as its executor names it (D170: "Worker"): an ECS
 * task, a Kubernetes job, a Modal call. Linked out when the identifier holds
 * enough to build a URL; nothing for workers that run inline, locally or in
 * a pool, whose identifier means nothing outside the engine.
 */
export function workerOf(attempt: Attempt, manifest: Manifest): { label: string; href?: string } | null {
  const handle = attempt.handle;
  if (!handle || !attempt.executor) return null;
  const executor = manifest.executors[attempt.executor];
  switch (executor?.kind) {
    case "AWSECS": {
      const arn = typeof handle.task_arn === "string" ? handle.task_arn : null;
      if (!arn) return null;
      // arn:aws:ecs:REGION:ACCOUNT:task/CLUSTER/ID
      const [, , , region, , resource] = arn.split(":");
      const [, cluster, id] = (resource ?? "").split("/");
      if (!region || !cluster || !id) return { label: arn };
      return {
        label: `ECS task ${id}`,
        href: `https://${region}.console.aws.amazon.com/ecs/v2/clusters/${cluster}/tasks/${id}?region=${region}`,
      };
    }
    case "K8sJob": {
      const job = typeof handle.job === "string" ? handle.job : null;
      const namespace = executor.config.namespace;
      return job ? { label: `job ${typeof namespace === "string" ? `${namespace}/` : ""}${job}` } : null;
    }
    case "Modal":
      return typeof handle.call_id === "string" ? { label: `Modal call ${handle.call_id}` } : null;
    case "Local":
    case "Inline":
    case "Pool":
      return null;
    default: {
      // An executor the console doesn't know: its first identifier, as given.
      const first = Object.values(handle).find((v) => typeof v === "string");
      return typeof first === "string" ? { label: first } : null;
    }
  }
}
