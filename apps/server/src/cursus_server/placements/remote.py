"""Remote placements (§10 built-ins): AWSECS, K8sJob, Modal.

Each lazily imports its SDK so the server runs without the optional extras
installed; a missing SDK raises at `launch`, which the engine records as a
retryable attempt failure. `launch` hands the harness the two stage strings —
`attempt` and `objects` — via container override, argv, or function argument.
"""

from __future__ import annotations

import asyncio
import contextlib
import time


class AWSECS:
    """`AWSECS(cluster, region)(cpu, memory, gpu, image)` — one ECS task per attempt."""

    max_concurrent = None

    def __init__(self, environment: dict, options: dict, ctx):
        self.environment, self.options, self.ctx = environment, options, ctx

    def _client(self):
        import boto3

        return boto3.client("ecs", region_name=self.environment["region"])

    def _overrides(self, stage: dict) -> dict:
        command = [
            "python",
            "-m",
            "cursus_worker",
            "run",
            "--objects",
            stage["objects"],
            "--attempt",
            stage["attempt"],
            "--run",
            stage["run"],
        ]
        override: dict = {"name": "worker", "command": command}
        if self.options.get("cpu") is not None:
            override["cpu"] = str(self.options["cpu"] * 1024)
        if self.options.get("memory") is not None:
            override["memory"] = str(self.options["memory"] // 10**6)
        if self.options.get("gpu") is not None:
            override["resourceRequirements"] = [{"type": "GPU", "value": str(self.options["gpu"])}]
        return {"containerOverrides": [override]}

    async def launch(self, stage: dict) -> dict:
        client = self._client()
        response = await asyncio.to_thread(
            client.run_task,
            cluster=self.environment["cluster"],
            taskDefinition=self.options.get("image") or "cursus-worker",
            overrides=self._overrides(stage),
            launchType="FARGATE",
        )
        failures = response.get("failures") or []
        if failures:
            raise RuntimeError(f"run_task failed: {failures[0].get('reason')}")
        return {"task_arn": response["tasks"][0]["taskArn"]}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        client = self._client()
        deadline = time.monotonic() + timeout
        while True:
            response = await asyncio.to_thread(
                client.describe_tasks,
                cluster=self.environment["cluster"],
                tasks=[run["task_arn"]],
            )
            tasks = response.get("tasks") or []
            if not tasks:
                return {"code": None, "reason": "lost", "meta": {}}
            task = tasks[0]
            if task.get("lastStatus") == "STOPPED":
                containers = task.get("containers") or [{}]
                meta = {}
                if containers[0].get("logUrl"):
                    meta["log_url"] = containers[0]["logUrl"]
                return {
                    "code": containers[0].get("exitCode"),
                    "reason": task.get("stoppedReason"),
                    "meta": meta,
                }
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(5.0, deadline - time.monotonic()))

    async def cancel(self, run: dict) -> None:
        client = self._client()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                client.stop_task,
                cluster=self.environment["cluster"],
                task=run["task_arn"],
                reason="canceled by cursus",
            )


class K8sJob:
    """`K8sJob(cluster, namespace)(cpu, memory, image)` — one Job per attempt."""

    max_concurrent = None

    def __init__(self, environment: dict, options: dict, ctx):
        self.environment, self.options, self.ctx = environment, options, ctx

    def _clients(self):
        from kubernetes import client as kclient

        return kclient, kclient.BatchV1Api()

    def _job(self, stage: dict):
        kclient, _ = self._clients()
        resources = {}
        if self.options.get("cpu") is not None:
            resources["cpu"] = str(self.options["cpu"])
        if self.options.get("memory") is not None:
            resources["memory"] = str(self.options["memory"])
        container = kclient.V1Container(
            name="worker",
            image=self.options.get("image") or "cursus-worker",
            args=["run", "--objects", stage["objects"], "--attempt", stage["attempt"], "--run", stage["run"]],
            resources=kclient.V1ResourceRequirements(limits=resources) if resources else None,
        )
        spec = kclient.V1JobSpec(
            backoff_limit=0,
            template=kclient.V1PodTemplateSpec(
                spec=kclient.V1PodSpec(restart_policy="Never", containers=[container])
            ),
        )
        return kclient.V1Job(
            metadata=kclient.V1ObjectMeta(name=f"cursus-{stage['attempt'][-12:]}".replace("/", "-")),
            spec=spec,
        )

    async def launch(self, stage: dict) -> dict:
        _, api = self._clients()
        job = await asyncio.to_thread(
            api.create_namespaced_job, self.environment["namespace"], self._job(stage)
        )
        return {"job": job.metadata.name}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        _, api = self._clients()
        deadline = time.monotonic() + timeout
        while True:
            try:
                job = await asyncio.to_thread(
                    api.read_namespaced_job, run["job"], self.environment["namespace"]
                )
            except Exception:
                return {"code": None, "reason": "lost", "meta": {}}
            for condition in job.status.conditions or []:
                if condition.type == "Complete" and condition.status == "True":
                    return {"code": 0, "reason": None, "meta": {}}
                if condition.type == "Failed" and condition.status == "True":
                    return {"code": 1, "reason": condition.reason, "meta": {}}
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(2.0, deadline - time.monotonic()))

    async def cancel(self, run: dict) -> None:
        _, api = self._clients()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                api.delete_namespaced_job,
                run["job"],
                self.environment["namespace"],
                propagation_policy="Background",
            )


class Modal:
    """`Modal(app)(gpu)` — spawns the harness as a Modal function call."""

    max_concurrent = None

    def __init__(self, environment: dict, options: dict, ctx):
        self.environment, self.options, self.ctx = environment, options, ctx

    def _function(self):
        import modal

        return modal.Function.from_name(self.environment["app"], "run_attempt")

    async def launch(self, stage: dict) -> dict:
        function = self._function()
        call = await asyncio.to_thread(
            function.spawn, attempt=stage["attempt"], run=stage["run"], objects=stage["objects"]
        )
        return {"call_id": call.object_id}

    async def wait(self, run: dict, timeout: float) -> dict | None:
        import modal

        deadline = time.monotonic() + timeout
        while True:
            try:
                call = modal.functions.FunctionCall.from_id(run["call_id"])
                await asyncio.to_thread(call.get, timeout=0)
                return {"code": 0, "reason": None, "meta": {}}
            except modal.exception.FunctionNotFoundError:
                return {"code": None, "reason": "lost", "meta": {}}
            except TimeoutError:
                if time.monotonic() >= deadline:
                    return None
                await asyncio.sleep(min(2.0, deadline - time.monotonic()))
            except Exception as error:
                return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}

    async def cancel(self, run: dict) -> None:
        import modal

        with contextlib.suppress(Exception):
            call = modal.functions.FunctionCall.from_id(run["call_id"])
            await asyncio.to_thread(call.cancel)
