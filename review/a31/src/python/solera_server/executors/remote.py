"""Remote placements (§10 built-ins): AWSECS, K8sJob, Modal.

Each lazily imports its SDK so the server runs without the optional extras
installed; a missing SDK raises at `launch`, which the engine records as a
retryable attempt failure. `launch` hands the worker the stage strings —
`attempt`, `run` and `objects` — via container override, argv, or function
argument.

The provider names each attempt's run after the attempt (an ECS client
token, a Kubernetes job name), so launching an attempt twice starts it once:
`resume`, after a restart that lost the handle, is `launch` again. `wait`
reports an exit only when the provider says so; when it cannot tell — an API
error, a run it does not show (yet) — it raises, and the engine keeps the
handle and follows the worker's own reports meanwhile.
"""

from __future__ import annotations

import asyncio
import contextlib
import time


class AWSECS:
    """`AWSECS(cluster, region)(cpu, memory, gpu, image)` — one ECS task per attempt."""

    max_concurrent = None

    def __init__(self, config: dict, options: dict, ctx):
        self.config, self.options, self.ctx = config, options, ctx

    def _client(self):
        import boto3

        return boto3.client("ecs", region_name=self.config["region"])

    def _overrides(self, stage: dict) -> dict:
        command = [
            "python",
            "-m",
            "solera_worker",
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
            cluster=self.config["cluster"],
            taskDefinition=self.options.get("image") or "solera-worker",
            overrides=self._overrides(stage),
            launchType="FARGATE",
            clientToken=stage["attempt"],  # a second launch of the attempt returns the first's task
        )
        failures = response.get("failures") or []
        if failures:
            raise RuntimeError(f"run_task failed: {failures[0].get('reason')}")
        return {"task_arn": response["tasks"][0]["taskArn"]}

    resume = launch

    async def wait(self, handle: dict, timeout: float) -> dict | None:
        client = self._client()
        deadline = time.monotonic() + timeout
        while True:
            response = await asyncio.to_thread(
                client.describe_tasks,
                cluster=self.config["cluster"],
                tasks=[handle["task_arn"]],
            )
            tasks = response.get("tasks") or []
            if not tasks:
                # Not shown yet (ECS is eventually consistent after a launch),
                # or stopped long ago: either way, ECS cannot tell.
                raise LookupError(f"ECS shows no task {handle['task_arn']}")
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

    async def cancel(self, handle: dict) -> None:
        client = self._client()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                client.stop_task,
                cluster=self.config["cluster"],
                task=handle["task_arn"],
                reason="canceled by solera",
            )


class K8sJob:
    """`K8sJob(cluster, namespace)(cpu, memory, image)` — one Job per attempt."""

    max_concurrent = None

    def __init__(self, config: dict, options: dict, ctx):
        self.config, self.options, self.ctx = config, options, ctx

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
            image=self.options.get("image") or "solera-worker",
            args=["run", "--objects", stage["objects"], "--attempt", stage["attempt"], "--run", stage["run"]],
            resources=kclient.V1ResourceRequirements(limits=resources) if resources else None,
        )
        spec = kclient.V1JobSpec(
            backoff_limit=0,
            template=kclient.V1PodTemplateSpec(
                spec=kclient.V1PodSpec(restart_policy="Never", containers=[container])
            ),
        )
        return kclient.V1Job(metadata=kclient.V1ObjectMeta(name=self._name(stage)), spec=spec)

    @staticmethod
    def _name(stage: dict) -> str:
        """The attempt's job: a DNS label, so lowercase."""

        return f"solera-{stage['attempt'].lower()}"

    async def launch(self, stage: dict) -> dict:
        _, api = self._clients()
        try:
            await asyncio.to_thread(api.create_namespaced_job, self.config["namespace"], self._job(stage))
        except Exception as error:
            if getattr(error, "status", None) != 409:
                raise
            # Already there: this attempt's job, from a launch whose answer was lost.
        return {"job": self._name(stage)}

    resume = launch

    async def wait(self, handle: dict, timeout: float) -> dict | None:
        _, api = self._clients()
        deadline = time.monotonic() + timeout
        while True:
            try:
                job = await asyncio.to_thread(
                    api.read_namespaced_job, handle["job"], self.config["namespace"]
                )
            except Exception as error:
                if getattr(error, "status", None) == 404:
                    return {"code": None, "reason": "lost", "meta": {}}
                raise
            for condition in job.status.conditions or []:
                if condition.type == "Complete" and condition.status == "True":
                    return {"code": 0, "reason": None, "meta": {}}
                if condition.type == "Failed" and condition.status == "True":
                    return {"code": 1, "reason": condition.reason, "meta": {}}
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(min(2.0, deadline - time.monotonic()))

    async def cancel(self, handle: dict) -> None:
        _, api = self._clients()
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                api.delete_namespaced_job,
                handle["job"],
                self.config["namespace"],
                propagation_policy="Background",
            )


class Modal:
    """`Modal(app)(gpu)` — spawns the worker as a Modal function call."""

    max_concurrent = None

    def __init__(self, config: dict, options: dict, ctx):
        self.config, self.options, self.ctx = config, options, ctx

    def _function(self):
        import modal

        return modal.Function.from_name(self.config["app"], "run_attempt")

    async def launch(self, stage: dict) -> dict:
        function = self._function()
        call = await asyncio.to_thread(
            function.spawn, attempt=stage["attempt"], run=stage["run"], objects=stage["objects"]
        )
        return {"call_id": call.object_id}

    async def wait(self, handle: dict, timeout: float) -> dict | None:
        """An exit only for what Modal says of the call itself: it returned
        (its code), raised, timed out, or failed inside Modal. Modal's own
        client, service and auth errors say nothing of the call: they raise,
        as does a network error. (Polling with `timeout=0` signals "not done
        yet" with the builtin `TimeoutError`; Modal's own `TimeoutError`
        family is something else.)"""

        import modal

        errors = modal.exception
        ended = (
            errors.FunctionTimeoutError,
            errors.InternalFailure,
            errors.RemoteError,
            errors.ExecutionError,
        )
        deadline = time.monotonic() + timeout
        while True:
            try:
                call = modal.functions.FunctionCall.from_id(handle["call_id"])
                code = await asyncio.to_thread(call.get, timeout=0)
                return {"code": code if isinstance(code, int) else 0, "reason": None, "meta": {}}
            except TimeoutError:
                if time.monotonic() >= deadline:
                    return None
                await asyncio.sleep(min(2.0, deadline - time.monotonic()))
            except (errors.NotFoundError, errors.OutputExpiredError):
                return {"code": None, "reason": "lost", "meta": {}}  # no call, or its outcome is gone
            except ended as error:
                return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}
            except (OSError, errors.Error):
                raise  # no word on the call
            except Exception as error:  # the function itself raised
                return {"code": 1, "reason": f"{type(error).__name__}: {error}", "meta": {}}

    async def cancel(self, handle: dict) -> None:
        import modal

        with contextlib.suppress(Exception):
            call = modal.functions.FunctionCall.from_id(handle["call_id"])
            await asyncio.to_thread(call.cancel)
