"""Azure Container Apps Sandboxes (microVM sandbox groups), not dynamic sessions.

Designed against azure-containerapps-sandbox 0.1.0b4 and an existing sandbox group.
The identity needs the Container Apps SandboxGroup Data Owner role on the group.
"""

import asyncio
import contextvars
import hashlib
import time
from datetime import datetime, timezone

from azure.containerapps.sandbox import (
    AutoDeletePolicy,
    AutoSuspendPolicy,
    LifecyclePolicy,
    endpoint_for_region,
)
from azure.containerapps.sandbox.aio import SandboxGroupClient
from azure.core.exceptions import AzureError, ClientAuthenticationError, ResourceNotFoundError

from cloudrift.core.exceptions import (
    SandboxError,
    SandboxPermissionError,
    SandboxSessionNotFoundError,
)
from cloudrift.sandbox.base import (
    DEFAULT_READ_CHUNK_BYTES,
    DEFAULT_WRITE_CHUNK_B64,
    ExecResult,
    SandboxBackend,
    SandboxSessionInfo,
)

_DEFAULT_IDLE_SECONDS = 300
_DEFAULT_RETENTION_SECONDS = 30 * 24 * 60 * 60
_RESUMABLE_STATES = frozenset({"stopped", "suspended", "idle"})
_TERMINAL_STATES = frozenset({"failed", "deleting"})


class AzureSandboxesBackend(SandboxBackend):
    """A durable, resumable sandbox in a pre-provisioned Azure sandbox group.

    The group client is async and shared by sandbox-scoped clients. Closing this
    backend closes the local client and credential, never the remote sandboxes.
    """

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        sandbox_group: str,
        region: str,
        credential,
        *,
        disk_image: str = "ubuntu",
        idle_seconds: int = _DEFAULT_IDLE_SECONDS,
        retention_seconds: int = _DEFAULT_RETENTION_SECONDS,
        read_chunk_bytes: int = DEFAULT_READ_CHUNK_BYTES,
        write_chunk_b64: int = DEFAULT_WRITE_CHUNK_B64,
        scope: str = "lyzr-agent",
    ) -> None:
        super().__init__(read_chunk_bytes=read_chunk_bytes, write_chunk_b64=write_chunk_b64)
        if idle_seconds <= 0 or retention_seconds <= 0:
            raise ValueError("idle_seconds and retention_seconds must be positive")
        if not disk_image:
            raise ValueError("disk_image must be a public disk image name")
        self._disk_image = disk_image
        self._idle_seconds = int(idle_seconds)
        self._retention_seconds = int(retention_seconds)
        self._scope = scope
        self._credential = credential
        self._client = SandboxGroupClient(
            endpoint_for_region(region),
            credential,
            subscription_id=subscription_id,
            resource_group=resource_group,
            sandbox_group=sandbox_group,
        )
        # The base class calls self.exec() recursively while preparing a
        # detached command. Keep the extended policy throughout those calls.
        self._active_exec: contextvars.ContextVar[str | None] = contextvars.ContextVar(
            "azure_sandbox_active_exec", default=None
        )
        self._policy_intervals: dict[str, int] = {}
        self._exec_locks: dict[str, asyncio.Lock] = {}

    @classmethod
    def from_managed_identity(
        cls,
        subscription_id: str,
        resource_group: str,
        sandbox_group: str,
        region: str,
        disk_image: str = "ubuntu",
        client_id: str | None = None,
        *,
        idle_seconds: int = _DEFAULT_IDLE_SECONDS,
        retention_seconds: int = _DEFAULT_RETENTION_SECONDS,
        credential_options: dict | None = None,
        **kwargs,
    ) -> "AzureSandboxesBackend":
        """Use Cloudrift's workload identity → managed identity → Azure CLI chain."""
        if idle_seconds <= 0 or retention_seconds <= 0 or not disk_image:
            raise ValueError("idle_seconds, retention_seconds and disk_image must be positive/nonempty")
        from cloudrift.core.azure_credentials import build_async_credential

        credential = build_async_credential(client_id, **(credential_options or {}))
        return cls(
            subscription_id, resource_group, sandbox_group, region, credential,
            disk_image=disk_image, idle_seconds=idle_seconds,
            retention_seconds=retention_seconds, **kwargs,
        )

    async def close(self) -> None:
        try:
            await self._client.close()
        finally:
            await self._credential.close()

    def _sandbox(self, session_id: str):
        return self._client.get_sandbox_client(session_id)

    @staticmethod
    def _missing(exc: AzureError) -> bool:
        return isinstance(exc, ResourceNotFoundError) or getattr(exc, "status_code", None) == 404

    @staticmethod
    def _error(exc: AzureError, session_id: str) -> SandboxError:
        status = getattr(exc, "status_code", None)
        if status in (401, 403) or isinstance(exc, ClientAuthenticationError):
            return SandboxPermissionError(f"access denied to Azure sandbox {session_id!r}: {exc}")
        if AzureSandboxesBackend._missing(exc):
            return SandboxSessionNotFoundError(f"Azure sandbox not found: {session_id}")
        return SandboxError(f"Azure sandbox {session_id!r} request failed: {exc}")

    def _policy(self, idle_seconds: int) -> LifecyclePolicy:
        """Suspend after running idle; delete retention_seconds after Stopped."""
        return LifecyclePolicy(
            auto_suspend=AutoSuspendPolicy(enabled=True, interval=idle_seconds, mode="Disk"),
            auto_delete=AutoDeletePolicy(
                enabled=True, delete_interval_seconds=self._retention_seconds
            ),
        )

    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        """Recover a labeled VM for this logical key, or allocate one.

        Azure assigns the ID. A durable hash label identifies a reservation
        whose create succeeded but whose ID was not published to the caller.
        The caller serializes allocations for each logical key; Azure's label
        lookup alone is not an atomic create-if-absent operation.
        ``timeout_seconds`` is not an absolute TTL: the native idle and
        stopped-retention policies govern lifetime.
        """
        logical_key = hashlib.sha256(key.encode()).hexdigest()
        try:
            candidates = [
                sb async for sb in self._client.list_sandboxes(
                    labels={"scope": self._scope, "logical_key": logical_key}
                )
                if sb.labels.get("scope") == self._scope
                and sb.labels.get("logical_key") == logical_key
            ]
        except AzureError as exc:
            raise self._error(exc, "<group>") from exc
        if len(candidates) > 1:
            raise SandboxError(f"Multiple Azure sandboxes exist for logical key {logical_key}")
        if candidates:
            session_id = candidates[0].id
            if not session_id:
                raise SandboxError("Azure returned a sandbox without an ID")
            if await self.resume_session(session_id):
                await self._sandbox(session_id).set_lifecycle_policy(self._policy(self._idle_seconds))
                return session_id
        try:
            poller = await self._client.begin_create_sandbox(
                disk=self._disk_image,
                auto_suspend_seconds=self._idle_seconds,
                auto_suspend_mode="Disk",
                labels={"scope": self._scope, "logical_key": logical_key},
            )
            sandbox = await poller.result()
        except AzureError as exc:
            raise self._error(exc, "<new>") from exc
        except (TimeoutError, RuntimeError) as exc:
            raise SandboxError(f"Azure sandbox creation did not complete: {exc}") from exc

        # Creation supports auto-suspend but not auto-delete. Set both together
        # on the newly created sandbox; never return a sandbox without retention.
        try:
            await sandbox.set_lifecycle_policy(self._policy(self._idle_seconds))
        except BaseException as exc:
            try:
                await sandbox.delete()
            except AzureError as cleanup_exc:
                exc.add_note(f"Could not delete Azure sandbox {sandbox.sandbox_id}: {cleanup_exc}")
            if isinstance(exc, AzureError):
                raise self._error(exc, sandbox.sandbox_id) from exc
            raise
        return sandbox.sandbox_id

    async def list_sessions(self) -> list[SandboxSessionInfo]:
        """List only this application's sandboxes, without resuming or touching them."""
        sessions = []
        try:
            async for sandbox in self._client.list_sandboxes(labels={"scope": self._scope}):
                if sandbox.labels.get("scope") != self._scope:
                    continue
                if not sandbox.id or not sandbox.created_at:
                    raise SandboxError("Azure returned a scoped sandbox without an ID or creation time")
                created_at = datetime.fromisoformat(sandbox.created_at)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                sessions.append(
                    SandboxSessionInfo(
                        session_id=sandbox.id, created_at=created_at, metadata=sandbox.labels
                    )
                )
        except AzureError as exc:
            raise self._error(exc, "<group>") from exc
        return sessions

    async def session_alive(self, session_id: str) -> bool:
        """Inspect without resuming or touching the sandbox's idle timer."""
        try:
            sandbox = await self._sandbox(session_id).get()
        except AzureError as exc:
            if self._missing(exc):
                return False
            raise self._error(exc, session_id) from exc
        if (sandbox.state or "").lower() in _TERMINAL_STATES:
            raise SandboxError(f"Azure sandbox {session_id!r} is {sandbox.state}")
        if sandbox.state_details and not sandbox.state_details.is_auto_resume_allowed():
            raise SandboxPermissionError(f"Azure sandbox {session_id!r} is administratively disabled")
        return True

    async def resume_session(self, session_id: str, *, timeout_seconds: int = 300) -> bool:
        """Resume the same VM and wait for Running; only a 404 means absent."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        sandbox = self._sandbox(session_id)
        deadline = time.monotonic() + timeout_seconds
        resumed = False
        while True:
            try:
                current = await sandbox.get()
                state = (current.state or "").lower()
                if state == "running":
                    return True
                if state in _TERMINAL_STATES:
                    raise SandboxError(f"Azure sandbox {session_id!r} is {current.state}")
                if state in _RESUMABLE_STATES:
                    if current.state_details and not current.state_details.is_auto_resume_allowed():
                        raise SandboxPermissionError(
                            f"Azure sandbox {session_id!r} is administratively disabled"
                        )
                    if not resumed:
                        await sandbox.resume()
                        resumed = True
                elif state not in {"creating", "resuming", "stopping"}:
                    raise SandboxError(f"Azure sandbox {session_id!r} has unknown state {current.state!r}")
            except AzureError as exc:
                if self._missing(exc):
                    return False
                raise self._error(exc, session_id) from exc
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SandboxError(f"Azure sandbox {session_id!r} did not become Running within {timeout_seconds}s")
            await asyncio.sleep(min(3, remaining))

    async def close_session(self, session_id: str) -> None:
        """Delete the sandbox itself; a missing sandbox is already closed."""
        try:
            await self._sandbox(session_id).delete()
        except AzureError as exc:
            if not self._missing(exc):
                raise self._error(exc, session_id) from exc
        self._exec_locks.pop(session_id, None)
        self._policy_intervals.pop(session_id, None)

    async def touch(self, session_id: str, *, timeout_seconds: int = _DEFAULT_IDLE_SECONDS) -> None:
        """Extend running idle time while retaining disk-only suspend and auto-delete."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        interval = max(self._idle_seconds, int(timeout_seconds))
        try:
            await self._sandbox(session_id).set_lifecycle_policy(self._policy(interval))
        except AzureError as exc:
            raise self._error(exc, session_id) from exc
        self._policy_intervals[session_id] = interval

    async def exec(
        self, session_id: str, command: str, *, timeout_seconds: int = 60
    ) -> ExecResult:
        """Keep detached work alive; an uncertain failure may leave it running."""
        if self._active_exec.get() == session_id:
            return await super().exec(session_id, command, timeout_seconds=timeout_seconds)
        async with self._exec_locks.setdefault(session_id, asyncio.Lock()):
            token = self._active_exec.set(session_id)
            previous = self._policy_intervals.get(session_id, self._idle_seconds)
            extended = timeout_seconds + 60 > previous
            applied = False
            try:
                if extended:
                    await self.touch(session_id, timeout_seconds=timeout_seconds + 60)
                    applied = True
                result = await super().exec(session_id, command, timeout_seconds=timeout_seconds)
                # Only a completed command proves it is safe to restore the
                # short idle timer. A lost poll/response may leave a guest job
                # executing until its original deadline.
                if applied:
                    await self.touch(session_id, timeout_seconds=previous)
                return result
            finally:
                self._active_exec.reset(token)

    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        try:
            # The preview SDK has no per-command timeout keyword. The shared
            # foreground wrapper enforces shell timeout; this also bounds a
            # stalled data-plane request independently of the shell.
            result = await asyncio.wait_for(
                self._sandbox(session_id).exec(command), timeout=timeout_seconds + 30
            )
            return ExecResult(
                stdout=result.stdout, stderr=result.stderr, exit_code=result.exit_code
            )
        except AzureError as exc:
            raise self._error(exc, session_id) from exc
        except TimeoutError as exc:
            raise SandboxError(f"Azure sandbox {session_id!r} exec timed out") from exc
