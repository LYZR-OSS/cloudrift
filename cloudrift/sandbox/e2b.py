import asyncio
import hashlib

from e2b import (
    AsyncSandbox,
    CommandExitException,
    SandboxNotFoundException,
    SandboxQuery,
)

from cloudrift.sandbox.base import (
    DEFAULT_READ_CHUNK_BYTES,
    DEFAULT_WRITE_CHUNK_B64,
    ExecResult,
    SandboxBackend,
    SandboxSessionInfo,
)


class E2BSandboxBackend(SandboxBackend):
    """E2B SaaS sandbox backend.
    Preserves the same sandbox ID and filesystem across idle auto-pause and
    explicit cold-boot resume; callers never recreate it on transient errors.

    Use ``from_api_key`` to construct.
    """

    def __init__(
        self,
        api_key: str,
        *,
        template: str | None = None,
        allow_internet_access: bool = True,
        read_chunk_bytes: int = DEFAULT_READ_CHUNK_BYTES,
        write_chunk_b64: int = DEFAULT_WRITE_CHUNK_B64,
        scope: str = "lyzr-agent",
    ) -> None:
        super().__init__(read_chunk_bytes=read_chunk_bytes, write_chunk_b64=write_chunk_b64)
        self._api_key = api_key
        self._template = template
        self._allow_internet_access = allow_internet_access
        self._scope = scope
        # Cache only command transports, never the answer to a control-plane
        # existence/resume check. A remote kill must be observed on lookup.
        self._sandboxes: dict[str, AsyncSandbox] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_api_key(cls, api_key: str, **kwargs) -> "E2BSandboxBackend":
        """Authenticate with an E2B API key."""
        return cls(api_key, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _connect(self, session_id: str) -> AsyncSandbox:
        cached = self._sandboxes.get(session_id)
        if cached is not None:
            return cached
        async with self._lock:
            cached = self._sandboxes.get(session_id)
            if cached is None:
                cached = await AsyncSandbox.connect(
                    sandbox_id=session_id, on_resume="reboot", api_key=self._api_key
                )
                self._sandboxes[session_id] = cached
        return cached

    async def close(self) -> None:
        # Sandboxes are session-scoped and outlive the process; only the
        # local connection cache is dropped, not the remote sandboxes.
        self._sandboxes.clear()

    # ------------------------------------------------------------------
    # SandboxBackend implementation
    # ------------------------------------------------------------------

    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        sb = await AsyncSandbox.create(
            template=self._template,
            timeout=int(timeout_seconds),
            allow_internet_access=self._allow_internet_access,
            lifecycle={
                "on_timeout": {"action": "pause", "keep_memory": False},
                "auto_resume": False,
            },
            metadata={
                "scope": self._scope,
                "logical_key": hashlib.sha256(key.encode()).hexdigest(),
            },
            api_key=self._api_key,
        )
        self._sandboxes[sb.sandbox_id] = sb
        return sb.sandbox_id

    async def session_alive(self, session_id: str) -> bool:
        try:
            # get_info does not resume a paused sandbox. Never infer liveness
            # from the command transport cached in this process.
            await AsyncSandbox.get_info(session_id, api_key=self._api_key)
            return True
        except SandboxNotFoundException:
            self._sandboxes.pop(session_id, None)
            return False

    async def resume_session(
        self, session_id: str, *, timeout_seconds: int = 300
    ) -> bool:
        try:
            sb = await AsyncSandbox.connect(
                sandbox_id=session_id,
                timeout=int(timeout_seconds),
                on_resume="reboot",
                api_key=self._api_key,
            )
        except SandboxNotFoundException:
            self._sandboxes.pop(session_id, None)
            return False
        self._sandboxes[session_id] = sb
        return True

    async def close_session(self, session_id: str) -> None:
        self._sandboxes.pop(session_id, None)
        await AsyncSandbox.kill(session_id, api_key=self._api_key)

    async def list_sessions(self) -> list[SandboxSessionInfo]:
        """List this backend's running and paused sessions for orphan cleanup.

        Scope filtering occurs remotely and is checked again locally. Listing
        does not connect to, resume, or extend any returned sandbox.
        """
        paginator = AsyncSandbox.list(
            query=SandboxQuery(metadata={"scope": self._scope}),
            api_key=self._api_key,
        )
        sessions = []
        while paginator.has_next:
            for sb in await paginator.next_items():
                if sb.metadata.get("scope") == self._scope:
                    sessions.append(
                        SandboxSessionInfo(
                            session_id=sb.sandbox_id,
                            created_at=sb.started_at,
                            metadata=sb.metadata,
                        )
                    )
        return sessions

    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        sb = await self._connect(session_id)
        try:
            res = await sb.commands.run(cmd=command, timeout=timeout_seconds)
            return ExecResult(stdout=res.stdout, stderr=res.stderr, exit_code=res.exit_code)
        except CommandExitException as exc:
            # commands.run raises on any non-zero exit — the one place this
            # backend must translate an exception into a normal result,
            # matching the shared base class's exit-code semantics.
            return ExecResult(stdout=exc.stdout, stderr=exc.stderr, exit_code=exc.exit_code)

    async def touch(self, session_id: str, *, timeout_seconds: int = 3600) -> None:
        # Use the control plane by ID: a stale cached command transport must
        # not hide a killed sandbox or a failed timeout extension.
        await AsyncSandbox.set_timeout(
            session_id, timeout=int(timeout_seconds), api_key=self._api_key
        )
