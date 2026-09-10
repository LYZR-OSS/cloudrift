import asyncio

from e2b import AsyncSandbox, CommandExitException, SandboxException

from cloudrift.sandbox.base import (
    DEFAULT_READ_CHUNK_BYTES,
    DEFAULT_WRITE_CHUNK_B64,
    ExecResult,
    SandboxBackend,
)


class E2BSandboxBackend(SandboxBackend):
    """E2B SaaS sandbox backend.

    Keeps the existing E2B service reachable through the provider-neutral
    :class:`SandboxBackend` interface, so callers get exactly one code path
    across AWS, Azure, and E2B.

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
    ) -> None:
        super().__init__(read_chunk_bytes=read_chunk_bytes, write_chunk_b64=write_chunk_b64)
        self._api_key = api_key
        self._template = template
        self._allow_internet_access = allow_internet_access
        # Reused across calls in one process so repeated exec/filesystem
        # operations against the same session share a connection.
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
                cached = await AsyncSandbox.connect(sandbox_id=session_id, api_key=self._api_key)
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
            api_key=self._api_key,
        )
        self._sandboxes[sb.sandbox_id] = sb
        return sb.sandbox_id

    async def session_alive(self, session_id: str) -> bool:
        try:
            await self._connect(session_id)
            return True
        except SandboxException:
            return False

    async def close_session(self, session_id: str) -> None:
        sb = self._sandboxes.pop(session_id, None)
        try:
            if sb is None:
                sb = await self._connect(session_id)
            await sb.kill()
        except SandboxException:
            pass

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
        try:
            sb = await self._connect(session_id)
            await sb.set_timeout(timeout=int(timeout_seconds))
        except SandboxException:
            pass
