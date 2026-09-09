import asyncio
import base64
import shlex
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass

from cloudrift.core.exceptions import SandboxError, SandboxTransferError

# 10 MiB is a conservative ceiling for a single base64-over-exec transfer —
# comfortably below any provider's documented request/response body limit.
MAX_TRANSFER_BYTES = 10 * 1024 * 1024
DEFAULT_READ_CHUNK_BYTES = 196_608  # 192 KiB -> 262_144 base64 chars per exec
# Each write chunk is embedded as a single `bash -lc "..."` argv element by
# backends that exec via argv (e.g. the AWS MicroVM exec server). Linux caps
# any single argv/envp string at MAX_ARG_STRLEN (32 pages = 128 KiB on the
# common 4 KiB page size); 100_000 base64 chars plus the `printf .. | base64
# -d >> path` scaffold stays comfortably under that on every provider.
DEFAULT_WRITE_CHUNK_B64 = 100_000
BACKGROUND_EXEC_THRESHOLD_SECONDS = 120
MAX_CAPTURED_OUTPUT_BYTES = 1_048_576
_RC_MARKER = "__CRSB_RC__"
_JOB_ROOT = "/tmp/.crsb"


@dataclass(frozen=True)
class ExecResult:
    stdout: str
    stderr: str
    exit_code: int


@dataclass(frozen=True)
class FileEntry:
    name: str
    type: str  # "file" | "dir"


class SandboxBackend(ABC):
    """Abstract base class for cloud sandbox (code execution) backends.

    A sandbox backend gives a caller a session-scoped shell: arbitrary bash as
    root, ``pip install``, package-manager installs, ``git clone``, and a
    filesystem that survives across calls. Concrete backends (AWS Lambda
    MicroVMs, Azure Container Apps dynamic sessions, E2B) implement exactly
    four methods — :meth:`open_session`, :meth:`session_alive`,
    :meth:`close_session`, and ``_exec_raw`` — everything else (every
    filesystem operation and the long-running-command protocol) is
    implemented once here, on top of those four, so the providers cannot
    drift on filesystem or timeout semantics.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release them cleanly.
    """

    def __init__(
        self,
        *,
        read_chunk_bytes: int = DEFAULT_READ_CHUNK_BYTES,
        write_chunk_b64: int = DEFAULT_WRITE_CHUNK_B64,
    ) -> None:
        self._read_chunk_bytes = read_chunk_bytes
        self._write_chunk_b64 = write_chunk_b64

    # ------------------------------------------------------------------
    # Abstract surface
    # ------------------------------------------------------------------

    @abstractmethod
    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        """Allocate a sandbox and return its provider session id, ready to accept
        commands. ``key`` is the caller's logical session key; providers that
        address sessions by a caller-chosen identifier derive it from ``key``,
        others ignore it."""

    @abstractmethod
    async def session_alive(self, session_id: str) -> bool:
        """True if the session can still accept commands (including a suspended
        session that the provider resumes on the next request)."""

    @abstractmethod
    async def close_session(self, session_id: str) -> None:
        """Terminate the session and release its compute. Idempotent: a missing
        session is not an error."""

    @abstractmethod
    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        """Run ``command`` in the session. Never called directly by users —
        :meth:`exec` wraps the command and normalizes the exit code first."""

    # ------------------------------------------------------------------
    # exec — short (foreground) vs. long (detached + poll)
    # ------------------------------------------------------------------

    async def exec(
        self, session_id: str, command: str, *, timeout_seconds: int = 60
    ) -> ExecResult:
        """Run ``command`` in the session, normalizing exit-code semantics.

        Commands at or under :data:`BACKGROUND_EXEC_THRESHOLD_SECONDS` run
        synchronously in the foreground. Longer commands — package installs,
        large clones — are launched detached and polled, because a single
        request cannot be trusted to survive a multi-minute HTTP proxy through
        either cloud.
        """
        if timeout_seconds <= BACKGROUND_EXEC_THRESHOLD_SECONDS:
            return await self._exec_foreground(session_id, command, timeout_seconds)
        return await self._exec_background(session_id, command, timeout_seconds)

    async def _exec_foreground(
        self, session_id: str, command: str, timeout_seconds: int
    ) -> ExecResult:
        wrapped = (
            f"timeout {int(timeout_seconds)}s bash -lc {shlex.quote(command)}; "
            f"printf '\\n{_RC_MARKER}%s' \"$?\""
        )
        result = await self._exec_raw(session_id, wrapped, timeout_seconds + 10)
        marker_index = result.stdout.rfind(_RC_MARKER)
        if marker_index == -1:
            # Session killed / output lost: keep the backend's own exit code
            # and leave stdout untouched rather than guessing.
            return result
        rc_text = result.stdout[marker_index + len(_RC_MARKER) :].strip()
        try:
            exit_code = int(rc_text)
        except ValueError:
            return result
        stdout = result.stdout[:marker_index]
        if stdout.endswith("\n"):
            stdout = stdout[:-1]
        return ExecResult(stdout=stdout, stderr=result.stderr, exit_code=exit_code)

    async def _exec_background(
        self, session_id: str, command: str, timeout_seconds: int
    ) -> ExecResult:
        job = f"{_JOB_ROOT}/{uuid.uuid4().hex}"
        # Routing the command text through the base64 writer means no quoting
        # hazard whatever the command contains; write_text uses short
        # foreground execs, so there is no recursion into this method.
        await self.write_text(session_id, f"{job}/cmd.sh", command)
        launch = (
            f"setsid bash -c 'bash -l {job}/cmd.sh > {job}/out 2> {job}/err; echo $? > {job}/rc' "
            f"</dev/null >/dev/null 2>&1 & echo $! > {job}/pid"
        )
        await self._exec_raw(session_id, launch, 30)

        deadline = time.monotonic() + timeout_seconds
        delay = 2.0
        rc_text = ""
        while time.monotonic() < deadline:
            probe = await self._exec_raw(
                session_id, f"cat {shlex.quote(job)}/rc 2>/dev/null", 30
            )
            if probe.stdout.strip():
                rc_text = probe.stdout.strip()
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 15.0)

        if rc_text:
            try:
                exit_code = int(rc_text)
            except ValueError:
                exit_code = 1
        else:
            await self._exec_raw(
                session_id,
                f"kill -TERM -$(cat {shlex.quote(job)}/pid) 2>/dev/null; "
                f"sleep 2; kill -KILL -$(cat {shlex.quote(job)}/pid) 2>/dev/null",
                30,
            )
            exit_code = 124

        out_result = await self._exec_raw(
            session_id, f"tail -c {MAX_CAPTURED_OUTPUT_BYTES} -- {shlex.quote(job)}/out 2>/dev/null", 30
        )
        err_result = await self._exec_raw(
            session_id, f"tail -c {MAX_CAPTURED_OUTPUT_BYTES} -- {shlex.quote(job)}/err 2>/dev/null", 30
        )
        await self._exec_raw(session_id, f"rm -rf -- {shlex.quote(job)}", 30)
        return ExecResult(stdout=out_result.stdout, stderr=err_result.stdout, exit_code=exit_code)

    # ------------------------------------------------------------------
    # Filesystem — all built on exec(); unexpected non-zero exit raises
    # SandboxError carrying stderr.
    # ------------------------------------------------------------------

    async def _run_checked(self, session_id: str, command: str) -> ExecResult:
        result = await self.exec(session_id, command)
        if result.exit_code != 0:
            raise SandboxError(f"command failed ({result.exit_code}): {command}\n{result.stderr}")
        return result

    async def make_dir(self, session_id: str, path: str) -> None:
        await self._run_checked(session_id, f"mkdir -p -- {shlex.quote(path)}")

    async def list_dir(self, session_id: str, path: str = ".") -> list[FileEntry]:
        result = await self._run_checked(session_id, f"ls -Ap -- {shlex.quote(path)}")
        entries: list[FileEntry] = []
        for line in result.stdout.splitlines():
            if not line:
                continue
            if line.endswith("/"):
                entries.append(FileEntry(name=line[:-1], type="dir"))
            else:
                entries.append(FileEntry(name=line, type="file"))
        return entries

    async def rename(self, session_id: str, old_path: str, new_path: str) -> None:
        await self._run_checked(
            session_id, f"mv -- {shlex.quote(old_path)} {shlex.quote(new_path)}"
        )

    async def remove(self, session_id: str, path: str) -> None:
        await self._run_checked(session_id, f"rm -rf -- {shlex.quote(path)}")

    async def exists(self, session_id: str, path: str) -> bool:
        result = await self.exec(session_id, f"test -e {shlex.quote(path)}")
        if result.exit_code == 0:
            return True
        if result.exit_code == 1:
            return False
        raise SandboxError(f"exists check failed for {path!r}: {result.stderr}")

    async def is_dir(self, session_id: str, path: str) -> bool:
        result = await self.exec(session_id, f"test -d {shlex.quote(path)}")
        if result.exit_code == 0:
            return True
        if result.exit_code == 1:
            return False
        raise SandboxError(f"is_dir check failed for {path!r}: {result.stderr}")

    async def size(self, session_id: str, path: str) -> int:
        result = await self._run_checked(session_id, f"stat -c %s {shlex.quote(path)}")
        return int(result.stdout.strip())

    async def read_text(self, session_id: str, path: str) -> str:
        data = await self.read_bytes(session_id, path)
        return data.decode("utf-8", errors="replace")

    async def write_text(self, session_id: str, path: str, content: str) -> None:
        await self.write_bytes(session_id, path, content.encode("utf-8"))

    async def read_bytes(self, session_id: str, path: str) -> bytes:
        total = await self.size(session_id, path)
        if total > MAX_TRANSFER_BYTES:
            raise SandboxTransferError(
                f"{path!r} is {total} bytes, exceeding the {MAX_TRANSFER_BYTES}-byte transfer limit"
            )
        chunk = self._read_chunk_bytes
        parts: list[bytes] = []
        i = 0
        received = 0
        while received < total:
            result = await self._run_checked(
                session_id,
                f"dd if={shlex.quote(path)} bs={chunk} skip={i} count=1 2>/dev/null | base64 -w0",
            )
            piece = base64.b64decode(result.stdout.strip() or "")
            if not piece:
                break
            parts.append(piece)
            received += len(piece)
            i += 1
        data = b"".join(parts)
        if len(data) != total:
            raise SandboxTransferError(f"read of {path!r} returned {len(data)} bytes, expected {total}")
        return data

    async def write_bytes(self, session_id: str, path: str, data: bytes) -> None:
        if len(data) > MAX_TRANSFER_BYTES:
            raise SandboxTransferError(
                f"write of {len(data)} bytes to {path!r} exceeds the "
                f"{MAX_TRANSFER_BYTES}-byte transfer limit"
            )
        await self._run_checked(session_id, f'mkdir -p -- "$(dirname {shlex.quote(path)})"')
        if not data:
            await self._run_checked(session_id, f": > {shlex.quote(path)}")
        else:
            encoded = base64.b64encode(data).decode("ascii")
            chunk = self._write_chunk_b64
            for i in range(0, len(encoded), chunk):
                piece = encoded[i : i + chunk]
                redirect = ">" if i == 0 else ">>"
                await self._run_checked(
                    session_id,
                    f"printf %s {shlex.quote(piece)} | base64 -d {redirect} {shlex.quote(path)}",
                )
        written = await self.size(session_id, path)
        if written != len(data):
            raise SandboxTransferError(
                f"write of {len(data)} bytes to {path!r} verified as {written} bytes"
            )

    async def touch(self, session_id: str, *, timeout_seconds: int = 3600) -> None:
        """Extend session lifetime. Default is a no-op: Lambda MicroVMs and ACA
        both treat endpoint traffic as activity; only E2B needs an explicit call."""

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "SandboxBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()
