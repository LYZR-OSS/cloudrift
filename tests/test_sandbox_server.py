"""Contract test between `cloudrift.sandbox` and `cloudrift_sandbox_server`.

Unlike `test_sandbox.py` (mocked SDKs and an in-memory `_FakeShellBackend`),
this file drives the real `cloudrift_sandbox_server` module over real HTTP
through the real `SandboxBackend` filesystem/exec layer. No cloud, no mocks.

`SandboxBackend` generates GNU-specific shell (`timeout Ns bash -lc ...` in
`_exec_foreground`, `stat -c %s` in `size`, `setsid` in `_exec_background`)
that does not exist on macOS, so the whole module is Linux-only.
"""

import asyncio
import http.client
import json
import os
import sys
import threading
from http.server import ThreadingHTTPServer

import pytest

import cloudrift_sandbox_server
from cloudrift.sandbox.base import (
    _JOB_ROOT,
    BACKGROUND_EXEC_THRESHOLD_SECONDS,
    DEFAULT_WRITE_CHUNK_B64,
    MAX_CAPTURED_OUTPUT_BYTES,
    SANDBOX_EXEC_PATH,
    SANDBOX_EXEC_PORT,
    SANDBOX_WORKDIR,
    ExecResult,
    SandboxBackend,
)
from cloudrift.sandbox.base import TIMEOUT_EXIT_CODE as BASE_TIMEOUT_EXIT_CODE

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="SandboxBackend emits GNU coreutils shell (timeout, stat -c, setsid); Linux only",
)


class _LocalHTTPBackend(SandboxBackend):
    """Talks to a real `cloudrift_sandbox_server` instance over loopback HTTP."""

    def __init__(self, port: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self._port = port

    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        return "local"

    async def session_alive(self, session_id: str) -> bool:
        return True

    async def close_session(self, session_id: str) -> None:
        pass

    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        def _do_request() -> ExecResult:
            conn = http.client.HTTPConnection("127.0.0.1", self._port, timeout=timeout_seconds + 30)
            try:
                body = json.dumps({"command": command, "timeout_seconds": timeout_seconds}).encode(
                    "utf-8"
                )
                conn.request(
                    "POST",
                    SANDBOX_EXEC_PATH,
                    body=body,
                    headers={"Content-Type": "application/json"},
                )
                resp = conn.getresponse()
                data = json.loads(resp.read())
                return ExecResult(
                    stdout=data["stdout"], stderr=data["stderr"], exit_code=data["exit_code"]
                )
            finally:
                conn.close()

        return await asyncio.to_thread(_do_request)


@pytest.fixture(scope="module")
def app_server_port():
    server = ThreadingHTTPServer(("127.0.0.1", 0), cloudrift_sandbox_server._AppHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def server_workdir(monkeypatch, tmp_path):
    monkeypatch.setattr(cloudrift_sandbox_server, "WORKDIR", str(tmp_path))
    return tmp_path


@pytest.fixture
def backend(app_server_port, server_workdir):
    return _LocalHTTPBackend(app_server_port)


# ---------------------------------------------------------------------------
# /exec contract
# ---------------------------------------------------------------------------


async def test_exec_returns_stdout_and_zero_exit(backend):
    # `_exec_foreground` strips only the printf-injected separator newline
    # before the RC marker, not the command's own trailing newline — `echo`
    # legitimately returns a trailing "\n" here.
    result = await backend.exec("local", "echo hello")
    assert result.stdout == "hello\n"
    assert result.exit_code == 0


async def test_exec_propagates_nonzero_exit_code(backend):
    result = await backend.exec("local", "exit 7")
    assert result.exit_code == 7


async def test_exec_runs_in_server_workdir(backend, server_workdir):
    result = await backend.exec("local", "pwd")
    assert result.stdout == str(server_workdir) + "\n"


async def test_exec_timeout_returns_timeout_exit_code(backend):
    result = await backend.exec("local", "sleep 5", timeout_seconds=1)
    assert result.exit_code == cloudrift_sandbox_server.TIMEOUT_EXIT_CODE


# ---------------------------------------------------------------------------
# Filesystem, built on /exec
# ---------------------------------------------------------------------------


async def test_filesystem_roundtrip_through_real_server(backend):
    payload = os.urandom(300_000)
    await backend.write_bytes("local", "blob.bin", payload)
    assert await backend.read_bytes("local", "blob.bin") == payload
    entries = await backend.list_dir("local", ".")
    assert any(e.name == "blob.bin" and e.type == "file" for e in entries)


# ---------------------------------------------------------------------------
# Long-command (detached) protocol
# ---------------------------------------------------------------------------


async def test_long_command_uses_detached_protocol(backend):
    # `_exec_background` tails the job's stdout file verbatim: no RC-marker
    # wrapping, so no newline stripping happens at all.
    assert 180 > BACKGROUND_EXEC_THRESHOLD_SECONDS
    result = await backend.exec("local", "sleep 1; echo done", timeout_seconds=180)
    assert result.stdout == "done\n"
    assert result.exit_code == 0
    leftover = await backend.exec("local", f"ls -A {_JOB_ROOT} 2>/dev/null")
    assert leftover.stdout.strip() == ""


# ---------------------------------------------------------------------------
# Lambda MicroVM lifecycle hooks (port 9000)
# ---------------------------------------------------------------------------


async def test_lifecycle_ready_hook_reports_503_before_ready_and_200_after():
    server = ThreadingHTTPServer(("127.0.0.1", 0), cloudrift_sandbox_server._LifecycleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    was_set = cloudrift_sandbox_server._ready.is_set()
    try:
        cloudrift_sandbox_server._ready.clear()

        def _post(path: str) -> int:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            try:
                conn.request("POST", path)
                return conn.getresponse().status
            finally:
                conn.close()

        status_before = await asyncio.to_thread(
            _post, cloudrift_sandbox_server.HOOK_PREFIX + "ready"
        )
        assert status_before == 503

        cloudrift_sandbox_server._ready.set()
        status_after = await asyncio.to_thread(
            _post, cloudrift_sandbox_server.HOOK_PREFIX + "ready"
        )
        assert status_after == 200
    finally:
        if was_set:
            cloudrift_sandbox_server._ready.set()
        else:
            cloudrift_sandbox_server._ready.clear()
        server.shutdown()
        thread.join(timeout=5)


async def test_lifecycle_validate_probes_exec_endpoint(
    monkeypatch, app_server_port, server_workdir
):
    monkeypatch.setattr(cloudrift_sandbox_server, "EXEC_PORT", app_server_port)
    server = ThreadingHTTPServer(("127.0.0.1", 0), cloudrift_sandbox_server._LifecycleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:

        def _post(path: str) -> int:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
            try:
                conn.request("POST", path)
                return conn.getresponse().status
            finally:
                conn.close()

        status = await asyncio.to_thread(_post, cloudrift_sandbox_server.HOOK_PREFIX + "validate")
        assert status == 200
    finally:
        server.shutdown()
        thread.join(timeout=5)


async def test_unknown_paths_404(app_server_port):
    def _get(path: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", app_server_port, timeout=5)
        try:
            conn.request("GET", path)
            return conn.getresponse().status
        finally:
            conn.close()

    def _post(path: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", app_server_port, timeout=5)
        try:
            conn.request("POST", path, body=b"{}")
            return conn.getresponse().status
        finally:
            conn.close()

    assert await asyncio.to_thread(_get, "/nope") == 404
    assert await asyncio.to_thread(_post, "/nope") == 404

    server = ThreadingHTTPServer(("127.0.0.1", 0), cloudrift_sandbox_server._LifecycleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:

        def _post_hook(path: str) -> int:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            try:
                conn.request("POST", path)
                return conn.getresponse().status
            finally:
                conn.close()

        status = await asyncio.to_thread(
            _post_hook, cloudrift_sandbox_server.HOOK_PREFIX + "not-a-real-hook"
        )
        assert status == 404
    finally:
        server.shutdown()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Drift alarm: client and server must agree on the wire contract
# ---------------------------------------------------------------------------


def test_client_and_server_protocol_constants_agree():
    # If this fails, the client (cloudrift/sandbox/base.py) and the guest
    # server (cloudrift_sandbox_server.py) have drifted apart on the wire
    # contract they are supposed to share. Fix the constant that changed,
    # do not change this test.
    assert cloudrift_sandbox_server.EXEC_PORT == SANDBOX_EXEC_PORT
    assert cloudrift_sandbox_server.WORKDIR == SANDBOX_WORKDIR
    assert cloudrift_sandbox_server.MAX_OUTPUT_BYTES == MAX_CAPTURED_OUTPUT_BYTES
    assert cloudrift_sandbox_server.TIMEOUT_EXIT_CODE == BASE_TIMEOUT_EXIT_CODE
    # Linux's MAX_ARG_STRLEN (32 pages = 128 KiB on 4 KiB pages) bounds any
    # single argv element; the server execs via `subprocess.run(["bash",
    # "-lc", cmd])`, so a write chunk plus its printf/base64 scaffold must
    # stay comfortably under that ceiling.
    assert DEFAULT_WRITE_CHUNK_B64 < 128 * 1024
