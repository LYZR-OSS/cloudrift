"""Unit tests for the `sandbox` category.

No real cloud calls: AWS Lambda MicroVMs and Azure ACA are exercised through
mocked SDK clients / aiohttp sessions, E2B through a stubbed `AsyncSandbox`,
and the shared exec/filesystem layer in `cloudrift.sandbox.base` through a
`_FakeShellBackend` that interprets the specific command shapes that layer
generates against an in-memory filesystem.
"""

import base64
import os
import re
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import (
    SandboxError,
    SandboxPermissionError,
    SandboxTransferError,
)
from cloudrift.sandbox import get_sandbox
from cloudrift.sandbox.aws_microvm import AWSMicroVMSandboxBackend
from cloudrift.sandbox.azure_aca import AzureACASessionsBackend
from cloudrift.sandbox.base import (
    _JOB_ROOT,
    MAX_TRANSFER_BYTES,
    ExecResult,
    FileEntry,
    SandboxBackend,
)
from cloudrift.sandbox.e2b import E2BSandboxBackend
from e2b import CommandExitException

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _RawStubBackend(SandboxBackend):
    """Returns a fixed raw `_exec_raw` result regardless of the command sent.

    For unit tests of exec()'s own wrapping/marker-stripping logic, where the
    inner command's real semantics don't matter.
    """

    def __init__(self, raw_result: ExecResult, **kwargs) -> None:
        super().__init__(**kwargs)
        self._raw_result = raw_result
        self.commands: list[str] = []

    async def open_session(self, key, *, timeout_seconds=3600):
        return "stub-session"

    async def session_alive(self, session_id):
        return True

    async def close_session(self, session_id):
        pass

    async def _exec_raw(self, session_id, command, timeout_seconds):
        self.commands.append(command)
        return self._raw_result


class _FakeShellBackend(SandboxBackend):
    """In-memory filesystem that interprets the specific command shapes
    `cloudrift.sandbox.base.SandboxBackend`'s shared exec/filesystem layer
    generates: `dd`, `base64`, `stat -c %s`, `printf | base64 -d`, `ls -Ap`,
    `test`, `mkdir`, `mv`, `cat`, `tail -c`, plus the `setsid`/poll/`kill`
    long-command protocol.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.fs: dict[str, bytes] = {}
        self.dirs: set[str] = {"."}
        self.fake_sizes: dict[str, int] = {}
        self.raw_commands: list[str] = []
        self.background_result: tuple[str, str, int] = ("", "", 0)
        self.never_complete_job = False
        self.truncate_next_dd = False
        self.last_cmd_sh: bytes | None = None

    async def open_session(self, key, *, timeout_seconds=3600):
        return "fake-session"

    async def session_alive(self, session_id):
        return True

    async def close_session(self, session_id):
        pass

    async def _exec_raw(self, session_id, command, timeout_seconds):
        self.raw_commands.append(command)
        m = re.match(
            r"^timeout (\d+)s bash -lc (.+); printf '\\n__CRSB_RC__%s' \"\$\?\"$",
            command,
        )
        if m:
            import shlex as _shlex

            inner = _shlex.split(m.group(2))[0]
            stdout, stderr, code = self._run_inner(inner)
            return ExecResult(stdout=f"{stdout}\n__CRSB_RC__{code}", stderr=stderr, exit_code=0)
        stdout, stderr, code = self._run_inner(command)
        return ExecResult(stdout=stdout, stderr=stderr, exit_code=code)

    def _run_inner(self, command: str) -> tuple[str, str, int]:
        if command.startswith("setsid bash -c "):
            return self._run_launch(command)
        m = re.match(r"^cat (\S+)/rc 2>/dev/null$", command)
        if m:
            return self.fs.get(f"{m.group(1)}/rc", b"").decode(), "", 0
        if command.startswith("kill -TERM -$(cat "):
            return "", "", 0
        m = re.match(r"^tail -c \d+ -- (\S+) 2>/dev/null$", command)
        if m:
            return self.fs.get(m.group(1), b"").decode("utf-8", "replace"), "", 0
        m = re.match(r'^mkdir -p -- "\$\(dirname (\S+)\)"$', command)
        if m:
            self.dirs.add(m.group(1).rsplit("/", 1)[0] or "/")
            return "", "", 0
        m = re.match(r"^mkdir -p -- (\S+)$", command)
        if m:
            self.dirs.add(m.group(1))
            return "", "", 0
        m = re.match(r"^ls -Ap -- (\S+)$", command)
        if m:
            return self._ls(m.group(1)), "", 0
        m = re.match(r"^mv -- (\S+) (\S+)$", command)
        if m:
            old, new = m.groups()
            if old not in self.fs:
                return "", f"mv: cannot stat '{old}': No such file or directory", 1
            self.fs[new] = self.fs.pop(old)
            return "", "", 0
        m = re.match(r"^rm -rf -- (\S+)$", command)
        if m:
            prefix = m.group(1)
            for p in list(self.fs):
                if p == prefix or p.startswith(prefix + "/"):
                    del self.fs[p]
            self.dirs = {d for d in self.dirs if not (d == prefix or d.startswith(prefix + "/"))}
            return "", "", 0
        m = re.match(r"^test -e (\S+)$", command)
        if m:
            path = m.group(1)
            return "", "", 0 if (path in self.fs or path in self.dirs) else 1
        m = re.match(r"^test -d (\S+)$", command)
        if m:
            return "", "", 0 if m.group(1) in self.dirs else 1
        m = re.match(r"^stat -c %s (\S+)$", command)
        if m:
            path = m.group(1)
            if path in self.fake_sizes:
                return str(self.fake_sizes[path]), "", 0
            if path not in self.fs:
                return "", f"stat: cannot stat '{path}': No such file or directory", 1
            return str(len(self.fs[path])), "", 0
        m = re.match(r"^dd if=(\S+) bs=(\d+) skip=(\d+) count=1 2>/dev/null \| base64 -w0$", command)
        if m:
            path, bs, skip = m.group(1), int(m.group(2)), int(m.group(3))
            data = self.fs.get(path, b"")
            chunk = data[skip * bs : skip * bs + bs]
            if self.truncate_next_dd and chunk:
                chunk = chunk[:-1]
                self.truncate_next_dd = False
            return base64.b64encode(chunk).decode("ascii"), "", 0
        m = re.match(r"^: > (\S+)$", command)
        if m:
            self.fs[m.group(1)] = b""
            return "", "", 0
        m = re.match(r"^printf %s (\S+) \| base64 -d (>>?) (\S+)$", command)
        if m:
            piece_b64, redirect, path = m.groups()
            piece = base64.b64decode(piece_b64)
            if redirect == ">":
                self.fs[path] = piece
            else:
                self.fs[path] = self.fs.get(path, b"") + piece
            return "", "", 0
        raise AssertionError(f"_FakeShellBackend cannot interpret command: {command!r}")

    def _run_launch(self, command: str) -> tuple[str, str, int]:
        m = re.match(r"^setsid bash -c 'bash -l (.+)/cmd\.sh > ", command)
        if not m:
            raise AssertionError(f"cannot parse launch command: {command!r}")
        job = m.group(1)
        self.last_cmd_sh = self.fs.get(f"{job}/cmd.sh")
        self.fs[f"{job}/pid"] = b"12345"
        if not self.never_complete_job:
            stdout, stderr, code = self.background_result
            self.fs[f"{job}/out"] = stdout.encode()
            self.fs[f"{job}/err"] = stderr.encode()
            self.fs[f"{job}/rc"] = str(code).encode()
        return "", "", 0

    def _ls(self, path: str) -> str:
        prefix = path.rstrip("/") + "/"
        names: set[str] = set()
        for p in self.fs:
            if p.startswith(prefix):
                rest = p[len(prefix) :]
                names.add(rest.split("/", 1)[0] + "/" if "/" in rest else rest)
        for d in self.dirs:
            if d.startswith(prefix):
                rest = d[len(prefix) :]
                if rest and "/" not in rest:
                    names.add(rest + "/")
        return "".join(f"{n}\n" for n in sorted(names))


class _FakeAsyncCM:
    def __init__(self, response) -> None:
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *exc):
        return False


class _FakeResponse:
    def __init__(self, status: int, json_body=None, text_body: str = "") -> None:
        self.status = status
        self._json_body = json_body
        self._text_body = text_body

    async def json(self):
        return self._json_body

    async def text(self):
        return self._text_body


# ---------------------------------------------------------------------------
# 1. Shared exec/filesystem layer
# ---------------------------------------------------------------------------


async def test_write_bytes_read_bytes_roundtrip_chunked():
    backend = _FakeShellBackend(read_chunk_bytes=16384, write_chunk_b64=4096)
    sid = await backend.open_session("k")
    payload = os.urandom(300_000)

    await backend.write_bytes(sid, "/tmp/x/blob.bin", payload)
    result = await backend.read_bytes(sid, "/tmp/x/blob.bin")

    assert result == payload


async def test_read_bytes_raises_on_truncated_chunk():
    backend = _FakeShellBackend(read_chunk_bytes=1024)
    sid = await backend.open_session("k")
    await backend.write_bytes(sid, "/tmp/x/blob.bin", os.urandom(2048))

    backend.truncate_next_dd = True
    with pytest.raises(SandboxTransferError):
        await backend.read_bytes(sid, "/tmp/x/blob.bin")


async def test_write_bytes_over_limit_raises_without_issuing_command():
    backend = _FakeShellBackend()
    sid = await backend.open_session("k")

    with pytest.raises(SandboxTransferError, match=str(MAX_TRANSFER_BYTES)):
        await backend.write_bytes(sid, "/tmp/x/big.bin", b"x" * (MAX_TRANSFER_BYTES + 1))

    assert backend.raw_commands == []


async def test_read_bytes_over_limit_raises_without_dd_command():
    backend = _FakeShellBackend()
    sid = await backend.open_session("k")
    backend.fake_sizes["/tmp/x/big.bin"] = MAX_TRANSFER_BYTES + 1

    with pytest.raises(SandboxTransferError, match=str(MAX_TRANSFER_BYTES)):
        await backend.read_bytes(sid, "/tmp/x/big.bin")

    assert not any("dd if=" in c for c in backend.raw_commands)


async def test_list_dir_maps_ls_output_to_file_entries():
    backend = _RawStubBackend(ExecResult(stdout="a.txt\nsub/\n\n__CRSB_RC__0", stderr="", exit_code=0))
    entries = await backend.list_dir("s", "wk")
    assert entries == [FileEntry("a.txt", "file"), FileEntry("sub", "dir")]


async def test_exists_false_on_exit_1_and_raises_on_exit_2():
    backend_false = _RawStubBackend(ExecResult(stdout="\n__CRSB_RC__1", stderr="", exit_code=1))
    assert await backend_false.exists("s", "/x") is False

    backend_err = _RawStubBackend(ExecResult(stdout="\n__CRSB_RC__2", stderr="denied", exit_code=2))
    with pytest.raises(SandboxError):
        await backend_err.exists("s", "/x")


async def test_exec_strips_marker_and_returns_exit_code():
    backend = _RawStubBackend(ExecResult(stdout="hello\n__CRSB_RC__7", stderr="", exit_code=1))
    result = await backend.exec("s", "ignored")
    assert result.exit_code == 7
    assert result.stdout == "hello"


async def test_exec_short_command_wraps_with_timeout():
    backend = _RawStubBackend(ExecResult(stdout="x\n__CRSB_RC__0", stderr="", exit_code=0))
    await backend.exec("s", "echo hi", timeout_seconds=30)
    assert any("timeout 30s bash -lc" in c for c in backend.commands)


# ---------------------------------------------------------------------------
# 2. Long-command protocol
# ---------------------------------------------------------------------------


async def test_long_command_writes_cmd_sh_and_polls_until_rc():
    backend = _FakeShellBackend()
    sid = await backend.open_session("k")
    backend.background_result = ("done-output", "", 0)

    result = await backend.exec(sid, "pip install cowsay", timeout_seconds=600)

    assert result.exit_code == 0
    assert result.stdout == "done-output"
    assert backend.last_cmd_sh == b"pip install cowsay"
    launch_cmds = [c for c in backend.raw_commands if c.startswith("setsid bash -c")]
    assert len(launch_cmds) == 1
    assert f"{_JOB_ROOT}/" in launch_cmds[0]
    assert "setsid" in launch_cmds[0]


async def test_long_command_times_out_and_sends_kill(monkeypatch):
    import cloudrift.sandbox.base as base_module

    # Mock only asyncio.sleep so the backoff loop doesn't consume real wall
    # time; call `_exec_background` directly (bypassing `exec()`'s dispatch)
    # so the internal `write_text` calls for cmd.sh stay on the foreground
    # path regardless of the module's real threshold constant.
    monkeypatch.setattr(base_module.asyncio, "sleep", AsyncMock())

    backend = _FakeShellBackend()
    sid = await backend.open_session("k")
    backend.never_complete_job = True

    result = await backend._exec_background(sid, "sleep forever", timeout_seconds=0.05)

    assert result.exit_code == 124
    assert any(c.startswith("kill -TERM") for c in backend.raw_commands)


# ---------------------------------------------------------------------------
# 3. Lambda MicroVMs
# ---------------------------------------------------------------------------


def _microvm_backend(**overrides) -> AWSMicroVMSandboxBackend:
    session = MagicMock(name="session")
    kwargs = dict(
        image_identifier="arn:aws:lambda:us-east-1:111122223333:microvm-image:x",
        region="us-east-1",
    )
    kwargs.update(overrides)
    return AWSMicroVMSandboxBackend(session, **kwargs)


def test_region_required_raises_value_error():
    session = MagicMock(name="session")
    session.get_config_variable = MagicMock(return_value=None)
    with pytest.raises(ValueError, match="region is required"):
        AWSMicroVMSandboxBackend(session, image_identifier="arn:x")


def test_default_connector_arns_are_region_formatted():
    backend = _microvm_backend()
    assert backend._ingress_connector_arn == (
        "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:ALL_INGRESS"
    )
    assert backend._egress_connector_arn == (
        "arn:aws:lambda:us-east-1:aws:network-connector:aws-network-connector:INTERNET_EGRESS"
    )


async def test_open_session_runs_microvm_and_waits_for_health():
    backend = _microvm_backend(startup_timeout_seconds=5)
    fake_client = SimpleNamespace(
        run_microvm=AsyncMock(
            return_value={"microvmId": "mv-1", "endpoint": "mv-1.lambda-url.example.com"}
        ),
        create_microvm_auth_token=AsyncMock(
            return_value={"authToken": {"X-aws-proxy-auth": "tok-1"}}
        ),
    )
    backend._ensure = AsyncMock(return_value=fake_client)
    fake_http = MagicMock()
    fake_http.get = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(200)))
    backend._http_session = AsyncMock(return_value=fake_http)

    microvm_id = await backend.open_session("key-1", timeout_seconds=999_999)

    assert microvm_id == "mv-1"
    call_kwargs = fake_client.run_microvm.call_args.kwargs
    assert call_kwargs["idlePolicy"]["autoResumeEnabled"] is True
    assert call_kwargs["maximumDurationInSeconds"] == 28800  # clamped
    assert call_kwargs["ingressNetworkConnectors"] == [backend._ingress_connector_arn]
    assert call_kwargs["egressNetworkConnectors"] == [backend._egress_connector_arn]
    fake_http.get.assert_called_once()
    assert fake_http.get.call_args.args[0] == "https://mv-1.lambda-url.example.com/health"
    assert fake_http.get.call_args.kwargs["headers"] == {
        "X-aws-proxy-auth": "tok-1",
        "X-aws-proxy-port": "8080",
    }


async def test_open_session_raises_when_health_never_ready():
    backend = _microvm_backend(startup_timeout_seconds=0.2)
    fake_client = SimpleNamespace(
        run_microvm=AsyncMock(return_value={"microvmId": "mv-2", "endpoint": "mv-2.example.com"}),
        create_microvm_auth_token=AsyncMock(
            return_value={"authToken": {"X-aws-proxy-auth": "tok-2"}}
        ),
    )
    backend._ensure = AsyncMock(return_value=fake_client)
    fake_http = MagicMock()
    fake_http.get = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(503)))
    backend._http_session = AsyncMock(return_value=fake_http)

    with pytest.raises(SandboxError, match="mv-2"):
        await backend.open_session("key-2")



async def test_session_alive_true_for_suspended_false_for_terminated():
    backend = _microvm_backend()
    fake_client = SimpleNamespace(
        get_microvm=AsyncMock(return_value={"state": "SUSPENDED", "endpoint": "h"})
    )
    backend._ensure = AsyncMock(return_value=fake_client)
    assert await backend.session_alive("mv-1") is True

    fake_client.get_microvm = AsyncMock(return_value={"state": "TERMINATED"})
    assert await backend.session_alive("mv-1") is False


async def test_exec_raw_sends_auth_headers_and_normalizes_bare_endpoint():
    backend = _microvm_backend()
    backend._endpoints["mv-1"] = "https://bare-host.example.com"
    backend._tokens["mv-1"] = ("tok-abc", time.time() + 999)
    fake_http = MagicMock()
    fake_http.post = MagicMock(
        return_value=_FakeAsyncCM(
            _FakeResponse(200, json_body={"stdout": "hi", "stderr": "", "exit_code": 0})
        )
    )
    backend._http_session = AsyncMock(return_value=fake_http)

    result = await backend._exec_raw("mv-1", "echo hi", 30)

    assert result == ExecResult("hi", "", 0)
    call = fake_http.post.call_args
    assert call.args[0] == "https://bare-host.example.com/exec"
    assert call.kwargs["headers"]["X-aws-proxy-auth"] == "tok-abc"
    assert call.kwargs["headers"]["X-aws-proxy-port"] == "8080"


async def test_exec_raw_retries_once_on_403_then_raises_permission_error():
    backend = _microvm_backend()
    backend._endpoints["mv-1"] = "https://h.example.com"
    fake_client = SimpleNamespace(
        create_microvm_auth_token=AsyncMock(
            return_value={"authToken": {"X-aws-proxy-auth": "tok"}}
        )
    )
    backend._ensure = AsyncMock(return_value=fake_client)
    fake_http = MagicMock()
    fake_http.post = MagicMock(
        side_effect=[
            _FakeAsyncCM(_FakeResponse(403)),
            _FakeAsyncCM(_FakeResponse(403)),
        ]
    )
    backend._http_session = AsyncMock(return_value=fake_http)

    with pytest.raises(SandboxPermissionError):
        await backend._exec_raw("mv-1", "echo hi", 30)

    assert fake_client.create_microvm_auth_token.call_count == 2
    assert fake_http.post.call_count == 2


async def test_get_microvm_resource_not_found_maps_to_session_alive_false():
    backend = _microvm_backend()
    error = ClientError({"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}}, "GetMicrovm")
    fake_client = SimpleNamespace(get_microvm=AsyncMock(side_effect=error))
    backend._ensure = AsyncMock(return_value=fake_client)

    assert await backend.session_alive("mv-x") is False


async def test_terminate_microvm_not_found_is_swallowed():
    backend = _microvm_backend()
    error = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}}, "TerminateMicrovm"
    )
    fake_client = SimpleNamespace(terminate_microvm=AsyncMock(side_effect=error))
    backend._ensure = AsyncMock(return_value=fake_client)

    await backend.close_session("mv-x")  # must not raise


# ---------------------------------------------------------------------------
# 4. Azure Container Apps dynamic sessions
# ---------------------------------------------------------------------------


def _aca_backend(**overrides):
    credential = MagicMock(name="credential")
    credential.get_token = AsyncMock(
        return_value=SimpleNamespace(token="tok", expires_on=time.time() + 3600)
    )
    backend = AzureACASessionsBackend(
        "https://pool.env.eastus.azurecontainerapps.io", credential, **overrides
    )
    return backend, credential


async def test_open_session_no_network_call_returns_hex_identifier():
    backend, credential = _aca_backend()
    session_id = await backend.open_session("apikey:sessionid")
    assert len(session_id) == 64
    assert all(c in "0123456789abcdef" for c in session_id)
    credential.get_token.assert_not_awaited()


def test_from_managed_identity_builds_credential():
    fake_credential = MagicMock(name="fake-credential")
    with patch(
        "cloudrift.core.azure_credentials.build_async_credential", return_value=fake_credential
    ) as mock_build:
        backend = AzureACASessionsBackend.from_managed_identity(
            "https://pool.env.eastus.azurecontainerapps.io", client_id="cid"
        )
    mock_build.assert_called_once_with("cid")
    assert backend._credential is fake_credential


async def test_exec_raw_posts_to_pool_endpoint_with_identifier_query():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(
        return_value=_FakeAsyncCM(
            _FakeResponse(200, json_body={"stdout": "hi", "stderr": "", "exit_code": 0})
        )
    )
    backend._http = fake_http

    result = await backend._exec_raw("sess-1", "echo hi", 30)

    assert result == ExecResult("hi", "", 0)
    call = fake_http.post.call_args
    assert call.args[0] == "https://pool.env.eastus.azurecontainerapps.io/exec"
    assert call.kwargs["params"] == {"identifier": "sess-1"}
    assert call.kwargs["headers"]["Authorization"] == "Bearer tok"


async def test_session_alive_maps_400_not_found_body_to_false():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(
        return_value=_FakeAsyncCM(
            _FakeResponse(400, text_body='{"error":"SessionWithIdentifierNotFound"}')
        )
    )
    backend._http = fake_http
    assert await backend.session_alive("sess-1") is False


async def test_session_alive_true_on_200():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(200)))
    backend._http = fake_http
    assert await backend.session_alive("sess-1") is True


async def test_close_session_posts_to_stop_session():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(200)))
    backend._http = fake_http

    await backend.close_session("sess-1")

    call = fake_http.post.call_args
    assert call.args[0].endswith("/.management/stopSession")


async def test_exec_raw_403_raises_permission_error():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(403)))
    backend._http = fake_http
    with pytest.raises(SandboxPermissionError):
        await backend._exec_raw("sess-1", "echo hi", 30)


async def test_exec_raw_500_raises_sandbox_error():
    backend, _ = _aca_backend()
    fake_http = MagicMock()
    fake_http.post = MagicMock(return_value=_FakeAsyncCM(_FakeResponse(500, text_body="boom")))
    backend._http = fake_http
    with pytest.raises(SandboxError):
        await backend._exec_raw("sess-1", "echo hi", 30)


# ---------------------------------------------------------------------------
# 5. E2B
# ---------------------------------------------------------------------------


async def test_e2b_exec_raw_converts_command_exit_exception():
    backend = E2BSandboxBackend("api-key")
    fake_sandbox = SimpleNamespace(
        commands=SimpleNamespace(
            run=AsyncMock(
                side_effect=CommandExitException(
                    stdout="x", stderr="boom", exit_code=3, error=None
                )
            )
        )
    )
    backend._sandboxes["sid-1"] = fake_sandbox

    result = await backend._exec_raw("sid-1", "false", 30)

    assert result == ExecResult("x", "boom", 3)


# ---------------------------------------------------------------------------
# 6. Factory
# ---------------------------------------------------------------------------


def test_get_sandbox_unknown_provider_raises():
    with pytest.raises(ValueError, match="Unknown sandbox provider"):
        get_sandbox("nope")


def test_get_sandbox_lambda_microvm_routes_to_from_access_key():
    sentinel = object()
    with patch(
        "cloudrift.sandbox.aws_microvm.AWSMicroVMSandboxBackend.from_access_key",
        return_value=sentinel,
    ) as mock_ctor:
        result = get_sandbox(
            "lambda_microvm",
            image_identifier="arn:aws:lambda:us-east-1:111122223333:microvm-image:x",
            aws_access_key_id="AKIA...",
            aws_secret_access_key="secret",
            region="us-east-1",
        )
    assert result is sentinel
    mock_ctor.assert_called_once()
