#!/usr/bin/env python
"""Real-cloud smoke test for the `sandbox` category.

Constructs a single sandbox backend from `SANDBOX_PROVIDER` and that
provider's env vars, then exercises the full capability envelope the
category promises: arbitrary bash, exit codes, timeouts, the detached
long-running-command protocol (package installs, clones), a real filesystem
round-trip, cross-process reconnect, and — for `lambda_microvm` only — the
suspend/resume snapshot guarantee. Nothing here is mocked; every check hits
the real provider.

    SANDBOX_PROVIDER=lambda_microvm AWS_REGION=us-east-1 \\
      MICROVM_IMAGE_ARN=arn:aws:lambda:us-east-1:123456789012:microvm-image:lyzr-sandbox \\
      MICROVM_IMAGE_VERSION=1.0 uv run python scripts/sandbox_smoke.py

    SANDBOX_PROVIDER=aca_sessions \\
      ACA_POOL_ENDPOINT=https://pool.env-id.eastus.azurecontainerapps.io \\
      uv run python scripts/sandbox_smoke.py

    SANDBOX_PROVIDER=e2b E2B_API_KEY=e2b_... uv run python scripts/sandbox_smoke.py

Anything unconfigured is reported as SKIP, not a failure. Exit code is
non-zero only if a check that actually ran failed. Set SMOKE_TRACEBACK=1 for
full tracebacks on failure.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import traceback
import uuid

from cloudrift.sandbox import get_sandbox
from cloudrift.sandbox.base import SandboxBackend

RUN_ID = uuid.uuid4().hex[:8]
WORKDIR = f"smoke-{RUN_ID}"

# ---------------------------------------------------------------------------
# Reporting — same shape as scripts/gcp_service_smoke.py
# ---------------------------------------------------------------------------

RESULTS: list[tuple[str, str, str]] = []


def record(name: str, status: str, detail: str = "") -> None:
    RESULTS.append((name, status, detail))
    icon = {"PASS": "\033[32m✓\033[0m", "FAIL": "\033[31m✗\033[0m", "SKIP": "\033[33m—\033[0m"}[status]
    print(f"  {icon} {name}" + (f"  \033[2m{detail}\033[0m" if detail else ""), flush=True)


class check:
    """Context manager recording PASS/FAIL, keeping the run going on failure."""

    def __init__(self, name: str) -> None:
        self.name = name

    async def __aenter__(self):
        self.started = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        elapsed = f"{(time.monotonic() - self.started) * 1000:.0f}ms"
        if exc is None:
            record(self.name, "PASS", elapsed)
        else:
            record(self.name, "FAIL", f"{type(exc).__name__}: {exc}")
            if os.environ.get("SMOKE_TRACEBACK"):
                traceback.print_exception(exc_type, exc, tb)
        return True  # swallow; the report carries the verdict


# ---------------------------------------------------------------------------
# Backend construction
# ---------------------------------------------------------------------------


def build_backend() -> SandboxBackend | None:
    provider = os.environ.get("SANDBOX_PROVIDER")
    if not provider:
        record("backend", "SKIP", "SANDBOX_PROVIDER unset")
        return None

    if provider == "lambda_microvm":
        region = os.environ.get("AWS_REGION")
        image_arn = os.environ.get("MICROVM_IMAGE_ARN")
        if not (region and image_arn):
            record("backend", "SKIP", "AWS_REGION or MICROVM_IMAGE_ARN unset")
            return None
        return get_sandbox(
            "lambda_microvm",
            image_identifier=image_arn,
            image_version=os.environ.get("MICROVM_IMAGE_VERSION"),
            execution_role_arn=os.environ.get("MICROVM_EXECUTION_ROLE_ARN"),
            region=region,
        )

    if provider == "aca_sessions":
        pool_endpoint = os.environ.get("ACA_POOL_ENDPOINT")
        if not pool_endpoint:
            record("backend", "SKIP", "ACA_POOL_ENDPOINT unset")
            return None
        return get_sandbox(
            "aca_sessions",
            pool_endpoint=pool_endpoint,
            client_id=os.environ.get("AZURE_CLIENT_ID"),
        )

    if provider == "e2b":
        api_key = os.environ.get("E2B_API_KEY")
        if not api_key:
            record("backend", "SKIP", "E2B_API_KEY unset")
            return None
        return get_sandbox("e2b", api_key=api_key)

    record("backend", "SKIP", f"unknown SANDBOX_PROVIDER={provider!r}")
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


async def run_checks(provider: str, backend: SandboxBackend) -> str | None:
    """Returns the opened session id, or None if allocation itself failed."""
    session_id: str | None = None

    async with check("open_session + basic exec"):
        session_id = await backend.open_session(f"smoke:{RUN_ID}", timeout_seconds=3600)
        result = await backend.exec(session_id, "echo hi")
        assert result.stdout.strip() == "hi", result
        assert result.exit_code == 0, result

    if session_id is None:
        return None

    async with check("exec propagates non-zero exit code"):
        result = await backend.exec(session_id, "exit 7")
        assert result.exit_code == 7, result

    async with check("short exec honors timeout_seconds (exit 124)"):
        result = await backend.exec(session_id, "sleep 5", timeout_seconds=1)
        assert result.exit_code == 124, result

    async with check("pip install + import (long-command protocol)"):
        result = await backend.exec(
            session_id,
            "pip install --quiet cowsay && python3 -c 'import cowsay; print(cowsay.__name__)'",
            timeout_seconds=300,
        )
        assert result.exit_code == 0, result
        assert result.stdout.strip() == "cowsay", result

    async with check("git clone (long-command protocol)"):
        result = await backend.exec(
            session_id,
            "git clone --depth 1 https://github.com/psf/requests /tmp/req && ls /tmp/req/setup.py",
            timeout_seconds=300,
        )
        assert result.exit_code == 0, result

    if provider == "e2b":
        record("root package install", "SKIP", "not applicable to e2b")
    else:
        install_cmd = (
            "dnf install -y jq"
            if provider == "lambda_microvm"
            else "apt-get update -qq && apt-get install -y -qq jq"
        )
        async with check("root package install"):
            result = await backend.exec(session_id, install_cmd, timeout_seconds=300)
            assert result.exit_code == 0, result
            result = await backend.exec(session_id, "jq --version", timeout_seconds=30)
            assert result.exit_code == 0, result

    async with check("detached command survives past any HTTP idle timeout"):
        result = await backend.exec(session_id, "sleep 150 && echo late", timeout_seconds=400)
        assert result.exit_code == 0, result
        assert result.stdout.strip() == "late", result

    async with check("filesystem round-trip: make_dir/write/read/list/rename/exists"):
        await backend.make_dir(session_id, f"{WORKDIR}/sub")
        payload = os.urandom(3_000_000)
        await backend.write_bytes(session_id, f"{WORKDIR}/sub/blob.bin", payload)
        assert await backend.read_bytes(session_id, f"{WORKDIR}/sub/blob.bin") == payload
        entries = await backend.list_dir(session_id, f"{WORKDIR}/sub")
        assert any(e.name == "blob.bin" for e in entries), entries
        await backend.rename(session_id, f"{WORKDIR}/sub/blob.bin", f"{WORKDIR}/sub/blob2.bin")
        assert await backend.exists(session_id, f"{WORKDIR}/sub/blob.bin") is False

    async with check("cross-process reconnect sees the same filesystem"):
        second = build_backend()
        assert second is not None
        assert await second.session_alive(session_id) is True
        entries = await second.list_dir(session_id, f"{WORKDIR}/sub")
        assert any(e.name == "blob2.bin" for e in entries), entries
        await second.close()

    if provider == "lambda_microvm":
        await run_suspend_resume_check(backend, session_id)

    return session_id


async def run_suspend_resume_check(backend: SandboxBackend, session_id: str) -> None:
    async with check("suspend/resume preserves memory and disk (lambda_microvm only)"):
        client = await backend._ensure()  # noqa: SLF001 - smoke script needs the raw client
        result = await backend.exec(session_id, "nohup sleep 3000 >/dev/null 2>&1 & echo $!")
        pid = result.stdout.strip()
        assert pid.isdigit(), result
        await backend.write_text(session_id, f"{WORKDIR}/sub/marker", "before-suspend")

        # Force the transition explicitly rather than waiting out the idle
        # timeout (default max_idle_seconds=900 — far past any reasonable
        # smoke-test budget).
        await client.suspend_microvm(microvmIdentifier=session_id)

        deadline = time.monotonic() + 60
        state = None
        while time.monotonic() < deadline:
            resp = await client.get_microvm(microvmIdentifier=session_id)
            state = resp.get("state")
            if state == "SUSPENDED":
                break
            await asyncio.sleep(3)
        assert state == "SUSPENDED", f"never observed SUSPENDED (last state={state})"

        assert await backend.session_alive(session_id) is True

        # Endpoint traffic auto-resumes the microVM (autoResumeEnabled=True).
        result = await backend.exec(session_id, f"cat {WORKDIR}/sub/marker; ps -p {pid} -o pid=")
        assert "before-suspend" in result.stdout, result
        assert pid in result.stdout, result


async def main() -> int:
    print(f"\033[1mcloudrift sandbox smoke test\033[0m  run={RUN_ID}")
    provider = os.environ.get("SANDBOX_PROVIDER", "<unset>")
    print(f"provider={provider}")

    backend = build_backend()
    if backend is None:
        print("\nNothing to run — set SANDBOX_PROVIDER and its env vars (see module docstring).")
        return 0

    session_id: str | None = None
    try:
        session_id = await run_checks(provider, backend)
    except Exception as e:  # a hard failure outside any `check` block
        record("smoke run", "FAIL", f"{type(e).__name__}: {e}")
        if os.environ.get("SMOKE_TRACEBACK"):
            traceback.print_exc()
    finally:
        if session_id is not None:
            async with check("close_session"):
                await backend.close_session(session_id)
                assert await backend.session_alive(session_id) is False
        await backend.close()

    passed = sum(1 for _, s, _ in RESULTS if s == "PASS")
    failed = sum(1 for _, s, _ in RESULTS if s == "FAIL")
    skipped = sum(1 for _, s, _ in RESULTS if s == "SKIP")

    print(f"\n\033[1m{'=' * 60}\033[0m")
    print(f"\033[1mpassed={passed}  failed={failed}  skipped={skipped}\033[0m")
    if failed:
        print("\nFailures:")
        for name, status, detail in RESULTS:
            if status == "FAIL":
                print(f"  \033[31m✗\033[0m {name}: {detail}")
        print("\nre-run with SMOKE_TRACEBACK=1 for full tracebacks")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
