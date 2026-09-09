import asyncio
import hashlib
import time

import aiohttp
from aiobotocore.session import AioSession
from botocore.exceptions import ClientError

from cloudrift.core.aws_session import build_session
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
)

# The service caps auth-token TTL at 60 minutes; mint tokens for a third of
# that and refresh 5 minutes before they actually expire.
_TOKEN_TTL_MINUTES = 30
_TOKEN_SAFETY_MARGIN_SECONDS = 300
_LIVE_STATES = frozenset({"PENDING", "RUNNING", "SUSPENDING", "SUSPENDED"})


def _normalize_endpoint(raw: str) -> str:
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    return f"https://{raw}"


class AWSMicroVMSandboxBackend(SandboxBackend):
    """AWS Lambda MicroVMs sandbox backend.

    Control plane through ``aiobotocore`` (service ``lambda-microvms``), data
    plane through ``aiohttp`` against the per-microVM HTTPS endpoint. Each
    session is a Firecracker microVM built from a caller-supplied container
    image, with snapshot suspend/resume: a suspended microVM costs no compute
    and the service auto-resumes it transparently on the next request, so
    this backend exposes no explicit ``suspend``/``resume`` methods.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials (+ optional session token)
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``
    """

    def __init__(
        self,
        session: AioSession,
        *,
        image_identifier: str,
        image_version: str | None = None,
        execution_role_arn: str | None = None,
        ingress_connector_arn: str | None = None,
        egress_connector_arn: str | None = None,
        region: str | None = None,
        max_idle_seconds: int = 900,
        suspended_seconds: int = 3600,
        startup_timeout_seconds: int = 60,
        read_chunk_bytes: int = DEFAULT_READ_CHUNK_BYTES,
        write_chunk_b64: int = DEFAULT_WRITE_CHUNK_B64,
    ) -> None:
        super().__init__(read_chunk_bytes=read_chunk_bytes, write_chunk_b64=write_chunk_b64)
        self._session = session
        self._image_identifier = image_identifier
        self._image_version = image_version
        self._execution_role_arn = execution_role_arn

        resolved_region = region or session.get_config_variable("region")
        if not resolved_region:
            raise ValueError("region is required for the Lambda MicroVMs sandbox backend")
        self._region = resolved_region
        self._ingress_connector_arn = ingress_connector_arn or (
            f"arn:aws:lambda:{resolved_region}:aws:network-connector:"
            "aws-network-connector:ALL_INGRESS"
        )
        self._egress_connector_arn = egress_connector_arn or (
            f"arn:aws:lambda:{resolved_region}:aws:network-connector:"
            "aws-network-connector:INTERNET_EGRESS"
        )

        self._max_idle_seconds = max_idle_seconds
        self._suspended_seconds = suspended_seconds
        self._startup_timeout_seconds = startup_timeout_seconds

        self._client_cm = None
        self._client = None
        self._lock = asyncio.Lock()

        self._http: aiohttp.ClientSession | None = None
        self._http_lock = asyncio.Lock()

        # Per-microVM state cached in-process.
        self._endpoints: dict[str, str] = {}
        self._tokens: dict[str, tuple[str, float]] = {}

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        *,
        aws_session_token: str | None = None,
        region: str | None = None,
        **kwargs,
    ) -> "AWSMicroVMSandboxBackend":
        """Authenticate with explicit access key / secret."""
        session = build_session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region=region,
        )
        return cls(session, region=region, **kwargs)

    @classmethod
    def from_iam_role(
        cls, *, region: str | None = None, **kwargs
    ) -> "AWSMicroVMSandboxBackend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = build_session(region=region)
        return cls(session, region=region, **kwargs)

    @classmethod
    def from_profile(
        cls, profile_name: str, *, region: str | None = None, **kwargs
    ) -> "AWSMicroVMSandboxBackend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = build_session(profile_name=profile_name, region=region)
        return cls(session, region=region, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.create_client("lambda-microvms")
                try:
                    self._client = await self._client_cm.__aenter__()
                except Exception:
                    self._client_cm = None
                    raise
        return self._client

    async def _http_session(self) -> aiohttp.ClientSession:
        if self._http is not None:
            return self._http
        async with self._http_lock:
            if self._http is None:
                self._http = aiohttp.ClientSession()
        return self._http

    async def close(self) -> None:
        client_cm, self._client_cm = self._client_cm, None
        self._client = None
        if client_cm is not None:
            await client_cm.__aexit__(None, None, None)
        http, self._http = self._http, None
        if http is not None:
            await http.close()

    # ------------------------------------------------------------------
    # SandboxBackend implementation
    # ------------------------------------------------------------------

    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        client = await self._ensure()
        kwargs: dict = {
            "imageIdentifier": self._image_identifier,
            "ingressNetworkConnectors": [self._ingress_connector_arn],
            "egressNetworkConnectors": [self._egress_connector_arn],
            "idlePolicy": {
                "autoResumeEnabled": True,
                "maxIdleDurationSeconds": self._max_idle_seconds,
                "suspendedDurationSeconds": self._suspended_seconds,
            },
            "maximumDurationInSeconds": min(int(timeout_seconds), 28800),
            "clientToken": hashlib.sha256(key.encode()).hexdigest()[:128],
        }
        if self._image_version:
            kwargs["imageVersion"] = self._image_version
        if self._execution_role_arn:
            kwargs["executionRoleArn"] = self._execution_role_arn
        try:
            resp = await client.run_microvm(**kwargs)
        except ClientError as exc:
            self._raise(exc, key)

        microvm_id = resp["microvmId"]
        endpoint = _normalize_endpoint(resp["endpoint"])
        self._endpoints[microvm_id] = endpoint

        # Wait for readiness by connecting, not by polling `state` (the docs
        # state it is eventually consistent) — this way the first exec never
        # races the snapshot restore. All requests, including health checks,
        # require the auth token — there is no unauthenticated path.
        http = await self._http_session()
        delay = 0.5
        deadline = time.monotonic() + self._startup_timeout_seconds
        last_status = "no response"
        while time.monotonic() < deadline:
            try:
                token = await self._token(microvm_id)
                headers = {"X-aws-proxy-auth": token, "X-aws-proxy-port": "8080"}
                async with http.get(
                    f"{endpoint}/health", headers=headers, timeout=aiohttp.ClientTimeout(total=5)
                ) as resp_health:
                    if resp_health.status == 200:
                        return microvm_id
                    if resp_health.status in (401, 403):
                        # Token may have been minted before the microVM finished
                        # registering; force a fresh one on the next attempt.
                        self._tokens.pop(microvm_id, None)
                    last_status = str(resp_health.status)
            except aiohttp.ClientError as exc:
                last_status = str(exc)
            await asyncio.sleep(delay)
            delay = min(delay * 1.5, 5.0)
        raise SandboxError(
            f"MicroVM {microvm_id!r} did not become ready within "
            f"{self._startup_timeout_seconds}s (last status: {last_status})"
        )

    async def session_alive(self, session_id: str) -> bool:
        client = await self._ensure()
        try:
            resp = await client.get_microvm(microvmIdentifier=session_id)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                return False
            self._raise(exc, session_id)
        state = resp.get("state")
        if state in _LIVE_STATES:
            endpoint = resp.get("endpoint")
            if endpoint:
                self._endpoints[session_id] = _normalize_endpoint(endpoint)
            return True
        return False

    async def close_session(self, session_id: str) -> None:
        client = await self._ensure()
        try:
            await client.terminate_microvm(microvmIdentifier=session_id)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("ResourceNotFoundException", "ConflictException"):
                pass
            else:
                self._raise(exc, session_id)
        finally:
            self._endpoints.pop(session_id, None)
            self._tokens.pop(session_id, None)

    async def _endpoint(self, session_id: str) -> str:
        cached = self._endpoints.get(session_id)
        if cached is not None:
            return cached
        client = await self._ensure()
        try:
            resp = await client.get_microvm(microvmIdentifier=session_id)
        except ClientError as exc:
            self._raise(exc, session_id)
        endpoint = _normalize_endpoint(resp["endpoint"])
        self._endpoints[session_id] = endpoint
        return endpoint

    async def _token(self, session_id: str, *, force_refresh: bool = False) -> str:
        now = time.time()
        cached = self._tokens.get(session_id)
        if cached is not None and not force_refresh and cached[1] > now:
            return cached[0]
        client = await self._ensure()
        try:
            resp = await client.create_microvm_auth_token(
                microvmIdentifier=session_id,
                expirationInMinutes=_TOKEN_TTL_MINUTES,
                allowedPorts=[{"port": 8080}],
            )
        except ClientError as exc:
            self._raise(exc, session_id)
        token = resp["authToken"]["X-aws-proxy-auth"]
        safe_expiry = now + _TOKEN_TTL_MINUTES * 60 - _TOKEN_SAFETY_MARGIN_SECONDS
        self._tokens[session_id] = (token, safe_expiry)
        return token

    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        endpoint = await self._endpoint(session_id)
        http = await self._http_session()
        body = {"command": command, "timeout_seconds": timeout_seconds}
        retried_auth = False
        retried_transient = False
        while True:
            token = await self._token(session_id, force_refresh=retried_auth)
            headers = {"X-aws-proxy-auth": token, "X-aws-proxy-port": "8080"}
            try:
                async with http.post(
                    f"{endpoint}/exec",
                    json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout_seconds + 60),
                ) as resp:
                    if resp.status in (401, 403):
                        if retried_auth:
                            raise SandboxPermissionError(
                                f"authorization failed for microVM {session_id!r} "
                                "after token refresh"
                            )
                        retried_auth = True
                        continue
                    if resp.status in (502, 503, 504):
                        if retried_transient:
                            raise SandboxError(
                                f"microVM {session_id!r} exec failed with status {resp.status}"
                            )
                        retried_transient = True
                        await asyncio.sleep(2)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        raise SandboxError(
                            f"microVM {session_id!r} exec failed with status "
                            f"{resp.status}: {text[:500]}"
                        )
                    data = await resp.json()
                    return ExecResult(
                        stdout=data.get("stdout", ""),
                        stderr=data.get("stderr", ""),
                        exit_code=int(data.get("exit_code", -1)),
                    )
            except aiohttp.ClientError as exc:
                raise SandboxError(f"microVM {session_id!r} exec request failed: {exc}") from exc

    def _raise(self, exc: ClientError, session_id: str):
        code = exc.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            raise SandboxSessionNotFoundError(f"MicroVM session not found: {session_id}") from exc
        if code == "AccessDeniedException":
            raise SandboxPermissionError(
                f"Access denied for microVM session: {session_id}"
            ) from exc
        raise SandboxError(str(exc)) from exc
