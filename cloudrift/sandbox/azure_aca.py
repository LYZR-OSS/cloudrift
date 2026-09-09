import asyncio
import hashlib
import time

import aiohttp

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

# Entra tokens for the dynamic-sessions audience are cached until 5 minutes
# before they actually expire.
_TOKEN_SAFETY_MARGIN_SECONDS = 300


class AzureACASessionsBackend(SandboxBackend):
    """Azure Container Apps custom-container dynamic sessions sandbox backend.

    REST over ``aiohttp`` against the dynamic-sessions pool management
    endpoint. Entra token comes from
    :func:`cloudrift.core.azure_credentials.build_async_credential` (never
    ``azure.identity`` directly). The caller must hold the *Azure ContainerApps
    Session Executor* role on the pool.

    Use ``from_managed_identity`` to construct: Azure AD workload identity ->
    managed identity -> Azure CLI, same chain every Azure backend uses.
    """

    def __init__(
        self,
        pool_endpoint: str,
        credential,
        *,
        api_version: str = "2025-02-02-preview",
        exec_path: str = "/exec",
        read_chunk_bytes: int = DEFAULT_READ_CHUNK_BYTES,
        write_chunk_b64: int = DEFAULT_WRITE_CHUNK_B64,
    ) -> None:
        super().__init__(read_chunk_bytes=read_chunk_bytes, write_chunk_b64=write_chunk_b64)
        self._pool_endpoint = pool_endpoint.rstrip("/")
        self._credential = credential
        self._api_version = api_version
        self._exec_path = exec_path

        self._http: aiohttp.ClientSession | None = None
        self._http_lock = asyncio.Lock()
        self._token_cache: tuple[str, float] | None = None

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_managed_identity(
        cls,
        pool_endpoint: str,
        client_id: str | None = None,
        credential_options: dict | None = None,
        **kwargs,
    ) -> "AzureACASessionsBackend":
        """Authenticate via Azure AD: workload identity → managed identity → az CLI.

        ``client_id`` selects a user-assigned managed identity; omit it for
        the system-assigned one. ``credential_options`` is forwarded to
        ``DefaultAzureCredential`` — see :mod:`cloudrift.core.azure_credentials`.
        The backend owns the returned credential and closes it in
        :meth:`close`.
        """
        from cloudrift.core.azure_credentials import build_async_credential

        credential = build_async_credential(client_id, **(credential_options or {}))
        return cls(pool_endpoint, credential, **kwargs)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _http_session(self) -> aiohttp.ClientSession:
        if self._http is not None:
            return self._http
        async with self._http_lock:
            if self._http is None:
                self._http = aiohttp.ClientSession()
        return self._http

    async def _token(self) -> str:
        now = time.time()
        if self._token_cache is not None and self._token_cache[1] > now:
            return self._token_cache[0]
        token = await self._credential.get_token("https://dynamicsessions.io/.default")
        self._token_cache = (token.token, token.expires_on - _TOKEN_SAFETY_MARGIN_SECONDS)
        return token.token

    async def close(self) -> None:
        http, self._http = self._http, None
        if http is not None:
            await http.close()
        if self._credential is not None and hasattr(self._credential, "close"):
            await self._credential.close()

    # ------------------------------------------------------------------
    # SandboxBackend implementation
    # ------------------------------------------------------------------

    async def open_session(self, key: str, *, timeout_seconds: int = 3600) -> str:
        # No network call: ACA allocates a session on the first request to an
        # unseen identifier. `timeout_seconds` is ignored — session lifetime
        # is the pool's own `cooldownPeriodInSeconds`. A 64-char hex digest
        # satisfies ACA's identifier charset (alphanumerics only, 4-128
        # chars), where a raw "api_key:session_id" would not (":" excluded).
        return hashlib.sha256(key.encode()).hexdigest()

    async def session_alive(self, session_id: str) -> bool:
        http = await self._http_session()
        token = await self._token()
        url = f"{self._pool_endpoint}/.management/getSession"
        params = {"identifier": session_id, "api-version": self._api_version}
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with http.post(url, params=params, headers=headers) as resp:
                if resp.status == 200:
                    return True
                if resp.status == 404:
                    return False
                if resp.status == 400:
                    body = await resp.text()
                    if "SessionWithIdentifierNotFound" in body:
                        return False
                text = await resp.text()
                raise SandboxError(f"getSession failed with status {resp.status}: {text[:500]}")
        except aiohttp.ClientError as exc:
            raise SandboxError(f"getSession request failed: {exc}") from exc

    async def close_session(self, session_id: str) -> None:
        http = await self._http_session()
        token = await self._token()
        url = f"{self._pool_endpoint}/.management/stopSession"
        params = {"identifier": session_id, "api-version": self._api_version}
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with http.post(url, params=params, headers=headers) as resp:
                if resp.status in (200, 204, 400, 404):
                    return
                text = await resp.text()
                raise SandboxError(f"stopSession failed with status {resp.status}: {text[:500]}")
        except aiohttp.ClientError as exc:
            raise SandboxError(f"stopSession request failed: {exc}") from exc

    async def _exec_raw(self, session_id: str, command: str, timeout_seconds: int) -> ExecResult:
        http = await self._http_session()
        token = await self._token()
        url = f"{self._pool_endpoint}{self._exec_path}"
        params = {"identifier": session_id}
        headers = {"Authorization": f"Bearer {token}"}
        body = {"command": command, "timeout_seconds": timeout_seconds}
        try:
            async with http.post(
                url,
                params=params,
                headers=headers,
                json=body,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds + 30),
            ) as resp:
                if resp.status in (401, 403):
                    raise SandboxPermissionError(
                        f"authorization failed for ACA session {session_id!r}"
                    )
                if resp.status == 404:
                    raise SandboxSessionNotFoundError(f"ACA session not found: {session_id}")
                if not 200 <= resp.status < 300:
                    text = await resp.text()
                    raise SandboxError(
                        f"ACA exec failed with status {resp.status}: {text[:500]}"
                    )
                data = await resp.json()
                return ExecResult(
                    stdout=data.get("stdout", ""),
                    stderr=data.get("stderr", ""),
                    exit_code=int(data.get("exit_code", -1)),
                )
        except aiohttp.ClientError as exc:
            raise SandboxError(f"ACA exec request failed: {exc}") from exc
