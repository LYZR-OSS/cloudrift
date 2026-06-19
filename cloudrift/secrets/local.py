"""Non-cloud secret backends: environment variables, a JSON file, or an in-memory
mapping.

These fill the gap between "no secrets manager" and a full cloud provider — useful
for local development, self-hosted/on-prem deployments, CI, and tests. They share
the same :class:`SecretBackend` interface as the AWS/Azure backends, so swapping
``SECRETS_PROVIDER`` requires no code change.
"""
import asyncio
import json
import os

from cloudrift.core.exceptions import SecretError, SecretNotFoundError
from cloudrift.secrets.base import SecretBackend


class EnvSecretBackend(SecretBackend):
    """Read/write secrets from process environment variables.

    A secret named ``db`` maps to the environment variable ``{prefix}db`` (the
    prefix lets you namespace secrets, e.g. ``SECRET_``).
    """

    def __init__(self, prefix: str = "") -> None:
        self._prefix = prefix

    def _key(self, name: str) -> str:
        return f"{self._prefix}{name}"

    async def get_secret(self, name: str) -> str:
        try:
            return os.environ[self._key(name)]
        except KeyError as e:
            raise SecretNotFoundError(f"Secret '{name}' not found in environment") from e

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        os.environ[self._key(name)] = value

    async def delete_secret(self, name: str) -> None:
        os.environ.pop(self._key(name), None)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        names = [
            k[len(self._prefix):]
            for k in os.environ
            if k.startswith(self._prefix)
        ]
        return [n for n in names if n.startswith(prefix)]


class MappingSecretBackend(SecretBackend):
    """Hold secrets in an in-memory dict. Useful for tests and dev seeding."""

    def __init__(self, mapping: dict | None = None) -> None:
        self._store: dict[str, str] = dict(mapping or {})

    async def get_secret(self, name: str) -> str:
        try:
            return self._store[name]
        except KeyError as e:
            raise SecretNotFoundError(f"Secret '{name}' not found") from e

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        self._store[name] = value

    async def delete_secret(self, name: str) -> None:
        self._store.pop(name, None)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        return [n for n in self._store if n.startswith(prefix)]


class FileSecretBackend(SecretBackend):
    """Persist secrets in a JSON file mapping name → value (a string; store JSON
    by serializing it). Writes are atomic-ish (temp file + replace) and run in a
    worker thread so the API stays non-blocking.
    """

    def __init__(self, path: str) -> None:
        self._path = path

    def _load_sync(self) -> dict:
        if not os.path.exists(self._path):
            return {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise SecretError(f"Secret file {self._path!r} is unreadable: {e}") from e
        if not isinstance(data, dict):
            raise SecretError(f"Secret file {self._path!r} must contain a JSON object")
        return data

    def _save_sync(self, data: dict) -> None:
        tmp = f"{self._path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, self._path)

    async def get_secret(self, name: str) -> str:
        data = await asyncio.to_thread(self._load_sync)
        try:
            return data[name]
        except KeyError as e:
            raise SecretNotFoundError(f"Secret '{name}' not found in {self._path}") from e

    async def get_secret_json(self, name: str) -> dict:
        raw = await self.get_secret(name)
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError) as e:
            raise SecretError(f"Secret '{name}' is not valid JSON") from e

    async def set_secret(self, name: str, value: str) -> None:
        def _set():
            data = self._load_sync()
            data[name] = value
            self._save_sync(data)

        await asyncio.to_thread(_set)

    async def delete_secret(self, name: str) -> None:
        def _del():
            data = self._load_sync()
            data.pop(name, None)
            self._save_sync(data)

        await asyncio.to_thread(_del)

    async def list_secrets(self, prefix: str = "") -> list[str]:
        data = await asyncio.to_thread(self._load_sync)
        return [n for n in data if n.startswith(prefix)]
