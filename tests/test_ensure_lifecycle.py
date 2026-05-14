"""Regression tests for `_ensure()` / `close()` lifecycle on the aioboto3 backends.

These cover the bug introduced in 0.2.0 where a failed ``__aenter__`` on the
underlying aioboto3 client context manager would leave ``_client_cm`` set but
``_client`` ``None``. A subsequent ``close()`` would then call ``__aexit__`` on
a never-entered context manager, raising a second exception that masked the
original error.

Fixes verified:
1. ``_ensure()`` clears ``_client_cm`` and re-raises the original error.
2. ``close()`` swaps state out atomically, so it stays consistent even if
   ``__aexit__`` itself raises.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cloudrift.messaging.sqs import AWSSQSBackend
from cloudrift.pubsub.sns import AWSSNSBackend
from cloudrift.secrets.aws_secrets_manager import AWSSecretsManagerBackend
from cloudrift.storage.s3 import AWSS3Client


class _BoomCM:
    """Async CM that raises ``Boom`` on ``__aenter__``."""

    def __init__(self) -> None:
        self.aenter_called = False
        self.aexit_called = False

    async def __aenter__(self):
        self.aenter_called = True
        raise Boom("aenter failed")

    async def __aexit__(self, exc_type, exc, tb):
        self.aexit_called = True


class _BadExitCM:
    """Async CM that enters fine but raises on ``__aexit__``."""

    async def __aenter__(self):
        return MagicMock(name="boto-client")

    async def __aexit__(self, exc_type, exc, tb):
        raise RuntimeError("aexit failed")


class Boom(Exception):
    pass


def _fake_session(cm):
    session = MagicMock(name="session")
    session.client = MagicMock(return_value=cm)
    return session


# ---------------------------------------------------------------------------
# Fix 1: _ensure must clear _client_cm on __aenter__ failure
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "factory",
    [
        lambda s: AWSS3Client(s),
        lambda s: AWSSNSBackend(s),
        lambda s: AWSSQSBackend("https://example/queue", s),
        lambda s: AWSSecretsManagerBackend(s),
    ],
    ids=["s3", "sns", "sqs", "secrets"],
)
async def test_ensure_clears_client_cm_on_aenter_failure(factory):
    cm = _BoomCM()
    client = factory(_fake_session(cm))

    with pytest.raises(Boom, match="aenter failed"):
        await client._ensure()

    assert client._client is None
    assert client._client_cm is None
    assert cm.aenter_called
    assert not cm.aexit_called  # close() must not call __aexit__ on a never-entered CM


@pytest.mark.parametrize(
    "factory",
    [
        lambda s: AWSS3Client(s),
        lambda s: AWSSNSBackend(s),
        lambda s: AWSSQSBackend("https://example/queue", s),
        lambda s: AWSSecretsManagerBackend(s),
    ],
    ids=["s3", "sns", "sqs", "secrets"],
)
async def test_close_is_noop_after_failed_ensure(factory):
    """Calling close() after a failed _ensure() must be a clean no-op."""
    cm = _BoomCM()
    client = factory(_fake_session(cm))

    with pytest.raises(Boom):
        await client._ensure()

    await client.close()  # must not raise
    assert not cm.aexit_called


# ---------------------------------------------------------------------------
# Fix 2: close() swaps state out atomically
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "factory",
    [
        lambda s: AWSS3Client(s),
        lambda s: AWSSNSBackend(s),
        lambda s: AWSSQSBackend("https://example/queue", s),
        lambda s: AWSSecretsManagerBackend(s),
    ],
    ids=["s3", "sns", "sqs", "secrets"],
)
async def test_close_clears_state_even_if_aexit_raises(factory):
    """If __aexit__ raises during close, the object must still end up clean."""
    client = factory(_fake_session(_BadExitCM()))
    await client._ensure()
    assert client._client is not None
    assert client._client_cm is not None

    with pytest.raises(RuntimeError, match="aexit failed"):
        await client.close()

    assert client._client is None
    assert client._client_cm is None
