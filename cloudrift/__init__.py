from importlib.metadata import PackageNotFoundError, version

from cloudrift.storage import get_storage
from cloudrift.messaging import get_queue
from cloudrift.document import get_mongodb, get_mongodb_sync
from cloudrift.cache import get_cache, cache_broker_url
from cloudrift.secrets import get_secrets
from cloudrift.sql import get_sql
from cloudrift.crypto import get_crypto
from cloudrift.pubsub import get_pubsub
from cloudrift.email import get_email

try:
    __version__ = version("lyzr-cloudrift")
except PackageNotFoundError:  # pragma: no cover - only when genuinely uninstalled
    __version__ = "0.0.0+unknown"
__all__ = [
    "get_storage",
    "get_queue",
    "get_mongodb",
    "get_mongodb_sync",
    "get_cache",
    "cache_broker_url",
    "get_secrets",
    "get_sql",
    "get_crypto",
    "get_pubsub",
    "get_email",
]
