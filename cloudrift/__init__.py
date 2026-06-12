from cloudrift.storage import get_storage
from cloudrift.messaging import get_queue
from cloudrift.document import get_mongodb, get_mongodb_sync
from cloudrift.cache import get_cache
from cloudrift.secrets import get_secrets
from cloudrift.pubsub import get_pubsub
from cloudrift.email import get_email

__version__ = "0.2.3"
__all__ = [
    "get_storage",
    "get_queue",
    "get_mongodb",
    "get_mongodb_sync",
    "get_cache",
    "get_secrets",
    "get_pubsub",
    "get_email",
]
