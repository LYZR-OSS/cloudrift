from cloudrift.storage import get_storage
from cloudrift.messaging import get_queue
from cloudrift.document import get_mongodb
from cloudrift.cache import get_cache
from cloudrift.secrets import get_secrets
from cloudrift.pubsub import get_pubsub

__version__ = "0.2.0"
__all__ = [
    "get_storage",
    "get_queue",
    "get_mongodb",
    "get_cache",
    "get_secrets",
    "get_pubsub",
]
