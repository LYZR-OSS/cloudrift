class CloudRiftError(Exception):
    """Base exception for all cloudrift errors."""


# Storage exceptions
class StorageError(CloudRiftError):
    """Base exception for storage operations."""


class ObjectNotFoundError(StorageError):
    """Raised when the requested object does not exist."""


class StoragePermissionError(StorageError):
    """Raised on storage access permission failures."""


# Messaging exceptions
class MessagingError(CloudRiftError):
    """Base exception for messaging operations."""


class QueueNotFoundError(MessagingError):
    """Raised when the target queue does not exist."""


class MessageSendError(MessagingError):
    """Raised when a message fails to send."""


# Document DB exceptions
class DocumentConnectionError(CloudRiftError):
    """Raised when a document database connection cannot be established."""


# Cache exceptions
class CacheError(CloudRiftError):
    """Base exception for cache operations."""


class CacheConnectionError(CacheError):
    """Raised when a cache connection cannot be established."""


class CacheKeyNotFoundError(CacheError):
    """Raised when the requested key does not exist."""


# Secret exceptions
class SecretError(CloudRiftError):
    """Base exception for secret operations."""


class SecretNotFoundError(SecretError):
    """Raised when the requested secret does not exist."""


class SecretPermissionError(SecretError):
    """Raised on secret access permission failures."""


# Pub/Sub exceptions
class PubSubError(CloudRiftError):
    """Base exception for pub/sub operations."""


class TopicNotFoundError(PubSubError):
    """Raised when the target topic does not exist."""


class PublishError(PubSubError):
    """Raised when a message fails to publish."""
