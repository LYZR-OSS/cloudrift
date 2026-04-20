import redis.asyncio as aioredis
from redis.credentials import CredentialProvider

from cloudrift.cache.base import CacheBackend, _RedisMixin
from cloudrift.core.exceptions import CacheConnectionError


class AWSElastiCacheBackend(_RedisMixin, CacheBackend):
    """AWS ElastiCache (Redis) backend.

    Use one of the class methods to construct:
    - ``from_auth_token`` — Redis AUTH token (ElastiCache auth token / password)
    - ``from_iam_auth``   — IAM-based authentication (ElastiCache Redis 7+, SigV4)
    - ``from_tls_cert``   — mTLS with client certificate and key files
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_auth_token(
        cls,
        host: str,
        port: int = 6379,
        auth_token: str | None = None,
        db: int = 0,
        ssl: bool = True,
        ssl_ca_certs: str | None = None,
    ) -> "AWSElastiCacheBackend":
        """Connect using an ElastiCache AUTH token (shared secret).

        For ElastiCache clusters with *Transit Encryption* enabled, set ``ssl=True``
        (the default). ``auth_token`` maps to the Redis ``AUTH`` password.

        Args:
            host: ElastiCache primary endpoint hostname.
            port: Redis port (default 6379).
            auth_token: ElastiCache auth token configured on the cluster.
            db: Database index (default 0).
            ssl: Enable TLS in-transit encryption (default ``True``).
            ssl_ca_certs: Optional path to the CA bundle (PEM) for server verification.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=auth_token,
                db=db,
                ssl=ssl,
                ssl_ca_certs=ssl_ca_certs,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to ElastiCache: {e}") from e

    @classmethod
    def from_iam_auth(
        cls,
        host: str,
        username: str,
        region: str,
        port: int = 6379,
        db: int = 0,
        ssl: bool = True,
        ssl_ca_certs: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
    ) -> "AWSElastiCacheBackend":
        """Connect using IAM-based authentication (ElastiCache Redis 7+ with IAM enabled).

        A short-lived SigV4 token is generated at connection time and refreshed
        automatically on reconnect via a ``CredentialProvider``.

        Args:
            host: ElastiCache primary endpoint hostname.
            username: Redis ACL username that maps to an IAM identity.
            region: AWS region (e.g. ``us-east-1``).
            port: Redis port (default 6379).
            db: Database index (default 0).
            ssl: Enable TLS (default ``True``; required for IAM auth).
            ssl_ca_certs: Optional path to the CA bundle (PEM).
            aws_access_key_id: Optional explicit AWS access key.
            aws_secret_access_key: Optional explicit AWS secret key.
            aws_session_token: Optional STS session token.
            profile_name: Optional named AWS credentials profile.
        """
        try:
            provider = _ElastiCacheIAMProvider(
                host=host,
                port=port,
                username=username,
                region=region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                profile_name=profile_name,
            )
            client = aioredis.Redis(
                host=host,
                port=port,
                db=db,
                ssl=ssl,
                ssl_ca_certs=ssl_ca_certs,
                credential_provider=provider,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to ElastiCache (IAM): {e}") from e

    @classmethod
    def from_tls_cert(
        cls,
        host: str,
        port: int = 6380,
        auth_token: str | None = None,
        db: int = 0,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_ca_certs: str | None = None,
    ) -> "AWSElastiCacheBackend":
        """Connect using mutual TLS (mTLS) with a client certificate and key.

        Args:
            host: ElastiCache primary endpoint hostname.
            port: Redis TLS port (default 6380).
            auth_token: Optional ElastiCache AUTH token.
            db: Database index (default 0).
            ssl_certfile: Path to the client certificate PEM file.
            ssl_keyfile: Path to the client private key PEM file.
            ssl_ca_certs: Path to the CA certificate bundle (PEM) for server verification.
        """
        try:
            client = aioredis.Redis(
                host=host,
                port=port,
                password=auth_token,
                db=db,
                ssl=True,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                ssl_ca_certs=ssl_ca_certs,
            )
            return cls(client)
        except Exception as e:
            raise CacheConnectionError(f"Failed to connect to ElastiCache (mTLS): {e}") from e


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _ElastiCacheIAMProvider(CredentialProvider):
    """Generates and refreshes a SigV4-signed IAM auth token for ElastiCache Redis 7+."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        region: str,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._region = region
        self._access_key = aws_access_key_id
        self._secret_key = aws_secret_access_key
        self._session_token = aws_session_token
        self._profile_name = profile_name

    def get_credentials(self) -> tuple[str, str]:
        token = _generate_iam_token(
            host=self._host,
            port=self._port,
            username=self._username,
            region=self._region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            aws_session_token=self._session_token,
            profile_name=self._profile_name,
        )
        return self._username, token


def _generate_iam_token(
    host: str,
    port: int,
    username: str,
    region: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_session_token: str | None = None,
    profile_name: str | None = None,
) -> str:
    """Generate a short-lived SigV4-signed token for ElastiCache IAM auth.

    Tokens are valid for 15 minutes; redis-py re-calls ``get_credentials()``
    on each new connection, ensuring tokens stay fresh.
    """
    import boto3
    import botocore.auth
    import botocore.awsrequest

    session = boto3.Session(
        profile_name=profile_name,
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
    )
    credentials = session.get_credentials().get_frozen_credentials()

    url = f"https://{host}:{port}/?Action=connect&User={username}"
    request = botocore.awsrequest.AWSRequest(method="GET", url=url)
    signer = botocore.auth.SigV4QueryAuth(credentials, "elasticache", region, expires=900)
    signer.add_auth(request)

    # Redis expects the signed URL as the password (without the https:// scheme)
    return request.url.replace("https://", "", 1)
