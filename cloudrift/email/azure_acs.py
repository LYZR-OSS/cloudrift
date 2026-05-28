import asyncio
import base64

from cloudrift.core.exceptions import (
    EmailError,
    EmailSendError,
    EmailThrottledError,
    RecipientRejectedError,
    SenderUnverifiedError,
)
from cloudrift.email.base import EmailBackend, _as_list


class AzureACSEmailBackend(EmailBackend):
    """Azure Communication Services Email backend.

    Uses the sync ``azure.communication.email.EmailClient`` SDK (the async
    variant is not GA at the time of writing) and wraps blocking calls with
    ``asyncio.to_thread`` so the public surface stays async-only.

    Use one of the class methods to construct:
    - ``from_connection_string`` — ACS resource connection string
    - ``from_managed_identity``  — Managed Identity (system or user-assigned)
    - ``from_service_principal`` — Azure AD service principal (client secret)

    Every constructor takes a ``default_from`` (a verified
    ``MailFrom`` / ``senderAddress`` on a domain linked to the ACS resource).
    """

    def __init__(
        self,
        *,
        endpoint: str | None,
        default_from: str | None,
        connection_string: str | None = None,
        credential=None,
    ) -> None:
        self._endpoint = endpoint
        self._connection_string = connection_string
        self._credential = credential
        self._default_from = default_from
        self._client = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_connection_string(
        cls,
        connection_string: str,
        default_from: str | None = None,
    ) -> "AzureACSEmailBackend":
        """Authenticate with an ACS connection string."""
        return cls(
            endpoint=None,
            default_from=default_from,
            connection_string=connection_string,
        )

    @classmethod
    def from_managed_identity(
        cls,
        endpoint: str,
        default_from: str | None = None,
        client_id: str | None = None,
    ) -> "AzureACSEmailBackend":
        """Authenticate via Azure Managed Identity."""
        from azure.identity import ManagedIdentityCredential

        credential = (
            ManagedIdentityCredential(client_id=client_id)
            if client_id
            else ManagedIdentityCredential()
        )
        return cls(endpoint=endpoint, default_from=default_from, credential=credential)

    @classmethod
    def from_service_principal(
        cls,
        endpoint: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        default_from: str | None = None,
    ) -> "AzureACSEmailBackend":
        """Authenticate via Azure AD service principal (client secret)."""
        from azure.identity import ClientSecretCredential

        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        return cls(endpoint=endpoint, default_from=default_from, credential=credential)

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                from azure.communication.email import EmailClient

                if self._connection_string is not None:
                    self._client = EmailClient.from_connection_string(self._connection_string)
                else:
                    self._client = EmailClient(self._endpoint, self._credential)
        return self._client

    async def close(self) -> None:
        client, self._client = self._client, None
        if client is not None and hasattr(client, "close"):
            await asyncio.to_thread(client.close)
        if self._credential is not None and hasattr(self._credential, "close"):
            # azure.identity sync credentials have a sync close().
            await asyncio.to_thread(self._credential.close)

    # ------------------------------------------------------------------
    # EmailBackend implementation
    # ------------------------------------------------------------------

    async def send(
        self,
        to,
        subject,
        *,
        body_text=None,
        body_html=None,
        from_=None,
        cc=None,
        bcc=None,
        reply_to=None,
        attachments=None,
        headers=None,
    ) -> str:
        sender = from_ or self._default_from
        if not sender:
            raise EmailError(
                "No sender address: pass from_=... or set default_from on the backend."
            )
        if body_text is None and body_html is None:
            raise EmailError("send() requires body_text and/or body_html.")

        client = await self._ensure()

        content: dict = {"subject": subject}
        if body_text is not None:
            content["plainText"] = body_text
        if body_html is not None:
            content["html"] = body_html

        recipients: dict = {"to": [{"address": addr} for addr in _as_list(to)]}
        if cc:
            recipients["cc"] = [{"address": addr} for addr in _as_list(cc)]
        if bcc:
            recipients["bcc"] = [{"address": addr} for addr in _as_list(bcc)]

        message: dict = {
            "senderAddress": sender,
            "recipients": recipients,
            "content": content,
        }
        if reply_to:
            message["replyTo"] = [{"address": addr} for addr in _as_list(reply_to)]
        if attachments:
            message["attachments"] = [
                {
                    "name": att.filename,
                    "contentType": att.content_type,
                    "contentInBase64": base64.b64encode(att.content).decode("ascii"),
                }
                for att in attachments
            ]
        if headers:
            message["headers"] = dict(headers)

        try:
            poller = await asyncio.to_thread(client.begin_send, message)
            result = await asyncio.to_thread(poller.result)
        except Exception as e:
            self._raise(e)
        # ACS returns an object with an ``id`` attr (or dict-like). Be forgiving.
        if isinstance(result, dict):
            return str(result.get("id") or result.get("messageId") or "")
        return str(getattr(result, "id", None) or getattr(result, "message_id", "") or "")

    def _raise(self, exc: Exception):
        from azure.core.exceptions import HttpResponseError

        if isinstance(exc, HttpResponseError):
            status = getattr(exc, "status_code", None)
            message = str(exc)
            if status == 429:
                raise EmailThrottledError(message) from exc
            if status == 403 and "DomainNotLinked" in message:
                raise SenderUnverifiedError(message) from exc
            if status == 400 and (
                "InvalidRecipient" in message or "InvalidAddress" in message
            ):
                raise RecipientRejectedError(message) from exc
            raise EmailSendError(message) from exc
        raise EmailSendError(str(exc)) from exc
