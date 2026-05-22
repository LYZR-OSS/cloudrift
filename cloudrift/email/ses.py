import asyncio
from email.message import EmailMessage as MIMEEmailMessage

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from cloudrift.core.exceptions import (
    EmailError,
    EmailSendError,
    EmailThrottledError,
    RecipientRejectedError,
    SenderUnverifiedError,
)
from cloudrift.email.base import Attachment, EmailBackend, _as_list


class AWSSESBackend(EmailBackend):
    """AWS SES email backend (native async via ``aioboto3``, SESv2 API).

    A single async SESv2 client is created lazily on first use and reused for
    the lifetime of the backend.

    Use one of the class methods to construct:
    - ``from_access_key`` — static credentials
    - ``from_iam_role``   — instance profile / environment / ECS task role
    - ``from_profile``    — named profile from ``~/.aws/credentials``

    Every constructor takes a ``default_from`` (a verified SES sender). The
    ``from_`` argument on :meth:`send` overrides it per call.
    """

    def __init__(
        self,
        session: aioboto3.Session,
        *,
        default_from: str | None = None,
        endpoint_url: str | None = None,
        max_pool_connections: int = 25,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        client_kwargs: dict | None = None,
    ) -> None:
        self._session = session
        self._default_from = default_from
        self._endpoint_url = endpoint_url
        self._config = Config(
            max_pool_connections=max_pool_connections,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._client_kwargs = client_kwargs or {}
        self._client_cm = None
        self._client = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_access_key(
        cls,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        default_from: str | None = None,
        region: str = "us-east-1",
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSESBackend":
        """Authenticate with explicit access key / secret."""
        session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region,
        )
        return cls(
            session,
            default_from=default_from,
            endpoint_url=endpoint_url,
            **kwargs,
        )

    @classmethod
    def from_iam_role(
        cls,
        default_from: str | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSESBackend":
        """Authenticate via IAM role / instance profile / environment variables."""
        session = aioboto3.Session(region_name=region)
        return cls(
            session,
            default_from=default_from,
            endpoint_url=endpoint_url,
            **kwargs,
        )

    @classmethod
    def from_profile(
        cls,
        profile_name: str,
        default_from: str | None = None,
        region: str = "us-east-1",
        endpoint_url: str | None = None,
        **kwargs,
    ) -> "AWSSESBackend":
        """Authenticate using a named profile from ``~/.aws/credentials``."""
        session = aioboto3.Session(profile_name=profile_name, region_name=region)
        return cls(
            session,
            default_from=default_from,
            endpoint_url=endpoint_url,
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Internal lifecycle
    # ------------------------------------------------------------------

    async def _ensure(self):
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client_cm = self._session.client(
                    "sesv2",
                    endpoint_url=self._endpoint_url,
                    config=self._config,
                    **self._client_kwargs,
                )
                try:
                    self._client = await self._client_cm.__aenter__()
                except Exception:
                    self._client_cm = None
                    raise
        return self._client

    async def close(self) -> None:
        client_cm, self._client_cm = self._client_cm, None
        self._client = None
        if client_cm is not None:
            await client_cm.__aexit__(None, None, None)

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

        to_list = _as_list(to)
        cc_list = _as_list(cc)
        bcc_list = _as_list(bcc)
        client = await self._ensure()

        if attachments or headers:
            raw = _build_mime(
                sender=sender,
                to=to_list,
                cc=cc_list,
                bcc=bcc_list,
                reply_to=_as_list(reply_to),
                subject=subject,
                body_text=body_text,
                body_html=body_html,
                attachments=attachments or [],
                headers=headers or {},
            )
            try:
                response = await client.send_email(
                    FromEmailAddress=sender,
                    Destination={
                        "ToAddresses": to_list,
                        "CcAddresses": cc_list,
                        "BccAddresses": bcc_list,
                    },
                    Content={"Raw": {"Data": raw}},
                    ReplyToAddresses=_as_list(reply_to),
                )
                return response["MessageId"]
            except ClientError as e:
                self._raise(e)

        body: dict = {}
        if body_text is not None:
            body["Text"] = {"Data": body_text, "Charset": "UTF-8"}
        if body_html is not None:
            body["Html"] = {"Data": body_html, "Charset": "UTF-8"}

        try:
            response = await client.send_email(
                FromEmailAddress=sender,
                Destination={
                    "ToAddresses": to_list,
                    "CcAddresses": cc_list,
                    "BccAddresses": bcc_list,
                },
                Content={
                    "Simple": {
                        "Subject": {"Data": subject, "Charset": "UTF-8"},
                        "Body": body,
                    }
                },
                ReplyToAddresses=_as_list(reply_to),
            )
            return response["MessageId"]
        except ClientError as e:
            self._raise(e)

    async def health_check(self) -> bool:
        try:
            client = await self._ensure()
            # ``list_email_identities`` is the cheapest read on SESv2 that
            # exercises both auth and the data path. ``get_account`` would be
            # smaller but is not implemented by moto.
            await client.list_email_identities()
            return True
        except Exception:
            return False

    def _raise(self, exc: ClientError):
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("MessageRejected",):
            raise RecipientRejectedError(str(exc)) from exc
        if code in (
            "MailFromDomainNotVerified",
            "MailFromDomainNotVerifiedException",
            "FromEmailAddressNotVerified",
        ):
            raise SenderUnverifiedError(str(exc)) from exc
        if code in ("Throttling", "TooManyRequestsException", "SendingPausedException"):
            raise EmailThrottledError(str(exc)) from exc
        raise EmailSendError(str(exc)) from exc


def _build_mime(
    *,
    sender: str,
    to: list[str],
    cc: list[str],
    bcc: list[str],
    reply_to: list[str],
    subject: str,
    body_text: str | None,
    body_html: str | None,
    attachments: list[Attachment],
    headers: dict[str, str],
) -> bytes:
    """Build a raw MIME message for SES SendEmail Content.Raw."""
    msg = MIMEEmailMessage()
    msg["From"] = sender
    if to:
        msg["To"] = ", ".join(to)
    if cc:
        msg["Cc"] = ", ".join(cc)
    if reply_to:
        msg["Reply-To"] = ", ".join(reply_to)
    msg["Subject"] = subject
    for header, value in headers.items():
        msg[header] = value

    if body_text is not None and body_html is not None:
        msg.set_content(body_text)
        msg.add_alternative(body_html, subtype="html")
    elif body_html is not None:
        msg.set_content(body_html, subtype="html")
    else:
        msg.set_content(body_text or "")

    for att in attachments:
        maintype, _, subtype = att.content_type.partition("/")
        msg.add_attachment(
            att.content,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=att.filename,
        )

    return msg.as_bytes()
