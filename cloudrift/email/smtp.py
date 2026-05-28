"""Raw SMTP email backend (SendGrid, Mailgun, Postmark, Office365, MailHog, ...).

A fresh ``aiosmtplib.SMTP`` connection is opened per :meth:`send`. SMTP servers
commonly drop idle connections and the simplicity is worth more than the
marginal latency win — transactional volumes don't benefit from pooling.
"""
from email.message import EmailMessage as MIMEEmailMessage
from email.utils import make_msgid

import aiosmtplib
from aiosmtplib.errors import (
    SMTPRecipientsRefused,
    SMTPResponseException,
    SMTPSenderRefused,
)

from cloudrift.core.exceptions import (
    EmailError,
    EmailSendError,
    EmailThrottledError,
    RecipientRejectedError,
    SenderUnverifiedError,
)
from cloudrift.email.base import Attachment, EmailBackend, _as_list


class SMTPEmailBackend(EmailBackend):
    """Raw SMTP backend.

    Use one of the class methods to construct:
    - ``from_plaintext`` — no TLS (port 25, dev only — MailHog / Mailpit)
    - ``from_starttls``  — STARTTLS upgrade (port 587, most providers)
    - ``from_tls``       — implicit TLS (port 465)
    """

    _MODE_PLAINTEXT = "plaintext"
    _MODE_STARTTLS = "starttls"
    _MODE_TLS = "tls"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        mode: str,
        username: str | None = None,
        password: str | None = None,
        default_from: str | None = None,
        ssl_context=None,
        timeout: float = 30.0,
    ) -> None:
        self._host = host
        self._port = port
        self._mode = mode
        self._username = username
        self._password = password
        self._default_from = default_from
        self._ssl_context = ssl_context
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Factory constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_plaintext(
        cls,
        host: str,
        port: int = 25,
        username: str | None = None,
        password: str | None = None,
        default_from: str | None = None,
        timeout: float = 30.0,
    ) -> "SMTPEmailBackend":
        """Connect without TLS. Dev / local-relay only — never use on the public internet."""
        return cls(
            host=host,
            port=port,
            mode=cls._MODE_PLAINTEXT,
            username=username,
            password=password,
            default_from=default_from,
            timeout=timeout,
        )

    @classmethod
    def from_starttls(
        cls,
        host: str,
        username: str,
        password: str,
        default_from: str | None = None,
        port: int = 587,
        ssl_context=None,
        timeout: float = 30.0,
    ) -> "SMTPEmailBackend":
        """Connect, then upgrade to TLS via STARTTLS (port 587). Default for most providers."""
        return cls(
            host=host,
            port=port,
            mode=cls._MODE_STARTTLS,
            username=username,
            password=password,
            default_from=default_from,
            ssl_context=ssl_context,
            timeout=timeout,
        )

    @classmethod
    def from_tls(
        cls,
        host: str,
        username: str,
        password: str,
        default_from: str | None = None,
        port: int = 465,
        ssl_context=None,
        timeout: float = 30.0,
    ) -> "SMTPEmailBackend":
        """Connect with implicit TLS (port 465)."""
        return cls(
            host=host,
            port=port,
            mode=cls._MODE_TLS,
            username=username,
            password=password,
            default_from=default_from,
            ssl_context=ssl_context,
            timeout=timeout,
        )

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

        msg = _build_mime(
            sender=sender,
            to=to_list,
            cc=cc_list,
            reply_to=_as_list(reply_to),
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachments=attachments or [],
            headers=headers or {},
        )
        message_id = make_msgid()
        msg["Message-ID"] = message_id

        kwargs = self._connect_kwargs()
        try:
            await aiosmtplib.send(
                msg,
                recipients=to_list + cc_list + bcc_list,
                sender=sender,
                **kwargs,
            )
        except SMTPRecipientsRefused as e:
            raise RecipientRejectedError(str(e)) from e
        except SMTPSenderRefused as e:
            raise SenderUnverifiedError(str(e)) from e
        except SMTPResponseException as e:
            if e.code in (421, 450, 451, 452):
                raise EmailThrottledError(str(e)) from e
            raise EmailSendError(str(e)) from e
        except (OSError, aiosmtplib.SMTPException) as e:
            raise EmailSendError(str(e)) from e

        return message_id

    async def health_check(self) -> bool:
        kwargs = self._connect_kwargs()
        client = aiosmtplib.SMTP(
            hostname=kwargs["hostname"],
            port=kwargs["port"],
            use_tls=kwargs.get("use_tls", False),
            start_tls=kwargs.get("start_tls", False),
            tls_context=kwargs.get("tls_context"),
            timeout=kwargs.get("timeout"),
        )
        try:
            await client.connect()
            await client.noop()
            await client.quit()
            return True
        except Exception:
            return False

    def _connect_kwargs(self) -> dict:
        kwargs: dict = {
            "hostname": self._host,
            "port": self._port,
            "timeout": self._timeout,
        }
        if self._username and self._password:
            kwargs["username"] = self._username
            kwargs["password"] = self._password
        if self._mode == self._MODE_TLS:
            kwargs["use_tls"] = True
            kwargs["start_tls"] = False
            if self._ssl_context is not None:
                kwargs["tls_context"] = self._ssl_context
        elif self._mode == self._MODE_STARTTLS:
            kwargs["use_tls"] = False
            kwargs["start_tls"] = True
            if self._ssl_context is not None:
                kwargs["tls_context"] = self._ssl_context
        else:
            kwargs["use_tls"] = False
            kwargs["start_tls"] = False
        return kwargs


def _build_mime(
    *,
    sender: str,
    to: list[str],
    cc: list[str],
    reply_to: list[str],
    subject: str,
    body_text: str | None,
    body_html: str | None,
    attachments: list[Attachment],
    headers: dict[str, str],
) -> MIMEEmailMessage:
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
    return msg
