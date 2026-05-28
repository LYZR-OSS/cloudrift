from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class Attachment:
    """An email attachment.

    ``content`` is the raw payload bytes. ``content_type`` is used directly in
    the MIME / provider request — pick the right one (``application/pdf``,
    ``image/png``, etc.) so the recipient's mail client renders it correctly.
    """

    filename: str
    content: bytes
    content_type: str = "application/octet-stream"


@dataclass
class EmailMessage:
    """An outbound email used by :meth:`EmailBackend.send_batch`.

    ``from_`` falls back to the backend's ``default_from`` when ``None``.
    At least one of ``body_text`` / ``body_html`` must be set.
    """

    to: list[str]
    subject: str
    body_text: str | None = None
    body_html: str | None = None
    from_: str | None = None
    cc: list[str] = field(default_factory=list)
    bcc: list[str] = field(default_factory=list)
    reply_to: list[str] = field(default_factory=list)
    attachments: list[Attachment] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)


class EmailBackend(ABC):
    """Abstract base class for transactional email backends.

    Backends hold long-lived async clients. Use ``await backend.close()`` (or
    ``async with backend:``) to release sockets cleanly.

    Implementations must accept a ``default_from`` at construction time. The
    ``from_`` argument on :meth:`send` overrides it per call.
    """

    @abstractmethod
    async def send(
        self,
        to: str | list[str],
        subject: str,
        *,
        body_text: str | None = None,
        body_html: str | None = None,
        from_: str | None = None,
        cc: list[str] | None = None,
        bcc: list[str] | None = None,
        reply_to: list[str] | None = None,
        attachments: list[Attachment] | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        """Send a single email. Returns the provider message ID."""

    async def send_batch(self, messages: list[EmailMessage]) -> list[str]:
        """Send a batch of emails. Default implementation loops :meth:`send`.

        Subclasses override only when the provider has a true bulk API.
        """
        ids: list[str] = []
        for msg in messages:
            ids.append(
                await self.send(
                    msg.to,
                    msg.subject,
                    body_text=msg.body_text,
                    body_html=msg.body_html,
                    from_=msg.from_,
                    cc=msg.cc or None,
                    bcc=msg.bcc or None,
                    reply_to=msg.reply_to or None,
                    attachments=msg.attachments or None,
                    headers=msg.headers or None,
                )
            )
        return ids

    async def health_check(self) -> bool:
        """Return True if the email backend is reachable."""
        return True

    async def close(self) -> None:
        """Close the underlying client and release sockets. Default is a no-op."""

    async def __aenter__(self) -> "EmailBackend":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


def _as_list(value: str | list[str] | None) -> list[str]:
    """Normalize a recipient / address field to a list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)
