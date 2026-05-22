"""Unit tests for the email category.

- SES: real `aioboto3` client hitting an in-process moto SESv2 server.
- ACS: `azure.communication.email.EmailClient` patched with a fake.
- SMTP: `aiosmtplib.send` patched to capture the MIME tree.
"""
from __future__ import annotations

import base64
from email import message_from_bytes
from email.message import EmailMessage as MIMEEmailMessage
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto.server import ThreadedMotoServer

from cloudrift.core.exceptions import (
    EmailError,
    EmailSendError,
    EmailThrottledError,
    RecipientRejectedError,
    SenderUnverifiedError,
)
from cloudrift.email import Attachment, EmailMessage, get_email
from cloudrift.email.azure_acs import AzureACSEmailBackend
from cloudrift.email.smtp import SMTPEmailBackend

REGION = "us-east-1"
SENDER = "sender@example.com"


# ===========================================================================
# SES — moto-backed
# ===========================================================================

@pytest.fixture(scope="module")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server._server.server_address
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture
def verified_sender(moto_server):
    """Verify SENDER in the moto SES backend so send_email succeeds."""
    ses = boto3.client(
        "ses",
        region_name=REGION,
        endpoint_url=moto_server,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    ses.verify_email_identity(EmailAddress=SENDER)
    return SENDER


@pytest.fixture
async def ses_backend(moto_server, verified_sender):
    backend = get_email(
        "ses",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
        default_from=SENDER,
    )
    yield backend
    await backend.close()


async def test_ses_send_simple_text(ses_backend):
    msg_id = await ses_backend.send(
        "to@example.com",
        "hello",
        body_text="plain body",
    )
    assert isinstance(msg_id, str) and msg_id


async def test_ses_send_with_html_and_text(ses_backend):
    msg_id = await ses_backend.send(
        ["to@example.com", "to2@example.com"],
        "hello",
        body_text="plain",
        body_html="<p>html</p>",
    )
    assert isinstance(msg_id, str) and msg_id


async def test_ses_send_with_attachment_uses_raw(ses_backend):
    msg_id = await ses_backend.send(
        "to@example.com",
        "with attachment",
        body_text="see attached",
        attachments=[Attachment(filename="hello.txt", content=b"hi", content_type="text/plain")],
    )
    assert isinstance(msg_id, str) and msg_id


async def test_ses_send_unverified_sender_maps_exception(moto_server):
    backend = get_email(
        "ses",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
        default_from="unverified@nowhere.example",
    )
    try:
        # moto raises MessageRejected with "Email address is not verified" when
        # the From identity isn't verified.
        with pytest.raises((RecipientRejectedError, SenderUnverifiedError, EmailSendError)):
            await backend.send(
                "to@example.com",
                "subject",
                body_text="hi",
            )
    finally:
        await backend.close()


async def test_ses_send_requires_body(ses_backend):
    with pytest.raises(EmailError):
        await ses_backend.send("to@example.com", "subj")


async def test_ses_send_requires_sender(moto_server):
    backend = get_email(
        "ses",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region=REGION,
        endpoint_url=moto_server,
        # no default_from
    )
    try:
        with pytest.raises(EmailError):
            await backend.send("to@example.com", "subj", body_text="hi")
    finally:
        await backend.close()


async def test_ses_send_batch_loops(ses_backend):
    ids = await ses_backend.send_batch(
        [
            EmailMessage(to=["to1@example.com"], subject="s1", body_text="b1"),
            EmailMessage(to=["to2@example.com"], subject="s2", body_html="<b>b2</b>"),
            EmailMessage(to=["to3@example.com"], subject="s3", body_text="b3"),
        ]
    )
    assert len(ids) == 3
    assert all(isinstance(i, str) and i for i in ids)


async def test_ses_health_check(ses_backend):
    assert await ses_backend.health_check() is True


# ===========================================================================
# ACS — mocked SDK
# ===========================================================================

class _FakePoller:
    def __init__(self, result):
        self._result = result
        self.result_called = False

    def result(self):
        self.result_called = True
        return self._result


class _FakeACSClient:
    def __init__(self):
        self.last_message = None
        self.closed = False
        self._next_result = {"id": "acs-msg-1"}

    def begin_send(self, message):
        self.last_message = message
        return _FakePoller(self._next_result)

    def close(self):
        self.closed = True


@pytest.fixture
def fake_acs_client():
    return _FakeACSClient()


@pytest.fixture
async def acs_backend(fake_acs_client):
    backend = AzureACSEmailBackend(
        endpoint=None,
        default_from="DoNotReply@example.com",
        connection_string="endpoint=https://x.communication.azure.com;accesskey=fake==",
    )
    # Force the client without going through ``_ensure``'s SDK import.
    backend._client = fake_acs_client
    yield backend
    await backend.close()


async def test_acs_send_basic(acs_backend, fake_acs_client):
    msg_id = await acs_backend.send(
        "to@example.com",
        "subject",
        body_text="hi",
        body_html="<p>hi</p>",
    )
    assert msg_id == "acs-msg-1"
    msg = fake_acs_client.last_message
    assert msg["senderAddress"] == "DoNotReply@example.com"
    assert msg["recipients"]["to"] == [{"address": "to@example.com"}]
    assert msg["content"]["subject"] == "subject"
    assert msg["content"]["plainText"] == "hi"
    assert msg["content"]["html"] == "<p>hi</p>"


async def test_acs_send_with_cc_bcc_reply_to(acs_backend, fake_acs_client):
    await acs_backend.send(
        ["a@example.com", "b@example.com"],
        "subject",
        body_text="hi",
        cc=["c@example.com"],
        bcc=["d@example.com"],
        reply_to=["r@example.com"],
    )
    msg = fake_acs_client.last_message
    assert msg["recipients"]["to"] == [
        {"address": "a@example.com"},
        {"address": "b@example.com"},
    ]
    assert msg["recipients"]["cc"] == [{"address": "c@example.com"}]
    assert msg["recipients"]["bcc"] == [{"address": "d@example.com"}]
    assert msg["replyTo"] == [{"address": "r@example.com"}]


async def test_acs_send_with_attachment_base64(acs_backend, fake_acs_client):
    await acs_backend.send(
        "to@example.com",
        "subject",
        body_text="hi",
        attachments=[
            Attachment(filename="doc.pdf", content=b"PDFDATA", content_type="application/pdf")
        ],
    )
    atts = fake_acs_client.last_message["attachments"]
    assert atts[0]["name"] == "doc.pdf"
    assert atts[0]["contentType"] == "application/pdf"
    assert base64.b64decode(atts[0]["contentInBase64"]) == b"PDFDATA"


async def test_acs_send_throttled_maps_exception(acs_backend, fake_acs_client):
    from azure.core.exceptions import HttpResponseError

    err = HttpResponseError(message="Too Many Requests")
    err.status_code = 429

    def _raise_429(_message):
        raise err

    fake_acs_client.begin_send = _raise_429  # type: ignore[assignment]
    with pytest.raises(EmailThrottledError):
        await acs_backend.send("to@example.com", "subj", body_text="hi")


async def test_acs_send_invalid_recipient_maps_exception(acs_backend, fake_acs_client):
    from azure.core.exceptions import HttpResponseError

    err = HttpResponseError(message="InvalidRecipient: bad email")
    err.status_code = 400

    def _raise_400(_message):
        raise err

    fake_acs_client.begin_send = _raise_400  # type: ignore[assignment]
    with pytest.raises(RecipientRejectedError):
        await acs_backend.send("bogus", "subj", body_text="hi")


async def test_acs_send_domain_not_linked_maps_exception(acs_backend, fake_acs_client):
    from azure.core.exceptions import HttpResponseError

    err = HttpResponseError(message="DomainNotLinked: sender domain not linked")
    err.status_code = 403

    def _raise_403(_message):
        raise err

    fake_acs_client.begin_send = _raise_403  # type: ignore[assignment]
    with pytest.raises(SenderUnverifiedError):
        await acs_backend.send("to@example.com", "subj", body_text="hi")


async def test_acs_close_idempotent(acs_backend, fake_acs_client):
    await acs_backend.close()
    assert fake_acs_client.closed is True
    # Second close must not raise.
    await acs_backend.close()


# ===========================================================================
# SMTP — mocked aiosmtplib
# ===========================================================================

@pytest.fixture
def captured_send():
    captured: dict = {}

    async def fake_send(msg, **kwargs):
        captured["msg"] = msg
        captured["kwargs"] = kwargs
        return ({}, "250 OK")

    with patch("cloudrift.email.smtp.aiosmtplib.send", side_effect=fake_send):
        yield captured


@pytest.fixture
def smtp_backend():
    return SMTPEmailBackend.from_starttls(
        host="smtp.example.com",
        username="user",
        password="pw",
        default_from=SENDER,
    )


def _extract_mime(msg) -> MIMEEmailMessage:
    """The captured object is already an EmailMessage."""
    if isinstance(msg, MIMEEmailMessage):
        return msg
    # Defensive: aiosmtplib.send accepts str/bytes too.
    return message_from_bytes(bytes(msg))  # type: ignore[arg-type]


async def test_smtp_send_text_only(smtp_backend, captured_send):
    msg_id = await smtp_backend.send(
        "to@example.com",
        "hello",
        body_text="plain body",
    )
    assert msg_id.startswith("<") and msg_id.endswith(">")
    mime = _extract_mime(captured_send["msg"])
    assert mime["From"] == SENDER
    assert mime["To"] == "to@example.com"
    assert mime["Subject"] == "hello"
    assert mime.get_content_type() == "text/plain"
    assert "plain body" in mime.get_content()


async def test_smtp_send_html_and_text_multipart_alternative(smtp_backend, captured_send):
    await smtp_backend.send(
        "to@example.com",
        "hello",
        body_text="plain",
        body_html="<b>html</b>",
    )
    mime = _extract_mime(captured_send["msg"])
    assert mime.get_content_type() == "multipart/alternative"
    parts = list(mime.iter_parts())
    types = sorted(p.get_content_type() for p in parts)
    assert types == ["text/html", "text/plain"]


async def test_smtp_send_html_only(smtp_backend, captured_send):
    await smtp_backend.send(
        "to@example.com",
        "hello",
        body_html="<p>only html</p>",
    )
    mime = _extract_mime(captured_send["msg"])
    assert mime.get_content_type() == "text/html"


async def test_smtp_send_with_attachment(smtp_backend, captured_send):
    await smtp_backend.send(
        "to@example.com",
        "hello",
        body_text="body",
        attachments=[
            Attachment(filename="doc.pdf", content=b"PDFDATA", content_type="application/pdf")
        ],
    )
    mime = _extract_mime(captured_send["msg"])
    # add_attachment promotes a text-only message to multipart/mixed
    assert mime.get_content_type() == "multipart/mixed"
    att_parts = [
        p for p in mime.iter_parts() if p.get_filename() == "doc.pdf"
    ]
    assert len(att_parts) == 1
    assert att_parts[0].get_content_type() == "application/pdf"
    assert att_parts[0].get_payload(decode=True) == b"PDFDATA"


async def test_smtp_send_cc_bcc_recipients_passed_to_aiosmtplib(smtp_backend, captured_send):
    await smtp_backend.send(
        "to@example.com",
        "hello",
        body_text="body",
        cc=["cc@example.com"],
        bcc=["bcc@example.com"],
    )
    recipients = captured_send["kwargs"]["recipients"]
    assert "to@example.com" in recipients
    assert "cc@example.com" in recipients
    assert "bcc@example.com" in recipients
    # The MIME headers should expose To and Cc but NOT Bcc.
    mime = _extract_mime(captured_send["msg"])
    assert mime["To"] == "to@example.com"
    assert mime["Cc"] == "cc@example.com"
    assert mime["Bcc"] is None


async def test_smtp_send_starttls_kwargs(smtp_backend, captured_send):
    await smtp_backend.send("to@example.com", "hello", body_text="body")
    k = captured_send["kwargs"]
    assert k["hostname"] == "smtp.example.com"
    assert k["port"] == 587
    assert k["start_tls"] is True
    assert k["use_tls"] is False
    assert k["username"] == "user"
    assert k["password"] == "pw"


async def test_smtp_send_tls_mode_kwargs(captured_send):
    backend = get_email(
        "smtp",
        mode="tls",
        host="smtp.example.com",
        port=465,
        username="user",
        password="pw",
        default_from=SENDER,
    )
    await backend.send("to@example.com", "hello", body_text="body")
    k = captured_send["kwargs"]
    assert k["port"] == 465
    assert k["use_tls"] is True
    assert k["start_tls"] is False


async def test_smtp_send_plaintext_mode_kwargs(captured_send):
    backend = get_email(
        "smtp",
        mode="plaintext",
        host="mailhog.local",
        port=1025,
        default_from=SENDER,
    )
    await backend.send("to@example.com", "hello", body_text="body")
    k = captured_send["kwargs"]
    assert k["port"] == 1025
    assert k["use_tls"] is False
    assert k["start_tls"] is False


async def test_smtp_send_recipients_refused_maps_exception(smtp_backend):
    from aiosmtplib.errors import SMTPRecipientsRefused

    async def _raise(*_a, **_k):
        raise SMTPRecipientsRefused([])

    with patch("cloudrift.email.smtp.aiosmtplib.send", side_effect=_raise):
        with pytest.raises(RecipientRejectedError):
            await smtp_backend.send("to@example.com", "subj", body_text="hi")


async def test_smtp_send_sender_refused_maps_exception(smtp_backend):
    from aiosmtplib.errors import SMTPSenderRefused

    async def _raise(*_a, **_k):
        raise SMTPSenderRefused(550, "sender rejected", SENDER)

    with patch("cloudrift.email.smtp.aiosmtplib.send", side_effect=_raise):
        with pytest.raises(SenderUnverifiedError):
            await smtp_backend.send("to@example.com", "subj", body_text="hi")


async def test_smtp_send_throttled_maps_exception(smtp_backend):
    from aiosmtplib.errors import SMTPResponseException

    async def _raise(*_a, **_k):
        raise SMTPResponseException(421, "service not available")

    with patch("cloudrift.email.smtp.aiosmtplib.send", side_effect=_raise):
        with pytest.raises(EmailThrottledError):
            await smtp_backend.send("to@example.com", "subj", body_text="hi")


async def test_smtp_send_requires_body(smtp_backend):
    with pytest.raises(EmailError):
        await smtp_backend.send("to@example.com", "subj")


async def test_smtp_send_requires_sender():
    backend = SMTPEmailBackend.from_plaintext(host="mailhog.local", port=1025)
    with pytest.raises(EmailError):
        await backend.send("to@example.com", "subj", body_text="hi")


# ===========================================================================
# Factory
# ===========================================================================

def test_factory_unknown_provider():
    with pytest.raises(ValueError, match="Unknown email provider"):
        get_email("gmail", username="me", password="x")


def test_factory_unknown_smtp_mode():
    with pytest.raises(ValueError, match="Unknown SMTP mode"):
        get_email("smtp", mode="weird", host="smtp.example.com")


def test_factory_smtp_routing_starttls():
    backend = get_email(
        "smtp",
        host="smtp.example.com",
        username="u",
        password="p",
        default_from=SENDER,
    )
    assert isinstance(backend, SMTPEmailBackend)
    assert backend._mode == "starttls"
    assert backend._port == 587


def test_factory_smtp_routing_tls():
    backend = get_email(
        "smtp",
        mode="tls",
        host="smtp.example.com",
        username="u",
        password="p",
        default_from=SENDER,
    )
    assert backend._mode == "tls"


def test_factory_ses_routing_access_key():
    from cloudrift.email.ses import AWSSESBackend

    backend = get_email(
        "ses",
        aws_access_key_id="AKIA-test",
        aws_secret_access_key="secret",
        region=REGION,
        default_from=SENDER,
    )
    assert isinstance(backend, AWSSESBackend)


def test_factory_acs_routing_connection_string():
    backend = get_email(
        "azure_acs",
        connection_string="endpoint=https://x.communication.azure.com;accesskey=k",
        default_from="dnr@example.com",
    )
    assert isinstance(backend, AzureACSEmailBackend)
    assert backend._connection_string is not None


def test_factory_acs_routing_service_principal():
    # Just constructs the credential; no SDK calls.
    fake_cred = MagicMock(name="creds")
    with patch(
        "azure.identity.ClientSecretCredential", return_value=fake_cred
    ):
        backend = get_email(
            "azure_acs",
            endpoint="https://x.communication.azure.com",
            tenant_id="t",
            client_id="c",
            client_secret="s",
            default_from="dnr@example.com",
        )
        assert isinstance(backend, AzureACSEmailBackend)
        assert backend._credential is fake_cred
