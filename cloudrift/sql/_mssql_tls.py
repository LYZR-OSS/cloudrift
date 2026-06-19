"""Low-level TLS certificate-pinning helpers for SQL Server.

SQL Server wraps its TLS handshake inside the TDS protocol, so a plain
``ssl.connect()`` cannot reach the server certificate. To support pinning a
self-signed certificate, we perform a minimal TDS PRELOGIN exchange and drive a
``ssl.MemoryBIO`` handshake by hand to extract the peer certificate, then compare
its SHA-256 fingerprint against a caller-supplied PEM.

This is connection-security logic (independent of any query layer), which is why
it lives in cloudrift rather than in a consuming application.
"""
import asyncio
import socket
import ssl
import struct

from cloudrift.core.exceptions import SQLConnectionError


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise RuntimeError("Connection closed unexpectedly")
        buf += chunk
    return buf


def _send_tds(sock: socket.socket, payload: bytes) -> None:
    """Send payload as one or more TDS packets (type 0x12 = PRELOGIN)."""
    max_chunk = 4088
    for i in range(0, max(1, len(payload)), max_chunk):
        chunk = payload[i : i + max_chunk]
        is_last = (i + max_chunk) >= len(payload)
        status = 0x01 if is_last else 0x00
        header = struct.pack(">BBHHBB", 0x12, status, 8 + len(chunk), 0, 1, 0)
        sock.sendall(header + chunk)


def _recv_tds(sock: socket.socket) -> bytes:
    """Read one or more TDS packets and return the combined payload."""
    payload = b""
    while True:
        hdr = _recv_exact(sock, 8)
        pkt_len = struct.unpack(">H", hdr[2:4])[0]
        data = _recv_exact(sock, pkt_len - 8)
        payload += data
        if hdr[1] & 0x01:  # EOM bit
            break
    return payload


def _fetch_server_cert_der_sync(host: str, port: int) -> bytes:
    """Fetch the DER-encoded TLS certificate SQL Server presents via TDS PRELOGIN."""
    try:
        sock = socket.create_connection((host, port), timeout=10)
    except Exception as e:
        raise SQLConnectionError(
            f"Cannot reach {host}:{port} to fetch server certificate: {e}"
        ) from e

    try:
        # Minimal TDS PRELOGIN packet:
        #   Token 0x00 (VERSION):    offset=11, length=6
        #   Token 0x01 (ENCRYPTION): offset=17, length=1
        #   Token 0xFF (TERMINATOR)
        #   VERSION data: 9.0.0.0 (SQL Server 2005+); ENCRYPTION data: 0x01 (ENCRYPT_ON)
        prelogin = (
            b"\x00" + struct.pack(">HH", 11, 6)
            + b"\x01" + struct.pack(">HH", 17, 1)
            + b"\xff"
            + b"\x09\x00\x00\x00\x00\x00"
            + b"\x01"
        )
        _send_tds(sock, prelogin)
        _recv_tds(sock)  # consume server's PRELOGIN response

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # fingerprint comparison done by caller

        in_bio = ssl.MemoryBIO()
        out_bio = ssl.MemoryBIO()
        tls = ctx.wrap_bio(in_bio, out_bio, server_hostname=host)

        while True:
            try:
                tls.do_handshake()
                break
            except ssl.SSLWantReadError:
                pass
            except ssl.SSLError as e:
                raise SQLConnectionError(
                    f"TLS handshake failed while fetching server certificate: {e}"
                ) from e

            pending = out_bio.read()
            if pending:
                _send_tds(sock, pending)
            tds_data = _recv_tds(sock)
            if not tds_data:
                raise SQLConnectionError(
                    "Server closed the connection during TLS handshake."
                )
            in_bio.write(tds_data)

        pending = out_bio.read()
        if pending:
            _send_tds(sock, pending)

        der = tls.getpeercert(binary_form=True)
    except SQLConnectionError:
        raise
    except Exception as e:
        raise SQLConnectionError(f"Failed to retrieve server certificate: {e}") from e
    finally:
        sock.close()

    if der is None:
        raise SQLConnectionError("Server did not present a TLS certificate.")
    return der


async def validate_pinned_certificate(host: str, port: int, pinned_pem: str) -> None:
    """Fetch the live server cert and verify its SHA-256 fingerprint matches *pinned_pem*.

    Raises :class:`SQLConnectionError` on mismatch or if the cert cannot be
    fetched/parsed — the caller never reaches the driver connect step.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
    except ImportError as e:  # pragma: no cover - import guard
        raise SQLConnectionError(
            "Certificate pinning requires the 'cryptography' package. "
            "Install cloudrift[sql-mssql]."
        ) from e

    der = await asyncio.to_thread(_fetch_server_cert_der_sync, host, port)

    try:
        live_cert = x509.load_der_x509_certificate(der, default_backend())
        pinned_cert = x509.load_pem_x509_certificate(pinned_pem.encode(), default_backend())
    except Exception as e:
        raise SQLConnectionError(f"Failed to parse a server certificate: {e}") from e

    live_fp = live_cert.fingerprint(hashes.SHA256()).hex()
    pinned_fp = pinned_cert.fingerprint(hashes.SHA256()).hex()
    if live_fp != pinned_fp:
        raise SQLConnectionError(
            "Server certificate does not match the provided pinned certificate."
        )
