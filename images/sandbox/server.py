"""Standard-library-only exec server for the cloudrift sandbox image.

Answers a two-endpoint contract that both new sandbox backends (AWS Lambda
MicroVMs and Azure Container Apps dynamic sessions) forward HTTPS into:

- Port 8080 (application): ``GET /health``, ``POST /exec``.
- Port 9000 (Lambda MicroVM lifecycle hooks; Azure never calls this port):
  ``POST /aws/lambda-microvms/runtime/v1/{ready,validate,run,resume,suspend,terminate}``.

No third-party dependencies: this image ships nothing that could drift or
need patching independent of the base OS packages baked into the Dockerfile.
"""

import http.client
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import subprocess

MAX_OUTPUT_BYTES = 1024 * 1024  # 1 MiB
WORKDIR = "/workspace"
_HOOKS = frozenset({"ready", "validate", "run", "resume", "suspend", "terminate"})

# Set once the 8080 application listener is bound, so /ready never reports
# healthy before a real exec can be served.
_ready = threading.Event()


def _run_exec(command: str, timeout_seconds: float) -> dict:
    try:
        proc = subprocess.run(
            ["bash", "-lc", command],
            capture_output=True,
            timeout=timeout_seconds,
            cwd=WORKDIR,
        )
        stdout, stderr, exit_code = proc.stdout, proc.stderr, proc.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or b""
        stderr = exc.stderr or b""
        exit_code = 124
    return {
        "stdout": stdout[-MAX_OUTPUT_BYTES:].decode("utf-8", "replace"),
        "stderr": stderr[-MAX_OUTPUT_BYTES:].decode("utf-8", "replace"),
        "exit_code": exit_code,
    }


class _AppHandler(BaseHTTPRequestHandler):
    server_version = "cloudrift-sandbox/1.0"

    def _write_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._write_json(200, {"status": "ok"})
            return
        self._write_json(404, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path != "/exec":
            self._write_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length) if length else b"{}"
            payload = json.loads(raw or b"{}")
            command = payload["command"]
            timeout_seconds = payload.get("timeout_seconds", 60)
            result = _run_exec(command, timeout_seconds)
            self._write_json(200, result)
        except Exception as exc:  # noqa: BLE001 - boundary handler must never crash the server
            self._write_json(500, {"error": str(exc)})

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        pass  # Platform captures process stdout/stderr; skip the default access log.


class _LifecycleHandler(BaseHTTPRequestHandler):
    server_version = "cloudrift-sandbox-lifecycle/1.0"

    _PREFIX = "/aws/lambda-microvms/runtime/v1/"

    def do_POST(self) -> None:
        if not self.path.startswith(self._PREFIX):
            self._empty(404)
            return
        hook = self.path[len(self._PREFIX) :]
        if hook not in _HOOKS:
            self._empty(404)
            return
        if hook == "ready":
            self._empty(200 if _ready.is_set() else 503)
            return
        if hook == "validate":
            self._empty(200 if self._probe_exec() else 503)
            return
        # run / resume / suspend / terminate are informational only.
        self._empty(200)

    def _probe_exec(self) -> bool:
        conn = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)
        try:
            body = json.dumps({"command": "true", "timeout_seconds": 10}).encode("utf-8")
            conn.request("POST", "/exec", body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = json.loads(resp.read())
            return resp.status == 200 and data.get("exit_code") == 0
        except Exception:
            return False
        finally:
            conn.close()

    def _empty(self, status: int) -> None:
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - stdlib signature
        pass


def main() -> None:
    # Both servers bind 0.0.0.0: Lambda calls hooks across the network
    # namespace, so a localhost-only listener would be unreachable.
    app_server = ThreadingHTTPServer(("0.0.0.0", 8080), _AppHandler)
    _ready.set()  # TCPServer.__init__ already bound + listened above.

    app_thread = threading.Thread(target=app_server.serve_forever, daemon=True)
    app_thread.start()

    lifecycle_server = ThreadingHTTPServer(("0.0.0.0", 9000), _LifecycleHandler)
    lifecycle_server.serve_forever()


if __name__ == "__main__":
    main()
