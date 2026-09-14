#!/usr/bin/env python3
"""Stand-in for the embeddings API, for exercising the delivery path without a provider account.

Answers the same contract as /v1/embeddings with deterministic vectors, and counts requests and
inputs. That count is what makes batching observable: for N events the queue should produce
ceil(N / BATCH_SIZE) requests, not N.

Point a provider at it by setting its API Endpoint field:

    UPDATE etarc_vector_embed_provider SET api_endpoint = 'http://localhost:8099/v1/embeddings';

Note that a provider row shipped in a module dataset is restored by update.database, so the
endpoint has to be set again after one.

    python3 embeddings_stub.py 8099
    curl -s localhost:8099/stats
    curl -s -X POST localhost:8099/reset
"""
import hashlib, json, math, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

STATS = {"requests": 0, "inputs": 0, "per_request": []}


def vector(text, dims):
    """Deterministic and normalised: the same text always yields the same vector."""
    out, seed = [], hashlib.sha256(text.encode("utf-8")).digest()
    while len(out) < dims:
        seed = hashlib.sha256(seed).digest()
        out.extend((b - 127.5) / 127.5 for b in seed)
    out = out[:dims]
    norm = math.sqrt(sum(v * v for v in out)) or 1.0
    return [v / norm for v in out]


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._send(200, STATS) if self.path == "/stats" else self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/reset":
            STATS.update(requests=0, inputs=0, per_request=[])
            return self._send(200, STATS)
        if not self.path.endswith("/embeddings"):
            return self._send(404, {"error": {"message": "unknown path"}})

        raw = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        try:
            request = json.loads(raw or b"{}")
        except ValueError:
            return self._send(400, {"error": {"message": "invalid json"}})

        # The provider sends Authorization: Bearer <key>. Any non empty value works here, but a
        # missing one answers 401 like the real service so the failure path stays faithful.
        if not (self.headers.get("Authorization") or "").startswith("Bearer "):
            return self._send(401, {"error": {"message": "missing bearer token"}})

        raw_input = request.get("input")
        inputs = raw_input if isinstance(raw_input, list) else [raw_input]
        inputs = [i for i in inputs if i is not None]
        if not inputs:
            return self._send(400, {"error": {"message": "input is required"}})

        dims = int(request.get("dimensions") or 1536)
        STATS["requests"] += 1
        STATS["inputs"] += len(inputs)
        STATS["per_request"].append(len(inputs))
        print(f"  request #{STATS['requests']:>4}  inputs={len(inputs):>4}  "
              f"model={request.get('model')}  dims={dims}  (total inputs={STATS['inputs']})", flush=True)

        self._send(200, {
            "object": "list",
            "model": request.get("model", "stub"),
            "data": [{"object": "embedding", "index": i, "embedding": vector(str(t), dims)}
                     for i, t in enumerate(inputs)],
            "usage": {"prompt_tokens": sum(len(str(t).split()) for t in inputs),
                      "total_tokens": sum(len(str(t).split()) for t in inputs)},
        })

    def log_message(self, *args):
        pass  # the useful line is printed above


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8099
    print(f"embeddings stub listening on http://localhost:{port}/v1/embeddings", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
