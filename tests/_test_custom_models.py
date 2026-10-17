"""Custom model upstreams (models.json): routing, streaming, safe failure.

Runs a real gateway process-in-thread against a mock OpenAI-compatible
upstream, with %USERPROFILE% re-pointed at a temp dir so the gateway reads a
test-written ~/.workbuddy/models.json. Covers: /v1/models merge (no secrets),
chat non-stream + stream passthrough, Responses translation (both modes),
upstream 401/500/redirect/timeout mapping, empty-stream no-fake-success,
bad/missing config safe failure, hot reload, key redaction and built-in
model list regression. No network access beyond loopback.
"""
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

import _lifecycle as life  # noqa: E402  (shared spare-port pick)

WORK = tempfile.mkdtemp(prefix="custom-models-")
ACCOUNTS = os.path.join(WORK, "accounts")
os.makedirs(ACCOUNTS, exist_ok=True)
os.environ["ACCOUNTS_DIR"] = ACCOUNTS
os.environ["WB_PROXY_USAGE_DIR"] = os.path.join(WORK, "usage")
os.makedirs(os.environ["WB_PROXY_USAGE_DIR"], exist_ok=True)
# The gateway derives ~/.workbuddy/models.json from the home dir.
os.environ["HOME"] = WORK
os.environ["USERPROFILE"] = WORK
os.environ["WB_PROXY_KEY"] = "TESTKEY"

API_KEY = "sk-custom-test-abcdef123456"
USAGE_LOG = os.path.join(os.environ["WB_PROXY_USAGE_DIR"], "usage.jsonl")

UPSTREAM_PORT = life.free_port()
UPSTREAM_BASE = "http://127.0.0.1:%d/v1" % UPSTREAM_PORT

MOCK = {"mode": "stream", "requests": []}


def config_path():
    return os.path.join(WORK, ".workbuddy", "models.json")


def write_config(entries, raw=None):
    os.makedirs(os.path.dirname(config_path()), exist_ok=True)
    with open(config_path(), "w", encoding="utf-8") as fh:
        fh.write(raw if raw is not None else json.dumps(entries))


def entry(mid):
    return {
        "id": mid,
        "name": mid,
        "vendor": "Custom",
        "url": UPSTREAM_BASE,
        "apiKey": API_KEY,
        "supportsToolCall": True,
        "supportsImages": True,
        "supportsReasoning": True,
        "useCustomProtocol": False,
        "maxInputTokens": 128000,
        "maxOutputTokens": 32768,
        "reasoning": {"defaultEffort": "low",
                      "supportedEfforts": ["low", "high"]},
    }


write_config([entry("gpt-6.1-sol"), entry("gpt-6-astra")])


def sse(chunks):
    out = [("data: " + json.dumps(c) + "\n\n").encode("utf-8") for c in chunks]
    out.append(b"data: [DONE]\n\n")
    return b"".join(out)


TEXT_CHUNKS = [
    {"id": "cc1", "model": "gpt-6.1-sol",
     "choices": [{"index": 0, "delta": {"role": "assistant", "content": "Hello "}}]},
    {"id": "cc1", "model": "gpt-6.1-sol",
     "choices": [{"index": 0, "delta": {"content": "world"}}]},
    {"id": "cc1", "model": "gpt-6.1-sol",
     "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
     "usage": {"prompt_tokens": 11, "completion_tokens": 2, "total_tokens": 13}},
]
JSON_COMPLETION = {
    "id": "cc-json", "object": "chat.completion", "model": "gpt-6.1-sol",
    "choices": [{"index": 0, "message": {"role": "assistant", "content": "Hi there"},
                 "finish_reason": "stop"}],
    "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
}

TOOL_CHUNKS = [
    {"id": "cc3", "model": "gpt-6.1-sol",
     "choices": [{"index": 0, "delta": {"tool_calls": [
         {"index": 0, "id": "call_w1", "type": "function",
          "function": {"name": "get_weather",
                       "arguments": "{\"city\":"}}]}}]},
    {"id": "cc3", "model": "gpt-6.1-sol",
     "choices": [{"index": 0, "delta": {"tool_calls": [
         {"index": 0, "function": {"arguments": " \"Paris\"}"}}]},
         "finish_reason": "tool_calls"}],
     "usage": {"prompt_tokens": 9, "completion_tokens": 5, "total_tokens": 14}},
]


class MockUpstream(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode("utf-8"))
        except Exception:
            body = {"_raw": raw.decode("utf-8", "replace")}
        MOCK["requests"].append({
            "path": self.path,
            "auth": self.headers.get("Authorization"),
            "body": body,
        })
        mode = MOCK["mode"]
        wants_stream = bool(body.get("stream")) if isinstance(body, dict) else False
        if mode == "ok":
            # Behave like a real OpenAI-compatible upstream: stream request
            # -> SSE, non-stream request -> JSON.
            if wants_stream:
                self._json(200, sse(TEXT_CHUNKS), "text/event-stream")
            else:
                self._json(200, json.dumps(JSON_COMPLETION).encode("utf-8"))
        elif mode == "badbody":
            # 200 but the body is neither SSE nor a chat completion.
            self._json(200, b'{"not": "a completion"}')
        elif mode == "unauthorized":
            # The upstream echoes the caller's key back in the error body -
            # the gateway must scrub it before the client sees it.
            payload = json.dumps(
                {"error": {"message": "bad key %s" % API_KEY}}).encode("utf-8")
            self._json(401, payload)
        elif mode == "error500":
            payload = b'{"error": {"message": "boom"}}'
            self._json(500, payload)
        elif mode == "redirect":
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:9/evil")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif mode == "tools":
            self._json(200, sse(TOOL_CHUNKS), "text/event-stream")
        elif mode == "empty":
            self._json(200, b"", "text/event-stream")
        elif mode == "slow":
            time.sleep(5)
            self._json(200, sse(TEXT_CHUNKS), "text/event-stream")
        else:
            self._json(500, b'{"error": {"message": "unknown mode"}}')

    def _json(self, code, payload, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


upstream = ThreadingHTTPServer(("127.0.0.1", UPSTREAM_PORT), MockUpstream)
threading.Thread(target=upstream.serve_forever, daemon=True).start()

import wb_identity  # noqa: E402
import wb_proxy as P  # noqa: E402

# Deterministic built-in catalogue: no upstream/catalog network in this test.
P.fetch_models = lambda realm=None: [
    ("glm-5.3-flash", {"name": "glm-5.3-flash", "supportsToolCall": True})]

GATEWAY_PORT = life.free_port()


class Args(object):
    host = "127.0.0.1"
    port = GATEWAY_PORT
    api_key = "TESTKEY"
    system_prompt = "You are a helpful assistant."
    user_agent = None
    usage_dir = os.environ["WB_PROXY_USAGE_DIR"]
    accounts_dir = ACCOUNTS
    import_desktop = False
    lan = False
    panel_password = None
    info = None


P._apply_cli_overrides(Args())
P._bootstrap_runtime(Args())
gateway = ThreadingHTTPServer(("127.0.0.1", GATEWAY_PORT), P.Handler)
threading.Thread(target=gateway.serve_forever, daemon=True).start()

AUTH = {"Authorization": "Bearer TESTKEY"}
PASS = FAIL = 0


def check(label, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s" % label)
    else:
        FAIL += 1
        print("  [FAIL] %s  %s" % (label, detail))


def request(method, path, body=None, headers=None, timeout=30):
    url = "http://127.0.0.1:%d%s" % (GATEWAY_PORT, path)
    data = None if body is None else (
        body if isinstance(body, bytes) else json.dumps(body).encode("utf-8"))
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def wait_ready():
    for _ in range(60):
        try:
            code, _h, _b = request("GET", "/health", timeout=2)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def models_ids():
    code, _h, body = request("GET", "/v1/models", headers=AUTH)
    payload = json.loads(body or b"{}")
    return code, payload, [m.get("id") for m in payload.get("data") or []]


try:
    if not wait_ready():
        print("  [FAIL] gateway did not start")
        sys.exit(1)

    print("[1] /v1/models merges custom ids without leaking secrets")
    code, payload, ids = models_ids()
    check("models list 200", code == 200, code)
    check("both custom ids present",
          "gpt-6.1-sol" in ids and "gpt-6-astra" in ids, ids)
    check("built-in model still listed", "glm-5.3-flash" in ids, ids)
    custom = [m for m in payload["data"] if m.get("owned_by") == "custom"]
    check("custom entries flagged owned_by=custom", len(custom) == 2, custom)
    blob = json.dumps(payload)
    check("no apiKey in /v1/models", API_KEY not in blob)
    check("no upstream url in /v1/models", UPSTREAM_BASE not in blob)

    print("[2] chat completions, non-streaming (aggregated from upstream stream)")
    MOCK["mode"] = "ok"
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    out = json.loads(body or b"{}")
    content = ((out.get("choices") or [{}])[0].get("message") or {}).get("content")
    check("chat nonstream 200 with aggregated content",
          code == 200 and content == "Hello world" and out.get("usage"),
          (code, str(out)[:300]))
    req = MOCK["requests"][-1]
    check("hits /chat/completions on custom upstream",
          req["path"].endswith("/chat/completions"), req["path"])
    check("bearer key sent to custom upstream",
          req["auth"] == "Bearer " + API_KEY, req["auth"])
    check("upstream always streamed (header-timeout safe)",
          req["body"].get("stream") is True, req["body"].get("stream"))
    check("model preserved", req["body"].get("model") == "gpt-6.1-sol")
    check("default reasoning effort applied",
          req["body"].get("reasoning_effort") == "low",
          req["body"].get("reasoning_effort"))
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False, "reasoning_effort": "high",
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("client effort overrides default",
          json.loads(body or b"{}").get("choices") and
          MOCK["requests"][-1]["body"].get("reasoning_effort") == "high",
          MOCK["requests"][-1]["body"].get("reasoning_effort"))

    print("[3] chat completions, SSE passthrough")
    code, headers, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": True,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    text = body.decode("utf-8", "replace")
    lines = [l for l in text.splitlines() if l.startswith("data: ")]
    check("chat stream 200 + event-stream",
          code == 200 and "text/event-stream" in (headers.get("Content-Type") or ""),
          (code, headers.get("Content-Type")))
    check("SSE passthrough: 3 chunks + [DONE]",
          len(lines) == 4 and lines[-1] == "data: [DONE]", lines)
    check("no key in stream body", API_KEY not in text)

    MOCK["mode"] = "ok"
    print("[3b] 200 with non-SSE body -> 502, no fake success")
    MOCK["mode"] = "badbody"
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("non-SSE 200 body -> 502", code == 502, (code, body[:120]))

    MOCK["mode"] = "ok"
    print("[4] responses API, non-streaming (translated to chat upstream)")
    code, _h, body = request("POST", "/v1/responses", {
        "model": "gpt-6-astra", "stream": False, "input": "hi"}, headers=AUTH)
    out = json.loads(body or b"{}")
    check("responses nonstream 200", code == 200, (code, body[:300]))
    check("response object shape",
          out.get("object") == "response" and out.get("status") == "completed"
          and out.get("output_text") == "Hello world", out.get("output_text"))
    req = MOCK["requests"][-1]
    check("responses routed to chat upstream as stream",
          req["path"].endswith("/chat/completions")
          and req["body"].get("stream") is True,
          (req["path"], req["body"].get("stream")))

    print("[5] responses API, SSE events")
    code, headers, body = request("POST", "/v1/responses", {
        "model": "gpt-6-astra", "stream": True, "input": "hi"}, headers=AUTH)
    events = [l[7:] for l in body.decode("utf-8", "replace").splitlines()
              if l.startswith("event: ")]
    check("responses stream 200", code == 200, code)
    check("ends with response.completed",
          events and events[-1] == "response.completed", events[-3:])
    completed = [l for l in body.decode("utf-8", "replace").splitlines()
                 if l.startswith("data: ") and "response.completed" in l]
    check("completed frame carries output text",
          bool(completed) and "Hello world" in completed[0], completed[:1])

    print("[5b] tool calls round-trip (Codex core loop)")
    MOCK["mode"] = "tools"
    fn_tool = {"type": "function", "name": "get_weather",
               "description": "query weather",
               "parameters": {"type": "object",
                              "properties": {"city": {"type": "string"}},
                              "required": ["city"]}}
    # chat non-stream: aggregated tool_calls
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "weather in Paris?"}],
        "tools": [{"type": "function", "function": {
            "name": "get_weather", "parameters": fn_tool["parameters"]}}],
        "tool_choice": "auto"}, headers=AUTH)
    out = json.loads(body or b"{}")
    msg = (out.get("choices") or [{}])[0].get("message") or {}
    tcs = msg.get("tool_calls") or []
    args = (tcs[0].get("function") or {}).get("arguments") if tcs else ""
    ok_args = False
    try:
        ok_args = json.loads(args) == {"city": "Paris"}
    except Exception:
        pass
    check("chat aggregated tool_calls merged",
          code == 200 and tcs and tcs[0]["function"]["name"] == "get_weather"
          and ok_args and out["choices"][0]["finish_reason"] == "tool_calls",
          (code, str(out)[:300]))
    check("chat tool definition passed to upstream",
          MOCK["requests"][-1]["body"].get("tools"), MOCK["requests"][-1]["body"].get("tools"))
    # chat stream: tool_call deltas passthrough
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": True,
        "messages": [{"role": "user", "content": "weather?"}],
        "tools": [{"type": "function", "function": {
            "name": "get_weather", "parameters": fn_tool["parameters"]}}]},
        headers=AUTH)
    text = body.decode("utf-8", "replace")
    check("chat stream tool_call deltas passthrough",
          code == 200 and "get_weather" in text and "call_w1" in text
          and text.endswith("data: [DONE]\n\n"), code)
    # responses non-stream: function_call item
    code, _h, body = request("POST", "/v1/responses", {
        "model": "gpt-6-astra", "stream": False, "input": "weather?",
        "tools": [fn_tool]}, headers=AUTH)
    out = json.loads(body or b"{}")
    calls = [o for o in out.get("output") or [] if o.get("type") == "function_call"]
    ok_args = False
    if calls:
        try:
            ok_args = json.loads(calls[0].get("arguments") or "") == {"city": "Paris"}
        except Exception:
            pass
    check("responses nonstream returns function_call item",
          code == 200 and calls and calls[0].get("name") == "get_weather"
          and ok_args and out.get("status") == "completed",
          (code, str(out)[:400]))
    # responses stream: function_call events then completed
    code, _h, body = request("POST", "/v1/responses", {
        "model": "gpt-6-astra", "stream": True, "input": "weather?",
        "tools": [fn_tool]}, headers=AUTH)
    raw = body.decode("utf-8", "replace")
    events = [l[7:] for l in raw.splitlines() if l.startswith("event: ")]
    check("responses stream function_call + completed",
          code == 200 and "response.output_item.done" in events
          and '"get_weather"' in raw
          and events and events[-1] == "response.completed",
          (code, events[-3:] if events else events))

    print("[6] upstream 401 echoes the key -> must be redacted")
    MOCK["mode"] = "unauthorized"
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("401 mapped", code == 401, code)
    check("key scrubbed from error body", API_KEY not in body.decode("utf-8", "replace"),
          body[:200])

    print("[7] upstream 500 passthrough / redirect refused")
    MOCK["mode"] = "error500"
    code, _h, _b = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("500 passthrough", code == 500, code)
    MOCK["mode"] = "redirect"
    code, _h, _b = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("redirect not followed -> 502", code == 502, code)

    print("[8] upstream timeout -> 504")
    MOCK["mode"] = "slow"
    with open(os.path.join(ACCOUNTS, "settings.json"), "w", encoding="utf-8") as fh:
        json.dump({"upstream": {"header_timeout_seconds": 1,
                                "idle_timeout_seconds": 300}}, fh)
    code, _h, _b = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH, timeout=30)
    check("timeout mapped to 504", code == 504, code)
    MOCK["mode"] = "ok"

    print("[9] empty upstream stream -> in-band error, no fake success")
    MOCK["mode"] = "empty"
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": True,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    text = body.decode("utf-8", "replace")
    check("empty stream reports error event",
          '"error"' in text and "empty custom upstream stream" in text, text[:200])
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("empty stream non-streaming -> 502 (no fake completion)",
          code == 502 and b"empty upstream stream" in body, (code, body[:120]))
    code, _h, body = request("POST", "/v1/responses", {
        "model": "gpt-6.1-sol", "stream": False, "input": "hi"}, headers=AUTH)
    check("responses empty stream non-streaming -> 502", code == 502, code)

    print("[10] unknown model falls through to built-in routing")
    MOCK["mode"] = "ok"
    before = len(MOCK["requests"])
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "not-a-custom-model", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("unknown model not served as custom (5xx, upstream untouched)",
          code >= 400 and len(MOCK["requests"]) == before, (code, body[:200]))

    print("[11] bad JSON config -> safe failure, built-in list intact")
    write_config(None, raw="{ this is not json")
    code, payload, ids = models_ids()
    check("models list still 200", code == 200, code)
    check("custom ids gone",
          "gpt-6.1-sol" not in ids and "gpt-6-astra" not in ids, ids)
    check("built-in model survives", "glm-5.3-flash" in ids, ids)
    code, _h, _b = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("request for now-unknown model errors safely", code >= 400, code)

    print("[12] invalid entries skipped, valid one still served")
    bad = [entry("gpt-6.1-sol"),
           {"id": "no-key", "url": UPSTREAM_BASE},
           {"id": "bad-url", "url": "ftp://example.com/v1", "apiKey": API_KEY},
           {"id": "cred-url", "url": "http://user:pw@host/v1", "apiKey": API_KEY}]
    write_config(bad)
    code, payload, ids = models_ids()
    check("valid entry survives", "gpt-6.1-sol" in ids, ids)
    check("invalid entries dropped",
          all(m not in ids for m in ("no-key", "bad-url", "cred-url")), ids)

    print("[13] hot reload: restoring config re-enables the models")
    write_config([entry("gpt-6.1-sol"), entry("gpt-6-astra")])
    code, _p, ids = models_ids()
    check("custom ids back without restart",
          "gpt-6.1-sol" in ids and "gpt-6-astra" in ids, ids)
    MOCK["mode"] = "ok"
    code, _h, body = request("POST", "/v1/chat/completions", {
        "model": "gpt-6.1-sol", "stream": False,
        "messages": [{"role": "user", "content": "hi"}]}, headers=AUTH)
    check("chat works again after reload", code == 200, code)

    print("[14] missing config file -> same safe failure")
    os.remove(config_path())
    code, _p, ids = models_ids()
    check("models list still 200 without config", code == 200, code)
    check("custom ids gone", "gpt-6.1-sol" not in ids, ids)

    print("[15] secret hygiene across the whole surface")
    leak_targets = [USAGE_LOG, os.path.join(ACCOUNTS, "settings.json")]
    for path in leak_targets:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read()
            check("no key in %s" % os.path.basename(path), API_KEY not in content)
    check("key only ever sent to the configured upstream path",
          all(r["path"] == "/v1/chat/completions" for r in MOCK["requests"]),
          [r["path"] for r in MOCK["requests"]])

finally:
    upstream.shutdown()
    gateway.shutdown()

print("PASS=%d FAIL=%d" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
