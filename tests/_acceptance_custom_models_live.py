"""Real-upstream acceptance for custom models via wb-proxy on 127.0.0.1:8788.

Reads the real ~/.workbuddy/models.json (keys stay in memory, never printed)
and exercises both custom models on /v1/chat/completions and /v1/responses
(streaming + non-streaming), the built-in glm-5.3-flash regression, wrong
client key handling, and scans every response body plus the proxy log for
key leakage.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8788"
LOG = r"C://Users//Administrator//AppData//Local//Temp//wbproxy-live.log"
CUSTOM = ["gpt-6.1-sol", "gpt-6-astra"]

with open(os.path.join(os.path.expanduser("~"), ".workbuddy", "models.json"),
          encoding="utf-8") as fh:
    cfg = json.load(fh)
KEYS = [e["apiKey"] for e in cfg]
HOSTS = [e["url"] for e in cfg]

PASS = FAIL = 0


def check(label, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s" % label)
    else:
        FAIL += 1
        print("  [FAIL] %s  %s" % (label, detail))


def req(method, path, body=None, headers=None, timeout=120):
    url = BASE + path
    data = None if body is None else json.dumps(body).encode("utf-8")
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def wait_ready():
    for _ in range(120):
        try:
            code, _h, _b = req("GET", "/health", timeout=2)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def sse_events(raw):
    out = []
    for block in raw.decode("utf-8", "replace").split("\n\n"):
        data = ""
        for line in block.splitlines():
            if line.startswith("data: "):
                data += line[6:]
        if data and data != "[DONE]":
            try:
                out.append(json.loads(data))
            except Exception:
                pass
    return out


BODIES = []  # every response body, for the leak scan


def grab(result):
    BODIES.append(result[2].decode('utf-8', 'replace'))
    return result
    BODIES.append(body.decode("utf-8", "replace"))
    return code, body


try:
    if not wait_ready():
        print("gateway did not start")
        sys.exit(2)
    print("[1] GET /v1/models")
    code, _h, body = grab(req("GET", "/v1/models"))
    payload = json.loads(body or b"{}")
    ids = [m.get("id") for m in payload.get("data") or []]
    check("models 200", code == 200, code)
    for mid in CUSTOM:
        check("%s listed" % mid, mid in ids, ids)
    check("glm-5.3-flash listed", "glm-5.3-flash" in ids, ids)
    check("owned_by=custom", all(
        m.get("owned_by") == "custom" for m in payload["data"]
        if m.get("id") in CUSTOM))
    blob = json.dumps(payload)
    check("no apiKey in models payload",
          all(k not in blob for k in KEYS))

    print("[2] /v1/chat/completions non-stream (real upstream)")
    for mid in CUSTOM:
        code, _h, body = grab(req("POST", "/v1/chat/completions", {
            "model": mid, "stream": False, "max_tokens": 64,
            "messages": [{"role": "user", "content": "用一句话回答：1+1等于几？"}]}))
        out = json.loads(body or b"{}")
        content = ((out.get("choices") or [{}])[0].get("message") or {}).get("content")
        check("%s chat nonstream 200 with content" % mid,
              code == 200 and isinstance(content, str) and content.strip(),
              (code, str(out)[:300]))

    print("[3] /v1/chat/completions stream (real upstream)")
    for mid in CUSTOM:
        code, headers, body = grab(req("POST", "/v1/chat/completions", {
            "model": mid, "stream": True, "max_tokens": 64,
            "messages": [{"role": "user", "content": "用一句话回答：天空为什么是蓝的？"}]}))
        ctype = headers.get("Content-Type") or ""
        chunks = sse_events(body)
        text = "".join(
            ((c.get("choices") or [{}])[0].get("delta") or {}).get("content") or ""
            for c in chunks)
        usage = [c.get("usage") for c in chunks if c.get("usage")]
        check("%s chat stream SSE + text" % mid,
              code == 200 and "text/event-stream" in ctype
              and bool(text.strip()) and body.endswith(b"data: [DONE]\n\n")
              and bool(usage),
              (code, ctype, text[:80], bool(usage)))

    print("[4] /v1/responses non-stream (real upstream)")
    for mid in CUSTOM:
        code, _h, body = grab(req("POST", "/v1/responses", {
            "model": mid, "stream": False, "max_output_tokens": 64,
            "input": "用一句话回答：水的沸点是多少？"}))
        out = json.loads(body or b"{}")
        check("%s responses nonstream completed" % mid,
              code == 200 and out.get("status") == "completed"
              and out.get("output_text", "").strip(),
              (code, str(out)[:300]))

    print("[5] /v1/responses stream (real upstream)")
    for mid in CUSTOM:
        code, headers, body = grab(req("POST", "/v1/responses", {
            "model": mid, "stream": True, "max_output_tokens": 64,
            "input": "用一句话回答：中国最长的河流是？"}))
        ctype = headers.get("Content-Type") or ""
        raw = body.decode("utf-8", "replace")
        events = [l[7:] for l in raw.splitlines() if l.startswith("event: ")]
        check("%s responses stream SSE completed" % mid,
              code == 200 and "text/event-stream" in ctype
              and events and events[-1] == "response.completed",
              (code, ctype, events[-2:] if events else events))

    print("[6] built-in glm-5.3-flash regression (official pool path)")
    code, _h, body = grab(req("POST", "/v1/chat/completions", {
        "model": "glm-5.3-flash", "stream": False, "max_tokens": 64,
        "messages": [{"role": "user", "content": "用一句话回答：1+1等于几？"}]}))
    out = json.loads(body or b"{}")
    content = ((out.get("choices") or [{}])[0].get("message") or {}).get("content")
    check("glm-5.3-flash chat nonstream ok", code == 200 and content and content.strip(),
          (code, str(out)[:300]))
    code, headers, body = grab(req("POST", "/v1/chat/completions", {
        "model": "glm-5.3-flash", "stream": True, "max_tokens": 64,
        "messages": [{"role": "user", "content": "用一句话回答：你好"}]}))
    chunks = sse_events(body)
    check("glm-5.3-flash chat stream ok",
          code == 200 and bool(chunks), (code, len(chunks)))

    print("[7] wrong client API key handling")
    code, _h, body = grab(req("GET", "/v1/models",
                              headers={"Authorization": "Bearer sk-wrong"}))
    # Existing gateway semantics: with zero client keys configured (this
    # deployment), auth is open and a wrong key is simply ignored. When keys
    # are configured, the same request must 401. Either way: no crash, no
    # key echo, list still valid.
    check("wrong key handled safely",
          code in (200, 401) and b'"object": "list"' in body or code == 401,
          code)

    print("[8] secret scan across responses + proxy log")
    with open(LOG, encoding="utf-8", errors="replace") as fh:
        log_tail = fh.read()
    for i, key in enumerate(KEYS):
        check("upstream key #%d not in any response body" % i,
              key not in "".join(BODIES))
        check("upstream key #%d not in proxy log" % i, key not in log_tail)

finally:
    pass

print("PASS=%d FAIL=%d" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
