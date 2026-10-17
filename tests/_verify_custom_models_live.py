"""Real-upstream practical verification for custom models via wb-proxy.

Covers what an interactive Codex session would exercise against the real
upstream: a tool call the model must emit (function_call round-trip through
/v1/responses), a long-context request (~40KB), and wall-clock latency for
both. Does NOT require the Codex GUI: the traffic shape is identical.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8788"
MODEL = os.environ.get("VERIFY_MODEL", "gpt-6.1-sol")

PASS = FAIL = 0


def check(label, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s" % label)
    else:
        FAIL += 1
        print("  [FAIL] %s  %s" % (label, detail))


def req(method, path, body=None, timeout=300):
    data = None if body is None else json.dumps(body).encode("utf-8")
    r = urllib.request.Request(BASE + path, data=data, method=method)
    r.add_header("Content-Type", "application/json")
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


WEATHER_TOOL = {
    "type": "function",
    "name": "get_weather",
    "description": "查询指定城市当前的天气。回答天气问题必须调用此工具。",
    "parameters": {"type": "object",
                   "properties": {"city": {"type": "string",
                                           "description": "城市名，用拼音"}},
                   "required": ["city"]},
}

try:
    if not wait_ready():
        print("gateway did not start")
        sys.exit(2)

    print("[1] real tool-call round-trip via /v1/responses")
    t0 = time.time()
    code, _h, body = req("POST", "/v1/responses", {
        "model": MODEL, "stream": False, "max_output_tokens": 512,
        "input": "巴黎现在的天气怎么样？你必须调用 get_weather 工具查询，不要凭记忆回答。",
        "tools": [WEATHER_TOOL], "tool_choice": "auto"})
    wall = time.time() - t0
    out = json.loads(body or b"{}")
    calls = [o for o in out.get("output") or [] if o.get("type") == "function_call"]
    args = calls[0].get("arguments") if calls else ""
    ok_args = False
    try:
        ok_args = "paris" in json.loads(args).get("city", "").lower()
    except Exception:
        pass
    check("model emitted a function_call item",
          code == 200 and calls and calls[0].get("name") == "get_weather",
          (code, str(out)[:300]))
    check("arguments are valid JSON naming the queried city",
          bool(calls) and ok_args, args)
    check("completed with usage", out.get("status") == "completed"
          and bool(out.get("usage")), out.get("status"))
    print("  elapsed: %.1fs" % wall)

    print("[2] long-context (~40KB) via /v1/responses")
    marker = "今天的三叶草密码是：蔚蓝色鲸鱼在第%d层打盹。" % 47
    filler = ("这是一段用于填充上下文的普通文字，讲述一只小猫在院子里追蝴蝶、"
              "晒太阳、打盹的日常，没有任何需要记住的关键信息。\n" * 1)
    story = ""
    for i in range(600):
        story += "第%03d段：%s" % (i, filler)
        if i == 300:
            story += marker
    print("  input size: %.1f KB" % (len(story.encode("utf-8")) / 1024))
    t0 = time.time()
    code, _h, body = req("POST", "/v1/responses", {
        "model": MODEL, "stream": False, "max_output_tokens": 128,
        "input": "下面的文章里藏着一个『三叶草密码』，请原样告诉我密码内容。\n\n" + story})
    wall = time.time() - t0
    out = json.loads(body or b"{}")
    text = out.get("output_text") or ""
    check("long-context request completed", code == 200
          and out.get("status") == "completed", (code, str(out)[:200]))
    check("model found the hidden marker", "蔚蓝色鲸鱼" in text and "47" in text,
          text[:200])
    check("usage reported for long context",
          (out.get("usage") or {}).get("input_tokens", 0) > 5000,
          out.get("usage"))
    print("  elapsed: %.1fs" % wall)

    print("[3] same two probes streaming (SSE stability)")
    t0 = time.time()
    code, headers, body = req("POST", "/v1/responses", {
        "model": MODEL, "stream": True, "max_output_tokens": 512,
        "input": "北京现在的天气怎么样？请调用 get_weather 工具查询。",
        "tools": [WEATHER_TOOL]})
    raw = body.decode("utf-8", "replace")
    events = [l[7:] for l in raw.splitlines() if l.startswith("event: ")]
    check("streamed tool-call ends with response.completed",
          code == 200 and events and events[-1] == "response.completed"
          and "get_weather" in raw, (code, events[-2:] if events else events))
    print("  elapsed: %.1fs" % (time.time() - t0))

finally:
    pass

print("PASS=%d FAIL=%d" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
