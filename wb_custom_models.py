#!/usr/bin/env python3
"""Custom model upstreams declared in %USERPROFILE%\\.workbuddy\\models.json.

WorkBuddy's desktop app lets users register their own OpenAI-compatible model
endpoints (vendor "Custom"). The desktop client stores that list in
``~/.workbuddy/models.json`` as a JSON array of entries shaped like::

    [
      {
        "id": "gpt-6.1-sol",
        "name": "gpt-6.1-sol",
        "vendor": "Custom",
        "url": "https://api.example.net/v1",
        "apiKey": "sk-...",
        "supportsToolCall": true,
        "supportsImages": true,
        "supportsReasoning": true,
        "useCustomProtocol": false,
        "maxInputTokens": 128000,
        "maxOutputTokens": 32768,
        "reasoning": {"defaultEffort": "low", "supportedEfforts": ["low", "high"]}
      }
    ]

wb_proxy.py merges the model ids into GET /v1/models and routes requests for
those ids straight to the declared upstream instead of the WorkBuddy account
pool.

Safety boundaries enforced here (do not weaken them):

- The apiKey never leaves this process except inside the Authorization header
  sent to the entry's own upstream URL. It is never logged, never written to
  disk by this module, never included in any spec dict returned to callers,
  and is scrubbed from upstream error bodies via redact_text().
- Only http:// and https:// upstreams are accepted; URLs carrying embedded
  credentials (user:pass@host) or fragments are rejected. Redirects are never
  followed (a 3xx surfaces as an HTTP error instead), so a misbehaving
  upstream cannot bounce the bearer token to another host.
- A missing file, unreadable file, malformed JSON or invalid entry is a safe
  failure: the affected model simply disappears from the catalogue and
  requests for it fall through to the regular (built-in) routing. The built-in
  WorkBuddy path is never modified by this module.
- Configuration is re-read whenever the file's (mtime, size) changes, so
  edits take effect without a restart. A torn or partially written file fails
  validation as a whole; the empty catalogue persists until a valid file
  appears (documented in docs/custom-models.md).
"""
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# Resolved at import time, after tests have re-pointed HOME/USERPROFILE.
CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".workbuddy", "models.json")

# Custom upstreams speak the OpenAI Chat Completions protocol; the Responses
# API is served by wb_proxy's translation layer, never by this module.
CHAT_COMPLETIONS_PATH = "/chat/completions"

_BOOL_FIELDS = ("supportsToolCall", "supportsImages", "supportsReasoning")
_INT_FIELDS = ("maxInputTokens", "maxOutputTokens")

# Generic long-secret shapes scrubbed from error bodies in addition to the
# entry's own key: bearer-style tokens and OpenAI-style sk- keys.
_SECRET_PATTERNS = (
    re.compile(r"sk-[A-Za-z0-9_\-]{8,}"),
    re.compile(r"Bearer\s+[A-Za-z0-9._\-]{12,}", re.IGNORECASE),
)

_logger = None  # injected by wb_proxy: callable(message:str)
_on_change = None  # injected by wb_proxy: callable(specs) on successful parse


def set_logger(fn):
    global _logger
    _logger = fn


def set_on_change(fn):
    """Register a callback fired after every successful config re-parse.

    wb_proxy uses it to (re)mint pricing policies for entries that declare
    their own rates; kept as a hook so this module stays free of wb_pricing.
    """
    global _on_change
    _on_change = fn


def _log(message):
    try:
        if _logger:
            _logger(message)
    except Exception:
        pass


def config_path():
    return CONFIG_PATH


class ConfigError(ValueError):
    """A models.json entry (or the whole file) failed validation."""


# ---- parsing / validation -------------------------------------------------

def _clean_url(raw):
    """Validate a custom upstream base URL; return it normalised or raise."""
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError("url missing")
    text = raw.strip()
    try:
        parsed = urllib.parse.urlsplit(text)
    except ValueError as exc:
        raise ConfigError("url unparseable: %s" % exc)
    if parsed.scheme not in ("http", "https"):
        raise ConfigError("url scheme must be http/https")
    if not parsed.hostname:
        raise ConfigError("url has no hostname")
    if parsed.username or parsed.password:
        raise ConfigError("url must not embed credentials")
    if parsed.fragment:
        raise ConfigError("url must not carry a fragment")
    if parsed.query:
        raise ConfigError("url must not carry a query string")
    return text.rstrip("/")


def _public_meta(item):
    """Capability metadata safe to expose via /v1/models (never the key/url)."""
    meta = {"vendor": item.get("vendor") or "Custom"}
    for field in _BOOL_FIELDS:
        if field in item:
            meta[field] = bool(item[field])
    for field in _INT_FIELDS:
        value = item.get(field)
        if isinstance(value, (int, float)) and value > 0:
            meta[field] = int(value)
    reasoning = item.get("reasoning")
    if isinstance(reasoning, dict):
        clean = {}
        efforts = reasoning.get("supportedEfforts")
        if isinstance(efforts, list) and all(isinstance(e, str) for e in efforts):
            clean["supportedEfforts"] = list(efforts)
        for field in ("defaultEffort", "effort"):
            if isinstance(reasoning.get(field), str):
                clean[field] = reasoning[field]
        if clean:
            meta["reasoning"] = clean
    return meta


def parse_config(text):
    """Parse models.json text -> list of internal spec dicts.

    Raises ConfigError for whole-file problems. Invalid entries are skipped
    with a warning (one bad model must not disable the others).
    """
    try:
        data = json.loads(text)
    except ValueError as exc:
        raise ConfigError("invalid JSON: %s" % exc)
    if isinstance(data, dict) and isinstance(data.get("models"), list):
        # Tolerate a wrapped {"models": [...]} document.
        data = data["models"]
    if not isinstance(data, list):
        raise ConfigError("expected a JSON array of model entries")
    specs, seen = [], set()
    for index, item in enumerate(data):
        try:
            spec = _parse_entry(item)
        except ConfigError as exc:
            _log("custom models: entry #%d skipped (%s)" % (index, exc))
            continue
        if spec["id"] in seen:
            _log("custom models: duplicate id %r ignored" % spec["id"])
            continue
        seen.add(spec["id"])
        specs.append(spec)
    return specs


def _parse_pricing(item):
    """Optional per-model price declaration -> wb_pricing rate fields.

    Accepted (all per million tokens)::

        "pricing": {"input_per_million": 10.0,
                    "cached_input_per_million": 1.0,   # optional, defaults to input
                    "output_per_million": 50.0,
                    "currency": "USD"}                  # USD (default) or CNY

    Declaring pricing also fixes a real hazard: OpenRouter's price index
    contains same-named models, so without an explicit declaration a custom
    model can be silently cost-estimated at a stranger's rates.
    """
    raw = item.get("pricing")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ConfigError("pricing must be an object")
    fields = {}
    for src, dst in (("input_per_million", "input_cache_miss"),
                     ("cached_input_per_million", "input_cache_hit"),
                     ("output_per_million", "output")):
        value = raw.get(src)
        if value is None:
            continue
        try:
            value = float(value)
        except (TypeError, ValueError):
            raise ConfigError("pricing.%s must be a number" % src)
        if not (value > 0):
            raise ConfigError("pricing.%s must be > 0" % src)
        fields[dst] = value
    if "input_cache_miss" not in fields or "output" not in fields:
        raise ConfigError("pricing needs input_per_million and output_per_million")
    fields.setdefault("input_cache_hit", fields["input_cache_miss"])
    currency = str(raw.get("currency") or "USD").upper()
    if currency not in ("USD", "CNY"):
        raise ConfigError("pricing.currency must be USD or CNY")
    fields["currency"] = currency
    return fields


def _parse_entry(item):
    if not isinstance(item, dict):
        raise ConfigError("entry is not an object")
    mid = item.get("id") or item.get("name")
    if not isinstance(mid, str) or not mid.strip():
        raise ConfigError("id missing")
    mid = mid.strip()
    url = _clean_url(item.get("url"))
    key = item.get("apiKey") or item.get("api_key")
    if not isinstance(key, str) or not key.strip():
        raise ConfigError("apiKey missing")
    return {
        "id": mid,
        "url": url,
        "host": urllib.parse.urlsplit(url).netloc,
        "key": key.strip(),
        "meta": _public_meta(item),
        "pricing": _parse_pricing(item),
    }


# ---- atomic-ish cached load ------------------------------------------------

_cache = {"key": None, "specs": [], "error": None}
_cache_lock = threading.Lock()


def _log_config_error(error):
    # Only shout when the failure actually changes, so a broken file sitting
    # on disk does not spam the log on every request.
    with _cache_lock:
        changed = error != _cache["error"]
    if changed:
        _log("custom models: %s" % error)


def load(force=False):
    """Re-read models.json when it changed; return the public spec list.

    Never raises. Returns [] when the file is missing, unreadable or invalid;
    the reason is logged (rate-limited by message) and retrievable via
    last_error(). A previously valid parse stays cached until the file's
    (mtime, size) changes, so a torn rewrite cannot flap the catalogue.
    """
    path = CONFIG_PATH
    try:
        stat = os.stat(path)
        cache_key = (stat.st_mtime_ns, stat.st_size)
    except FileNotFoundError:
        with _cache_lock:
            _cache["key"], _cache["specs"], _cache["error"] = None, [], None
        return []
    except OSError as exc:
        _log_config_error("unreadable: %s" % exc)
        with _cache_lock:
            _cache["key"], _cache["specs"], _cache["error"] = None, [], str(exc)
        return []
    with _cache_lock:
        if not force and _cache["key"] == cache_key:
            return _cache["specs"]
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
        specs = parse_config(text)
        error = None
    except (OSError, UnicodeDecodeError, ConfigError) as exc:
        # Safe failure: the catalogue (just the custom half) goes empty and
        # the reason is cached so a broken file does not re-log per request.
        _log_config_error("config rejected: %s" % exc)
        with _cache_lock:
            _cache["key"], _cache["specs"], _cache["error"] = cache_key, [], str(exc)
        return []
    with _cache_lock:
        _cache["key"], _cache["specs"], _cache["error"] = cache_key, specs, error
    if _on_change:
        try:
            _on_change(specs)
        except Exception as exc:
            _log("custom models: on-change hook failed: %s" % exc)
    return specs


def last_error():
    with _cache_lock:
        return _cache["error"]


def public_models():
    """Spec list safe for /v1/models: id + capability metadata, no secrets."""
    specs = load()
    return [{"id": s["id"], "meta": s["meta"]} for s in specs]


def upstream_for(model_id):
    """Private routing spec for model_id, or None. Holds the apiKey."""
    if not model_id or not isinstance(model_id, str):
        return None
    for spec in load():
        if spec["id"] == model_id:
            return spec
    return None


# ---- secret scrubbing -------------------------------------------------------

def redact_text(text, spec=None):
    """Scrub the entry's key (and generic secret shapes) out of free text.

    Used on upstream error bodies and exception strings before they reach the
    client response or the log, so a chatty upstream can never leak the key.
    """
    if not isinstance(text, str):
        return text
    keys = []
    if spec:
        key = spec.get("key")
        if key:
            keys.append(key)
    for key in keys:
        text = text.replace(key, "[redacted]")
    return _SECRET_PATTERNS[0].sub("[redacted]", _SECRET_PATTERNS[1].sub(
        "Bearer [redacted]", text))


# ---- upstream HTTP -----------------------------------------------------------

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Reject redirects instead of following them.

    A 3xx then surfaces as urllib.error.HTTPError at the caller, so the
    bearer key can never be replayed against a host the user did not
    configure.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


def upstream_url(spec):
    return spec["url"] + CHAT_COMPLETIONS_PATH


def open_chat(spec, body, timeout=120.0):
    """POST body to the entry's /chat/completions and return the response.

    The caller owns the response (close it). Raises urllib.error.HTTPError
    (including for 3xx), socket.timeout and urllib.error.URLError; exception
    strings contain the URL, never the key.
    """
    url = upstream_url(spec)
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "Authorization": "Bearer " + spec["key"],
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "User-Agent": "wb-proxy-custom/1.0",
    })
    resp = _OPENER.open(req, timeout=timeout)
    # Belt and braces: the no-redirect opener should already guarantee this.
    final = urllib.parse.urlsplit(getattr(resp, "geturl", lambda: url)())
    origin = urllib.parse.urlsplit(spec["url"])
    if (final.scheme, final.hostname) != (origin.scheme, origin.hostname):
        try:
            resp.close()
        except Exception:
            pass
        raise urllib.error.URLError("custom upstream redirected to another host")
    return resp
