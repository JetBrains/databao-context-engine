from typing import Any
import json
import pytest
import requests

from nemory.core.services.providers.ollama.service import (
    OllamaService,
    OllamaConfig,
)


def test_embed_embedding_field():
    session = _StubSession()
    session.set_next_post(_StubResponse(status=200, json_obj={"embedding": [1, 2.5, 3]}))
    service = OllamaService(OllamaConfig(host="host", port=11434, timeout=12.0), session=session)

    vec = service.embed(model="nomic-embed-text", text="hello")

    assert vec == [1.0, 2.5, 3.0]
    call = session.calls[0]
    assert call["url"] == "http://host:11434/api/embeddings"
    assert call["json"] == {"model": "nomic-embed-text", "prompt": "hello"}
    assert call["timeout"] == 12.0


def test_embed_alt_schema_under_data_embedding():
    session = _StubSession()
    session.set_next_post(_StubResponse(status=200, json_obj={"data": [{"embedding": [0.1, 0.2]}]}))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    vec = service.embed(model="m", text="t")
    assert vec == [0.1, 0.2]


def test_embed_timeout_raises_timeouterror():
    session = _StubSession()
    session.set_next_post(requests.Timeout("boom"))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    with pytest.raises(TimeoutError):
        service.embed(model="m", text="t")


def test_embed_http_error_includes_body():
    session = _StubSession()
    session.set_next_post(_StubResponse(status=500, json_obj={"error": "x"}, text="server blew up"))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    with pytest.raises(requests.HTTPError) as ei:
        service.embed(model="m", text="t")
    assert "body:" in str(ei.value)
    assert "server blew up" in str(ei.value)


def test_embed_malformed_json_raises_valueerror():
    session = _StubSession()
    session.set_next_post(_StubResponse(status=200, json_obj=json.JSONDecodeError("err", "doc", 0)))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    with pytest.raises(ValueError):
        service.embed(model="m", text="t")


def test_embed_unexpected_schema_raises_valueerror():
    session = _StubSession()
    session.set_next_post(_StubResponse(status=200, json_obj={"nope": 123}))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    with pytest.raises(ValueError):
        service.embed(model="m", text="t")


def test_pull_model_waits_until_completion():
    sess = _StubSession()
    sess.set_next_post(_StubResponse(status=200, json_obj=None, text='{"status":"success"}'))
    service = OllamaService(OllamaConfig(host="host", port=11434), session=sess)

    service.pull_model(model="nomic-embed-text", timeout=5.0)

    call = sess.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://host:11434/api/pull"
    assert call["json"] == {"name": "nomic-embed-text"}


def test_pull_model_http_error_raises_with_body_snippet():
    sess = _StubSession()
    sess.set_next_post(_StubResponse(status=500, json_obj=None, text="internal boom"))
    service = OllamaService(OllamaConfig(host="x"), session=sess)

    with pytest.raises(requests.HTTPError) as ei:
        service.pull_model(model="m", timeout=1.0)
    assert "body:" in str(ei.value)
    assert "internal boom" in str(ei.value)


def test_pull_model_timeout_raises_timeouterror():
    session = _StubSession()
    session.set_next_post(requests.Timeout("slow"))
    service = OllamaService(OllamaConfig(host="x"), session=session)

    with pytest.raises(TimeoutError):
        service.pull_model(model="m", timeout=0.01)


def test_is_healthy_true_on_2xx():
    session = _StubSession()
    session.set_next_get(_StubResponse(status=200))
    service = OllamaService(OllamaConfig(host="h", port=11434), session=session)

    assert service.is_healthy(timeout=0.1) is True
    call = session.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "http://h:11434/api/tags"


def test_is_healthy_false_on_non_2xx():
    session = _StubSession()
    session.set_next_get(_StubResponse(status=503))
    service = OllamaService(OllamaConfig(host="h"), session=session)

    assert service.is_healthy() is False


def test_is_healthy_false_on_exception():
    session = _StubSession()
    session.set_next_get(requests.RequestException("net down"))
    service = OllamaService(OllamaConfig(host="h"), session=session)

    assert service.is_healthy() is False


def test_wait_until_healthy_becomes_true(monkeypatch):
    session = _StubSession()
    service = OllamaService(OllamaConfig(host="h"), session=session)

    states = iter([False, False, True])
    monkeypatch.setattr(service, "is_healthy", lambda timeout=0.5: next(states))
    monkeypatch.setattr("time.sleep", lambda _: None)

    assert service.wait_until_healthy(timeout=5.0, poll_interval=0.01) is True


def test_wait_until_healthy_times_out(monkeypatch):
    session = _StubSession()
    service = OllamaService(OllamaConfig(host="h"), session=session)

    monkeypatch.setattr(service, "is_healthy", lambda timeout=0.5: False)
    monkeypatch.setattr("time.sleep", lambda _: None)

    assert service.wait_until_healthy(timeout=0.05, poll_interval=0.01) is False


class _StubResponse:
    def __init__(self, status: int = 200, json_obj: Any = None, text: str = ""):
        self.status_code = status
        self._json = json_obj
        self.text = text

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            r = requests.Response()
            r.status_code = self.status_code
            r.reason = "Stubbed Error"
            raise requests.HTTPError(f"{self.status_code} {r.reason}")

    def json(self):
        if isinstance(self._json, Exception):
            raise self._json
        return self._json


class _StubSession:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []
        self._next_post = _StubResponse()
        self._next_get = _StubResponse()

    def set_next_post(self, resp):
        self._next_post = resp

    def set_next_get(self, resp):
        self._next_get = resp

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"method": "POST", "url": url, "json": json, "headers": headers, "timeout": timeout})
        tgt = self._next_post
        if isinstance(tgt, Exception):
            raise tgt
        return tgt

    def get(self, url, headers=None, timeout=None):
        self.calls.append({"method": "GET", "url": url, "headers": headers, "timeout": timeout})
        tgt = self._next_get
        if isinstance(tgt, Exception):
            raise tgt
        return tgt
