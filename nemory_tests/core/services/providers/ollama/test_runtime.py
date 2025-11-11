import os
import pytest

from nemory.core.services.providers.ollama.config import OllamaConfig
from nemory.core.services.providers.ollama.runtime import (
    OllamaRuntime,
)


def test_start_if_needed_when_already_healthy_does_not_spawn(monkeypatch, tmp_path):
    svc = _StubService()
    svc.set_healthy(True)

    config = OllamaConfig(bin_path="ollama", host="127.0.0.1", port=11434, work_dir=tmp_path)
    rt = OllamaRuntime(service=svc, config=config)

    popen_calls = []

    def _fake_popen(*args, **kwargs):
        popen_calls.append((args, kwargs))
        return _FakePopen(*args, **kwargs)

    monkeypatch.setattr("subprocess.Popen", _fake_popen)

    proc = rt.start_if_needed()
    assert proc is None
    assert popen_calls == []


def test_start_if_needed_spawns_with_correct_env_and_cwd(monkeypatch, tmp_path):
    svc = _StubService()
    svc.set_healthy(False)

    config = OllamaConfig(
        bin_path="ollama",
        host="0.0.0.0",
        port=12345,
        work_dir=tmp_path,
        extra_env={"FOO": "BAR"},
    )
    rt = OllamaRuntime(service=svc, config=config)

    captured = {}

    def _fake_popen(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return _FakePopen(*args, **kwargs)

    monkeypatch.setattr("subprocess.Popen", _fake_popen)

    proc = rt.start_if_needed()
    assert isinstance(proc, _FakePopen)

    assert captured["args"][0] == ["ollama", "serve"]

    assert captured["kwargs"]["cwd"] == str(tmp_path)

    env = captured["kwargs"]["env"]
    assert env["OLLAMA_HOST"] == "0.0.0.0:12345"
    assert env["FOO"] == "BAR"

    assert captured["kwargs"]["stdout"] is not None
    assert captured["kwargs"]["stderr"] is not None
    assert captured["kwargs"]["text"] is False
    assert captured["kwargs"]["close_fds"] == (os.name != "nt")


def test_start_and_await_already_healthy_returns_none_and_no_spawn(monkeypatch, tmp_path):
    svc = _StubService()
    svc.set_healthy(True)

    config = OllamaConfig(work_dir=tmp_path)
    rt = OllamaRuntime(service=svc, config=config)

    called = {"popen": 0}
    monkeypatch.setattr("subprocess.Popen", lambda *a, **k: called.__setitem__("popen", called["popen"] + 1))

    proc = rt.start_and_await(timeout=5.0, poll_interval=0.01)
    assert proc is None
    assert called["popen"] == 0


def test_start_and_await_starts_then_becomes_healthy(monkeypatch, tmp_path):
    svc = _StubService()
    svc.set_healthy(False)
    svc.set_wait_result(True)

    config = OllamaConfig(bin_path="ollama", host="127.0.0.1", port=11434, work_dir=tmp_path)
    rt = OllamaRuntime(service=svc, config=config)

    spawned = {"proc": None}

    def _fake_popen(*args, **kwargs):
        spawned["proc"] = _FakePopen(*args, **kwargs)
        return spawned["proc"]

    monkeypatch.setattr("subprocess.Popen", _fake_popen)

    proc = rt.start_and_await(timeout=5.0, poll_interval=0.01)
    assert proc is spawned["proc"]

    assert proc.kwargs["env"]["OLLAMA_HOST"] == "127.0.0.1:11434"


def test_start_and_await_times_out_kills_process(monkeypatch, tmp_path):
    svc = _StubService()
    svc.set_healthy(False)
    svc.set_wait_result(False)

    config = OllamaConfig(bin_path="ollama", host="127.0.0.1", port=11434, work_dir=tmp_path)
    rt = OllamaRuntime(service=svc, config=config)

    created = {"proc": None}

    def _fake_popen(*args, **kwargs):
        created["proc"] = _FakePopen(*args, **kwargs)
        return created["proc"]

    monkeypatch.setattr("subprocess.Popen", _fake_popen)

    with pytest.raises(TimeoutError):
        rt.start_and_await(timeout=0.01, poll_interval=0.005)

    assert created["proc"].terminated >= 1 or created["proc"].killed >= 1


class _FakePopen:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.terminated = 0
        self.killed = 0

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.killed += 1


class _StubService:
    def __init__(self):
        self._healthy_now = False
        self._wait_result = True
        self.calls = []

    def set_healthy(self, val: bool):
        self._healthy_now = val

    def set_wait_result(self, val: bool):
        self._wait_result = val

    def is_healthy(self, *args, **kwargs) -> bool:
        self.calls.append(("is_healthy", args, kwargs))
        return self._healthy_now

    def wait_until_healthy(self, *args, **kwargs) -> bool:
        self.calls.append(("wait_until_healthy", args, kwargs))
        return self._wait_result
