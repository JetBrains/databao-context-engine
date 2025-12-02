from pathlib import Path

from nemory.llm import install


def test_resolve_ollama_bin_uses_env_when_executable(tmp_path, monkeypatch):
    fake_bin = tmp_path / "ollama-env"
    fake_bin.write_text("#!/bin/sh\necho ollama\n")
    fake_bin.chmod(0o755)

    monkeypatch.setenv("NEMORY_OLLAMA_BIN", str(fake_bin))
    monkeypatch.setattr(install.shutil, "which", lambda name: None)

    result = install.resolve_ollama_bin()
    assert result == str(fake_bin)


def test_resolve_ollama_bin_ignores_env_if_not_executable(tmp_path, monkeypatch):
    fake_env_bin = tmp_path / "ollama-env"
    monkeypatch.setenv("NEMORY_OLLAMA_BIN", str(fake_env_bin))

    fake_system_bin = "/usr/local/bin/ollama"
    monkeypatch.setattr(install.shutil, "which", lambda name: fake_system_bin)

    result = install.resolve_ollama_bin()
    assert result == fake_system_bin


def test_resolve_ollama_bin_uses_system_ollama_if_present(monkeypatch):
    monkeypatch.delenv("NEMORY_OLLAMA_BIN", raising=False)

    fake_system_bin = "/usr/bin/ollama"
    monkeypatch.setattr(install.shutil, "which", lambda name: fake_system_bin)

    result = install.resolve_ollama_bin()
    assert result == fake_system_bin


def test_resolve_ollama_bin_installs_managed_when_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("NEMORY_OLLAMA_BIN", raising=False)
    monkeypatch.setattr(install.shutil, "which", lambda name: None)

    managed_bin = tmp_path / "ollama" / "bin" / "ollama"
    monkeypatch.setattr(install, "MANAGED_OLLAMA_BIN", managed_bin)

    calls = []

    def fake_install(target: Path) -> None:
        calls.append(target)

    monkeypatch.setattr(install, "install_ollama_to", fake_install)

    result = install.resolve_ollama_bin()

    assert calls == [managed_bin]
    assert result == str(managed_bin)


def test_resolve_ollama_bin_reuses_existing_managed(tmp_path, monkeypatch):
    monkeypatch.delenv("NEMORY_OLLAMA_BIN", raising=False)
    monkeypatch.setattr(install.shutil, "which", lambda name: None)

    managed_bin = tmp_path / "ollama" / "bin" / "ollama"
    managed_bin.parent.mkdir(parents=True, exist_ok=True)
    managed_bin.write_text("#!/bin/sh\necho managed\n")
    managed_bin.chmod(0o755)

    monkeypatch.setattr(install, "MANAGED_OLLAMA_BIN", managed_bin)

    def fail_install(*args, **kwargs):
        raise AssertionError("install_ollama_to should not be called when managed bin exists")

    monkeypatch.setattr(install, "install_ollama_to", fail_install)

    result = install.resolve_ollama_bin()
    assert result == str(managed_bin)
