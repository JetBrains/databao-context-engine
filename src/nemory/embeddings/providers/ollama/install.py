import hashlib
import logging
import os
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

from nemory.system.properties import get_nemory_path

MANAGED_OLLAMA_BIN = Path(get_nemory_path() / "ollama/bin/ollama").expanduser()

logger = logging.getLogger(__name__)


class ArtifactInfo(NamedTuple):
    name: str
    sha256: str


DEFAULT_VERSION = "v0.13.0"

ARTIFACTS: dict[str, ArtifactInfo] = {
    "linux-amd64": ArtifactInfo(
        "ollama-linux-amd64.tgz",
        "c5e5b4840008d9c9bf955ec32c32b03afc57c986ac1c382d44c89c9f7dd2cc30",
    ),
    "linux-arm64": ArtifactInfo(
        "ollama-linux-arm64.tgz",
        "05eb97b87c690fa82626c6f4c7d656ae46ad5f2b7ee6aa324cc19dd88b89982b",
    ),
    "windows-amd64": ArtifactInfo(
        "ollama-windows-amd64.zip",
        "0fc913fc3763b8d2a490f2be90a51d474491ee22ea5a43ff31f1c58301a89656",
    ),
    "windows-arm64": ArtifactInfo(
        "ollama-windows-arm64.zip",
        "84c395e4187bd560cfc7c26b0142d970bcbdf0e0214b007bc527b7954430ea21",
    ),
}


def resolve_ollama_bin() -> str:
    """
    Decide which `ollama` binary to use, in this order:

    1. NEMORY_OLLAMA_BIN env var, if set and exists
    2. `ollama` found on PATH
    3. Managed installation under MANAGED_OLLAMA_BIN

    Returns the full path to the binary
    """
    override = os.environ.get("NEMORY_OLLAMA_BIN")
    if override:
        p = Path(override).expanduser()
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)

    system_ollama = shutil.which("ollama")
    if system_ollama:
        return system_ollama

    if not MANAGED_OLLAMA_BIN.exists():
        logger.info("No existing Ollama installation detected. Nemory will download and install Ollama.")

        if _detect_platform() == "darwin":
            _install_ollama_macos()
            system_ollama = shutil.which("ollama")
            if not system_ollama:
                raise RuntimeError("Ollama installation on macOS appears to have failed.")
            return system_ollama

        install_ollama_to(MANAGED_OLLAMA_BIN)

    return str(MANAGED_OLLAMA_BIN)


def _detect_platform() -> str:
    """
    Return one of: 'darwin', 'linux-amd64', 'linux-arm64', 'windows-amd64', 'windows-arm64'.
    """
    os_name = sys.platform.lower()
    arch = (os.uname().machine if hasattr(os, "uname") else "").lower()

    if os_name.startswith("darwin"):
        return "darwin"
    if os_name.startswith("win"):
        if "arm" in arch or "aarch64" in arch:
            return "windows-arm64"
        return "windows-amd64"
    if os_name.startswith("linux"):
        if "arm" in arch or "aarch64" in arch:
            return "linux-arm64"
        return "linux-amd64"

    raise RuntimeError(f"Unsupported OS/arch: os={os_name!r} arch={arch!r}")


def _download_to_temp(url: str) -> Path:
    """
    Download to a temporary file and return its path.
    """
    import urllib.request

    tmp_dir = Path(tempfile.mkdtemp(prefix="ollama-download-"))
    file_name = url.rsplit("/", 1)[-1]
    dest = tmp_dir / file_name

    logger.info("Downloading %s to %s", url, dest)
    with urllib.request.urlopen(url) as resp, dest.open("wb") as out:
        shutil.copyfileobj(resp, out)

    return dest


def _verify_sha256(path: Path, expected_hex: str) -> None:
    """
    Verify SHA-256 of path matches expected_hex
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    actual = h.hexdigest()
    if actual.lower() != expected_hex.lower():
        raise RuntimeError(f"SHA256 mismatch for {path}: expected {expected_hex}, got {actual}")


def _extract_archive(archive: Path, target_dir: Path) -> None:
    """
    Extract archive into target_dir.
    """
    name = archive.name.lower()
    target_dir.mkdir(parents=True, exist_ok=True)

    if name.endswith(".zip"):
        with ZipFile(archive, "r") as zf:
            zf.extractall(target_dir)
    elif name.endswith(".tgz") or name.endswith(".tar.gz"):
        with tarfile.open(archive, "r:gz") as tf:
            tf.extractall(target_dir)
    else:
        raise RuntimeError(f"Unsupported archive format: {archive}")


def _ensure_executable(path: Path) -> None:
    """
    Mark path as executable
    """
    try:
        mode = path.stat().st_mode
        path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    except Exception:
        pass

def _install_ollama_macos() -> None:
    import shutil

    if not shutil.which("brew"):
        raise RuntimeError(
            "Homebrew is not installed, and Ollama can only be installed "
            "programmatically via Homebrew on macOS. "
            "Please install Homebrew from https://brew.sh/ "
            "or install Ollama manually from https://ollama.com/download."
        )

    logger.info("Running `brew install --cask ollama` to install Ollama on macOS...")
    cmd = ["brew", "install", "--cask", "ollama"]
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Failed to install Ollama on macOS via Homebrew") from exc

def install_ollama_to(target: Path) -> None:
    """
    Ensure an Ollama binary exists.

    If it doesn't exist, this will:
    - detect OS
    - download the archive from GitHub
    - verify its SHA-256 checksum
    - extract into the installation directory
    - make the binary executable
    """
    target = target.expanduser()
    if target.parent.name == "bin":
        install_root = target.parent.parent
    else:
        install_root = target.parent

    install_root.mkdir(parents=True, exist_ok=True)

    platform_key = _detect_platform()
    try:
        artifact = ARTIFACTS[platform_key]
    except KeyError as e:
        raise RuntimeError(f"Unsupported platform: {platform_key}") from e

    url = f"https://github.com/ollama/ollama/releases/download/{DEFAULT_VERSION}/{artifact.name}"
    archive_path = _download_to_temp(url)

    try:
        _verify_sha256(archive_path, artifact.sha256)
        logger.info("Verified SHA256 for %s", archive_path.name)

        _extract_archive(archive_path, install_root)

        candidates: list[Path] = []
        if sys.platform.startswith("win"):
            candidates.extend(
                [
                    install_root / "ollama.exe",
                    install_root / "bin" / "ollama.exe",
                ]
            )
        else:
            candidates.extend(
                [
                    install_root / "ollama",
                    install_root / "bin" / "ollama",
                ]
            )

        binary: Path | None = None
        for c in candidates:
            if c.exists():
                binary = c
                break

        if binary is None:
            raise RuntimeError(f"Installed Ollama archive but could not find binary under {install_root}")

        if binary.resolve() != target.resolve():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(binary, target)

        _ensure_executable(target)
        logger.info("Ollama installed at %s", target)

    finally:
        try:
            archive_path.unlink(missing_ok=True)
        except Exception:
            pass
