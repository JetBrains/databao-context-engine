import hashlib
import logging
import os
import shutil
import stat
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import NamedTuple
from zipfile import ZipFile

MANAGED_OLLAMA_BIN = Path("~/.nemory/ollama/bin/ollama").expanduser()

logger = logging.getLogger(__name__)


class ArtifactInfo(NamedTuple):
    name: str
    sha256: str


DEFAULT_VERSION = "v0.11.8"

ARTIFACTS: dict[str, ArtifactInfo] = {
    "darwin": ArtifactInfo(
        "ollama-darwin.tgz",
        "779ac8ca1ac9f0081471b04b7085392d3efd70cefb840eb739924cb70367de08",
    ),
    "linux-amd64": ArtifactInfo(
        "ollama-linux-amd64.tgz",
        "73b7bff63cb792b7020fee9b918cb53fd9a7cc012ef188ad5cdc48b31ed52198",
    ),
    "linux-arm64": ArtifactInfo(
        "ollama-linux-arm64.tgz",
        "238616870881c44ccdcb0d893aa2da1bd81a1806e68ef697bc6880a93db6baa7",
    ),
    "windows-amd64": ArtifactInfo(
        "ollama-windows-amd64.zip",
        "ba05ed4b40e03d39d8a2b7d218eb584eeb197375df7443f261cd038da6141092",
    ),
    "windows-arm64": ArtifactInfo(
        "ollama-windows-arm64.zip",
        "f05cf14d764318b274458e7bc2e850961a77670bc0a050ac7e41f081f5d9e946",
    ),
}


def resolve_ollama_bin() -> str:
    """
    Decide while `ollama` binary to use, in this order:

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
