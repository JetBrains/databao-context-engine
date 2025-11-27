import logging
import os
import subprocess

from nemory.embeddings.providers.ollama.config import OllamaConfig
from nemory.embeddings.providers.ollama.service import OllamaService

logger = logging.getLogger(__name__)


class OllamaRuntime:
    def __init__(self, service: OllamaService, config: OllamaConfig | None = None):
        self._service = service
        self._config = config or OllamaConfig()

    def start_if_needed(self) -> subprocess.Popen | None:
        if self._service.is_healthy():
            return None

        logger.info("Ollama server not running. Starting Ollama server...")
        cmd = [self._config.bin_path, "serve"]
        env = os.environ.copy()
        env["OLLAMA_HOST"] = f"{self._config.host}:{self._config.port}"
        if self._config.extra_env:
            env.update(self._config.extra_env)

        stdout = subprocess.DEVNULL

        proc = subprocess.Popen(
            cmd,
            cwd=str(self._config.work_dir) if self._config.work_dir else None,
            env=env,
            stdout=stdout,
            stderr=subprocess.STDOUT,
            text=False,
            close_fds=os.name != "nt",
        )

        return proc

    def start_and_await(
        self,
        *,
        timeout: float = 60.0,
        poll_interval: float = 0.5,
    ) -> subprocess.Popen | None:
        already_healthy = self._service.is_healthy()
        proc: subprocess.Popen | None = None

        if not already_healthy:
            proc = self.start_if_needed()

        ok = self._service.wait_until_healthy(timeout=timeout, poll_interval=poll_interval)
        if ok:
            if proc is not None:
                logger.info("Started Ollama server")
            else:
                logger.debug("Ollama server was already running")
            return proc

        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass
            try:
                proc.kill()
            except Exception:
                pass

        raise TimeoutError(
            f"Timed out waiting for Ollama to become healthy at http://{self._config.host}:{self._config.port}"
        )
