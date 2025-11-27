import logging
import time
from typing import Any

import requests

from .config import OllamaConfig

logger = logging.getLogger(__name__)


class OllamaService:
    def __init__(self, config: OllamaConfig, session: requests.Session | None = None):
        self._base = config.base_url.rstrip("/")
        self._timeout = config.timeout
        self._headers = config.headers
        self._session = session or requests.Session()

    def embed(self, *, model: str, text: str) -> list[float]:
        url = f"{self._base}/api/embeddings"

        payload: dict[str, Any] = {
            "model": model,
            "prompt": text,
        }

        try:
            response = self._session.post(url, json=payload, headers=self._headers, timeout=self._timeout)
        except requests.Timeout as e:
            raise TimeoutError(f"Ollama embeddings timed out after {self._timeout}s") from e
        except requests.RequestException:
            raise

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            message = response.text.strip()
            raise requests.HTTPError(f"{e} — body: {message[:500]}") from e

        data = response.json()

        vec = data.get("embedding")
        if not isinstance(vec, list) or not all(isinstance(x, (int, float)) for x in vec):
            alt = data.get("data")
            if isinstance(alt, list) and alt and isinstance(alt[0], dict) and isinstance(alt[0].get("embedding"), list):
                vec = alt[0]["embedding"]
            else:
                raise ValueError("Unexpected Ollama embedding response schema")

        return [float(x) for x in vec]

    def pull_model_if_needed(self, *, model: str, timeout: float = 900.0) -> None:
        if self._is_model_available(model_name=model):
            logger.debug(f"Ollama model {model} was already available, skipping pull")
            return

        self.pull_model(model=model, timeout=timeout)

        logger.debug(f"Ollama model {model} was pulled")

    def pull_model(self, *, model: str, timeout: float = 900.0) -> None:
        url = f"{self._base}/api/pull"

        payload: dict[str, Any] = {"name": model}

        try:
            resp = self._session.post(url, json=payload, headers=self._headers, timeout=timeout)
        except requests.Timeout as e:
            raise TimeoutError(f"Ollama pull timed out after {timeout}s") from e
        except requests.RequestException:
            raise

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            message = (resp.text or "").strip()
            raise requests.HTTPError(f"{e} — body: {message[:500]}") from e

    def is_healthy(self, *, timeout: float = 3.0) -> bool:
        url = f"{self._base}/api/tags"
        try:
            r = self._session.get(url, headers=self._headers, timeout=timeout)
            return 200 <= r.status_code < 300
        except requests.RequestException:
            return False

    def wait_until_healthy(self, *, timeout: float = 60.0, poll_interval: float = 0.5) -> bool:
        deadline = time.monotonic() + float(timeout)
        while time.monotonic() < deadline:
            if self.is_healthy(timeout=min(poll_interval, timeout)):
                return True
            time.sleep(poll_interval)
        return self.is_healthy(timeout=min(poll_interval, timeout))

    def _is_model_available(self, *, model_name, timeout: float = 5.0) -> bool:
        url = f"{self._base}/api/tags"
        try:
            r = self._session.get(url, headers=self._headers, timeout=timeout)

            if 200 <= r.status_code < 300:
                models = r.json().get("models")
                if models and isinstance(models, list):
                    local_model = next((model for model in models if model.get("name") == model_name), None)
                    return local_model is not None

            return False
        except requests.RequestException:
            return False
