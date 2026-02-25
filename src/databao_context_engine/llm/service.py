import logging
import textwrap
import time
from typing import Any

import requests

from databao_context_engine.llm.config import OllamaConfig
from databao_context_engine.llm.errors import OllamaPermanentError, OllamaTransientError

logger = logging.getLogger(__name__)


class OllamaService:
    def __init__(self, config: OllamaConfig, session: requests.Session | None = None):
        self._base = config.base_url.rstrip("/")
        self._timeout = config.timeout
        self._headers = config.headers
        self._session = session or requests.Session()

    def embed(self, *, model: str, text: str) -> list[float]:
        payload: dict[str, Any] = {
            "model": model,
            "input": text,
        }
        data = self._request_json(method="POST", path="/api/embed", json=payload)

        vectors = data.get("embeddings")
        if isinstance(vectors, list):
            vector = vectors[0]
            if isinstance(vector, list) and all(isinstance(n, (int, float)) for n in vector):
                return [float(n) for n in vector]

        raise ValueError(f"Unexpected Ollama embedding response schema. {data}")

    def embed_many(self, *, model: str, texts: list[str]) -> list[list[float]]:
        payload = {"model": model, "input": texts, "truncate": True}
        data = self._request_json(method="POST", path="/api/embed", json=payload)

        vectors = data.get("embeddings")
        if not (isinstance(vectors, list) and all(isinstance(v, list) for v in vectors)):
            raise ValueError(f"Unexpected embedding response schema. {data}")

        return [[float(n) for n in vec] for vec in vectors]

    def describe(self, *, model: str, text: str, context: str) -> str:
        """Ask Ollama to generate a short description for `text`."""
        prompt = self._build_description_prompt(text=text, context=context)

        return self.prompt(model=model, prompt=prompt)

    def prompt(self, *, model: str, prompt: str, temperature: float = 0.1, timeout: float | None = None) -> str:
        """Ask Ollama to generate a response for `text`."""
        payload: dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
        }
        data = self._request_json(method="POST", path="/api/generate", json=payload, timeout=timeout)

        response_text = data.get("response")
        if not isinstance(response_text, str):
            raise ValueError("Unexpected Ollama generate response schema (missing 'response' string)")
        return response_text.strip()

    def pull_model_if_needed(self, *, model: str, timeout: float = 900.0) -> None:
        if self._is_model_available(model_name=model):
            logger.debug(f"Ollama model {model} was already available, skipping pull")
            return

        logger.info("Ollama model %s not found locally. Pulling it (this may take several minutes)...", model)
        self.pull_model(model=model, timeout=timeout)
        logger.info("Ollama model %s pulled successfully", model)

    def pull_model(self, *, model: str, timeout: float = 900.0) -> None:
        payload: dict[str, Any] = {"name": model}
        self._request(method="POST", path="/api/pull", json=payload, timeout=timeout)

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

    def _request(
        self,
        *,
        method: str,
        path: str,
        timeout: float | None = None,
        **kwargs,
    ) -> requests.Response:
        url = f"{self._base}{path}"
        try:
            resp = self._session.request(
                method,
                url,
                headers=self._headers,
                timeout=timeout or self._timeout,
                **kwargs,
            )
        except requests.Timeout as e:
            raise OllamaTransientError(f"Ollama request to {path} timed out after {timeout}s") from e
        except requests.RequestException as e:
            raise OllamaTransientError(f"Ollama request to {path} failed: {e}") from e

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise OllamaPermanentError(f"Ollama error {resp.status_code} for {path}: {resp.text}") from e

        return resp

    def _request_json(
        self,
        *,
        method: str,
        path: str,
        timeout: float | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        resp = self._request(method=method, path=path, timeout=timeout, **kwargs)
        try:
            return resp.json()
        except ValueError as e:
            raise OllamaPermanentError(f"Invalid JSON from Ollama for {path}") from e

    @staticmethod
    def _build_description_prompt(text: str, context: str) -> str:
        base = """
        You are a helpful assistant.
        
        I will give you some TEXT and CONTEXT.
        Write a concise, human-readable description of the TEXT suitable for displaying in a UI.
        - 1-2 sentences
        - Be factual and avoid speculation
        - No markdown
        - No preambles or labels, just the description itself.
        - Your entire reply MUST be only the description itself. No extra commentary.
        
        CONTEXT:
        {context}
        
        TEXT:
        {text}
        """

        return textwrap.dedent(base).format(context=context, text=text).strip()
