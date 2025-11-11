from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class OllamaConfig:
    host: str = "127.0.0.1"
    port: int = 11434

    timeout: float = 30.0
    headers: Optional[dict[str, str]] = None

    bin_path: str = "ollama"
    work_dir: Optional[Path] = None
    extra_env: Optional[dict[str, str]] = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"
