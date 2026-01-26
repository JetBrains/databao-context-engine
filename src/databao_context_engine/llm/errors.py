class OllamaError(Exception):
    """Base class for Ollama-related errors."""


class OllamaTransientError(OllamaError):
    """Errors that are likely temporary (network issues, timeouts, 5xx, etc.), typically worth retrying."""


class OllamaPermanentError(OllamaError):
    """Errors that are unlikely to succeed on retry without changing inputs or configuration (4xx, bad response schema, etc.)."""
