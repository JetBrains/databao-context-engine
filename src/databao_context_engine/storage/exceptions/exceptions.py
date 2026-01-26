class RepositoryError(Exception):
    """Base exception for repository errors."""


class IntegrityError(RepositoryError):
    """Raised when a DB constraint is violated."""
