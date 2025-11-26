import os
from contextlib import contextmanager


@contextmanager
def env_variable(key: str, value: str):
    current_val = os.environ.get(key, None)
    os.environ[key] = value
    try:
        yield
    finally:
        if current_val:
            os.environ[key] = current_val
        else:
            del os.environ[key]
