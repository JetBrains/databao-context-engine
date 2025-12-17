from typing import Literal

from typing_extensions import TypeIs


class Ok[T]:
    def __init__(self, value: T) -> None:
        self._value = value

    def is_ok(self) -> Literal[True]:
        return True

    def is_err(self) -> Literal[False]:
        return False

    @property
    def ok_value(self) -> T:
        """
        Return the inner value.
        """
        return self._value


class Err[E]:
    def __init__(self, value: E) -> None:
        self._value = value

    def is_ok(self) -> Literal[False]:
        return False

    def is_err(self) -> Literal[True]:
        return True

    @property
    def err_value(self) -> E:
        """
        Return the inner value.
        """
        return self._value


type Result[T, E] = Ok[T] | Err[E]


def is_ok[T, E](result: Result[T, E]) -> TypeIs[Ok[T]]:
    return result.is_ok()


def is_err[T, E](result: Result[T, E]) -> TypeIs[Err[E]]:
    return result.is_err()
