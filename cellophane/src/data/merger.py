"""Merger implementation for reconciling data."""

from typing import Any, Callable


class Merger:
    """Reconciles two values based on their attribute name."""

    _impls: dict[str, Callable]

    def __init__(self) -> None:
        self._impls: dict[str, Callable] = {}

    def register(self, name: str) -> Callable:
        """
        Decorator for registering a new merge implementation for an attribute.

        The decorated function should take the two values to merge as arguments
        and return the merged value.

        This function will be called via reduce, so the first argument will be
        the result of the previous call to the function, or the first value if
        no previous calls have been made.

        Args:
            name (str): The name of attribute to merge.

        Returns:
            Callable: A decorator that registers the decorated function as the
                implementation for the specified name.
        """

        def wrapper(impl: Callable) -> Callable:
            self._impls[name] = impl
            return impl

        return wrapper

    def __call__(self, name: str, this: Any, that: Any) -> Any:
        """
        Reconciles two values based on their attribute name.

        Args:
            name (str): The name of the values (unused).
            this (Any): The first value.
            that (Any): The second value.

        Returns:
            Any: The reconciled value.
        """
        if name in self._impls:
            return self._impls[name](this, that)

        return this or that if this is None or that is None else (this, that)
