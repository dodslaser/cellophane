"""Miscellaneous utility functions."""

from typing import Any


def is_instance_or_subclass(obj: Any, cls: type) -> bool:
    """
    Checks if an object is an instance of or a subclass of a class.

    Args:
        obj (Any): The object to check.
        cls (type): The class to check against.

    Returns:
        bool: True if the object is an instance of or a subclass of the class,
            False otherwise.

    Example:
        ```python
        is_instance_or_subclass(1, int)  # True
        is_instance_or_subclass(1.0, int)  # False
        is_instance_or_subclass(int, int)  # True
        is_instance_or_subclass(int, object)  # True
        ```
    """
    if isinstance(obj, type):
        return issubclass(obj, cls) and obj != cls
    else:
        return isinstance(obj, cls)
