"""Miscellaneous utility functions."""

from typing import Any
import logging
from attrs import define, field


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


@define
class freeze_logs:
    """
    Context manager to suppress logging output.

    Example:
        ```python
        with silence_logs():
            logging.info("This will not be printed")
        ```
    """
    logger: logging.Logger = field(default=logging.root)
    original_handlers: set[logging.Handler] = field(factory=set)
    original_level: int = field(default=logging.CRITICAL)


    def __enter__(self) -> None:
        self.original_level = self.logger.level
        self.original_handlers = {*self.logger.handlers}
        self.logger.setLevel(logging.CRITICAL + 1)


    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        del args, kwargs # Unused
        for handler in {*self.logger.handlers} ^ self.original_handlers:
            handler.close()
            self.logger.removeHandler(handler)
        self.logger.setLevel(self.original_level)


