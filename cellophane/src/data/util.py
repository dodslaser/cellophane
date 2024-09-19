"""Utility functions for data manipulation."""

from pathlib import Path
from typing import Any

from .container import Container


def as_dict(data: Container, exclude: list[str] | None = None) -> dict[str, Any]:
    """Dictionary representation of a container.

    The returned dictionary will have the same nested structure as the container.

    Args:
    ----
        exclude (list[str] | None): A list of keys to exclude from the returned
            dictionary. Defaults to None.

    Returns:
    -------
        dict: A dictionary representation of the container object.

    Example:
    -------
        ```python
        data = Container(
            key_1 = "value_1",
            key_2 = Container(
                key_3 = "value_3",
                key_4 = "value_4"
            )
        )
        print(as_dict(data))
        # {
        #     "key_1": "value_1",
        #     "key_2": {
        #         "key_3": "value_3",
        #         "key_4": "value_4"
        #     }
        # }
        ```

    """
    return {
        k: as_dict(v) if isinstance(v, Container) else v
        for k, v in data.__data__.items()
        if k not in (exclude or [])
    }


def convert_path_list(data: list[str | Path]) -> list[Path]:
    """Convert a list of strings to a list of Path objects.

    Args:
    ----
        data (list[str | Path]): The list of strings to convert.

    Returns:
    -------
        list[Path]: The list of Path objects.

    """
    return [Path(p) for p in data]
