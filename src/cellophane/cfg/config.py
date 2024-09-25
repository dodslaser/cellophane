"""Configuration object based on a schema."""

from typing import Any

from attrs import define, field

from cellophane import data

from .jsonschema_ import get_flags
from .schema import Schema


@define(init=False, slots=False)
class Config(data.Container):
    """Represents a configuration object based on a schema.

    Attributes:
    ----------
        schema (Schema): The schema associated with the configuration.

    Methods:
    -------
        __init__(
            schema: Schema,
            allow_empty: bool = False,
            _data: dict | None = None,
            **kwargs,
        ):
            Initializes the Config object with the given schema and data.

    Args:
    ----
        schema (Schema): The schema associated with the configuration.
        allow_empty (bool, optional): Allow empty configuration. Defaults to False.
        __data__ (dict | None, optional): The data for the configuration.
            Defaults to None.
        **kwargs: Additional keyword arguments for the configuration.

    Example:
    -------
        ```python
        schema = Schema()
        config = Config(schema)

        # Creating a configuration from a file
        path = "config.yaml"
        config = Config.from_file(path, schema)
        ```

    """

    __schema__: Schema = field(repr=False, factory=Schema, init=False)

    def __init__(
        self,
        schema: Schema,
        _data: dict | None = None,
        include_defaults: bool = True,
        **kwargs: Any,
    ) -> None:
        self.__schema__ = schema

        for flag in get_flags(schema, _data):
            if flag.flag in kwargs:
                self[flag.key] = flag.convert(kwargs[flag.flag])
            elif flag.value is not None:
                self[flag.key] = flag.convert(flag.value)
            elif flag.default is not None and include_defaults:
                self[flag.key] = flag.convert(flag.default)

    def set_defaults(self) -> None:
        """Updates the configuration from keyword arguments"""
        for flag in get_flags(self.__schema__):
            if flag.default is not None and flag.key not in self:
                self[flag.key] = flag.convert(flag.default)
