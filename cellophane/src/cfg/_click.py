"""Click related utilities for configuration."""

import re
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Type, get_args

import rich_click as click
from attrs import define, field
from humanfriendly import parse_size

ITEMS_TYPES = Literal[
    "string",
    "number",
    "integer",
    "path",
    "mapping",
    "size",
]

SCHEMA_TYPES = Literal[
    "string",
    "number",
    "integer",
    "boolean",
    "mapping",
    "array",
    "path",
    "size",
]

from . import data


class StringMapping(click.ParamType):
    """
    Represents a click parameter type for comma-separated mappings.

    Attributes:
        name (str): The name of the parameter type.
        scanner (re.Scanner): The regular expression scanner used for parsing mappings.

    Methods:
        convert(
            value: str | Mapping,
            param: click.Parameter | None,
            ctx: click.Context | None,
        ) -> Mapping | None:
            Converts the input value to a mapping.

    Args:
        value (str | Mapping): The input value to be converted.
        param (click.Parameter | None): The click parameter associated with the value.
        ctx (click.Context | None): The click context.

    Returns:
        Mapping | None: The converted mapping.

    Raises:
        click.BadParameter: When the input value is not a valid comma-separated mapping.

    Example:
        ```python
        mapping_type = StringMapping()
        value = "a=1, b=2, c=3"
        param = None
        ctx = None

        result = mapping_type.convert(value, param, ctx)
        print(result)  # Output: {'a': '1', 'b': '2', 'c': '3'}
        ```
    """

    name = "mapping"
    scanner = re.Scanner(  # type: ignore[attr-defined]
        [
            (r'"[^"]*"', lambda _, token: token[1:-1]),
            (r"'[^']*'", lambda _, token: token[1:-1]),
            (r"[\w.]+(?==)", lambda _, token: token.strip()),
            (r"(?<==)[^,]+", lambda _, token: token.strip()),
            (r"\s*[=,]\s*", lambda *_: None),
        ]
    )

    def convert(
        self,
        value: str | Mapping,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> Mapping:
        """
        Converts a string value to a mapping.

        This method takes a value and converts it to a mapping.
        If the value is a string, it is scanned and split into tokens.
        If the number of tokens is even, the tokens are paired up and
        converted into a dictionary.
        If the value is already a mapping and there are no extra tokens,
        it is returned as is. Otherwise, an error is raised.

        Args:
            value (str | Mapping): The value to be converted.
            param (click.Parameter | None): The click parameter
                associated with the value.
            ctx (click.Context | None): The click context associated with the value.

        Returns:
            Mapping | None: The converted mapping value.

        Raises:
            ValueError: Raised when the value is not a valid comma-separated mapping.

        Example:
            ```python
            converter = Converter()
            value = "a=1,b=2"
            result = converter.convert(value, None, None)
            print(result)  # {'a': '1', 'b': '2'}
            ```
        """

        _extra = None
        if not value:
            return data._dict()
        elif isinstance(value, str):
            _tokens, _extra = self.scanner.scan(value)
            if len(_tokens) % 2 == 0:
                value = data._dict(zip(_tokens[::2], _tokens[1::2]))

        if isinstance(value, Mapping) and not _extra:
            return data._dict(value)

        else:
            self.fail("Expected a comma separated mapping (a=b,x=y)", param, ctx)


class TypedArray(click.ParamType):
    """
    A custom Click parameter type for representing typed arrays.

    Args:
        items (Literal["string", "number", "integer", "path"] | None):
            The type of items in the array. Defaults to "string".

    Raises:
        ValueError: If the provided items type is invalid.

    Returns:
        list: The converted list of values.

    Examples:
        >>> @click.command()
        ... @click.option("--values", type=TypedArray(items="number"))
        ... def process_values(values):
        ...     for value in values:
        ...         print(value)
        ...
        >>> process_values(["1", "2", "3"])
        1
        2
        3
    """

    name = "array"
    items: ITEMS_TYPES | None = None

    def __init__(self, items: ITEMS_TYPES | None = None) -> None:
        if items not in [*get_args(ITEMS_TYPES), None]:
            raise ValueError(f"Invalid type: {items}")

        self.items = items or "string"

    def convert(
        self,
        value: list,
        param: click.Parameter,
        ctx: click.Context,
    ) -> list:
        """
        Converts a list of values using the specified item type.

        Args:
            value (list): The list of values to convert.
            param (click.Parameter): The Click parameter associated with the conversion.
            ctx (click.Context): The Click context.

        Returns:
            list: The converted list of values.

        Raises:
            Exception: If an error occurs during the conversion.

        Examples:
            >>> items = TypedArray(items="number")
            >>> items.convert(["1", "2", "3"], param, ctx)
            [1, 2, 3]
        """
        try:
            _type = _click_type(self.items)
            if isinstance(_type, click.ParamType):
                return [_type.convert(v, param, ctx) for v in value]
            else:
                return [_type(v) for v in value]
        except Exception as exc:  # pylint: disable=broad-except
            self.fail(str(exc), param, ctx)


class ParsedSize(click.ParamType):
    name: str = "size"

    def convert(
        self,
        value: str | int,
        param: click.Parameter | None,
        ctx: click.Context | None,
    ) -> int:
        """
        Converts a string value to an integer.

        Args:
            value (str): The value to be converted.
            param (click.Parameter | None): The click parameter
                associated with the value.
            ctx (click.Context | None): The click context associated with the value.

        Returns:
            int: The converted integer value.

        Raises:
            ValueError: Raised when the value is not a valid integer.

        Example:
            ```python
            converter = Converter()
            value = "1"
            result = converter.convert(value, None, None)
            print(result)
        """
        del param, ctx  # Unused
        return parse_size(str(value))


def _click_type(  # type: ignore[return]
    _type: SCHEMA_TYPES | None = None,
    enum: list | None = None,
    items: ITEMS_TYPES | None = None,
) -> Type | click.Path | click.Choice | StringMapping | TypedArray | ParsedSize:
    """
    Translate jsonschema type to Python type.

    Returns:
        type: The Python type corresponding to the property type.
    """
    match _type:
        case _ if enum:
            return click.Choice(enum)
        case "string":
            return str
        case "number":
            return float
        case "integer":
            return int
        case "boolean":
            return bool
        case "mapping":
            return StringMapping()
        case "array":
            return TypedArray(items)
        case "path":
            return click.Path(path_type=Path)
        case "size":
            return ParsedSize()
        case _:
            return str


@define(slots=False)
class Flag:
    """
    Represents a flag used for command-line options.

    Attributes:
        key (list[str] | None): The key associated with the flag.
        type (
            Literal[
                "string",
                "number",
                "integer",
                "boolean",
                "mapping",
                "array",
                "path",
            ] | None
        ): The type of the flag.
        description (str | None): The description of the flag.
        default (Any): The default value of the flag.
        enum (list[Any] | None): The list of allowed values for the flag.
        required (bool): Indicates if the node is required.
        secret (bool): Determines if the value is hidden in the help section.

    Properties:
        required: Determines if the flag is required.
        pytype: Returns the Python type corresponding to the flag type.
        flag: Returns the flag name.
        click_option: Returns the click.option decorator for the flag.
        ```
    """

    type: SCHEMA_TYPES | None = field(default=None)
    items: ITEMS_TYPES | None = field(default=None)
    _key: list[str] | None = field(default=None)
    description: str | None = field(default=None)
    default: Any = field(default=None)
    value: Any = field(default=None)
    enum: list[Any] | None = field(default=None)
    required: bool = field(default=False)
    secret: bool = field(default=False)

    @type.validator
    def _type(self, attribute: str, value: str | None) -> None:
        del attribute  # Unused

        if value not in [*get_args(SCHEMA_TYPES), None]:
            raise ValueError(f"Invalid type: {value}")

    @property
    def key(self) -> list[str]:
        """
        Retrieves the key.

        Returns:
            list[str]: The key.

        Raises:
            ValueError: Raised when the key is not set.
        """
        if not self._key:
            raise ValueError("Key not set")
        return self._key

    @key.setter
    def key(self, value: list[str]) -> None:
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise ValueError(f"Invalid key: {value}")

        self._key = value

    @property
    def click_type(
        self,
    ) -> Type | click.Path | click.Choice | StringMapping | TypedArray | ParsedSize:
        """
        Translate jsonschema type to Python type.

        Returns:
            type: The Python type corresponding to the property type.
        """
        return _click_type(self.type, self.enum, self.items)

    @property
    def flag(self) -> str:
        """
        Constructs the flag name from the key.

        Raises:
            ValueError: Raised when the key is None.

        Returns:
            str: The flag name.
        """
        return "_".join(self.key)

    @property
    def no_flag(self) -> str:
        """
        Constructs the no-flag name from the key.

        Raises:
            ValueError: Raised when the key is None.

        Returns:
            str: The flag name.
        """
        return f"{'_'.join(self.key[:-1])}_no_{self.key[-1]}"

    @property
    def click_option(self) -> Callable:
        """
        Construct a click.option decorator from a Flag

        Returns:
            Callable: A click.option decorator
        """
        return click.option(
            f"--{self.flag}/--{self.no_flag}"
            if self.type == "boolean"
            else f"--{self.flag}",
            type=self.click_type,
            default=(
                True
                if self.type == "boolean" and self.default is None
                else self.value or self.default
            ),
            required=self.required,
            help=self.description,
            show_default=not self.secret,
        )
