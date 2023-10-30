import re
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Type

import rich_click as click
from attrs import define, field


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
            (r"\w+(?==)", lambda _, token: token.strip()),
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
            self: The instance of the class.
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

        if not value:
            return {}
        elif isinstance(value, str):
            _tokens, _extra = self.scanner.scan(value)
            if len(_tokens) % 2 == 0:
                value = dict(zip(_tokens[::2], _tokens[1::2]))

        if isinstance(value, Mapping) and not _extra:
            return value
        else:
            self.fail("Expected a comma separated mapping (a=b,x=y)", param, ctx)


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

    type: Literal[
        "string",
        "number",
        "integer",
        "boolean",
        "mapping",
        "array",
        "path",
    ] | None = field(default=None)
    _key: list[str] | None = field(default=None)
    description: str | None = field(default=None)
    default: Any = field(default=None)
    enum: list[Any] | None = field(default=None)
    required: bool = field(default=False)
    secret: bool = field(default=False)

    @type.validator
    def _type(self, attribute: str, value: str | None) -> None:
        del attribute  # Unused

        if value not in [
            "string",
            "number",
            "integer",
            "boolean",
            "mapping",
            "array",
            "path",
            None,
        ]:
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
    def click_type(self) -> Type | click.Path | click.Choice | StringMapping:
        """
        Translate jsonschema type to Python type.

        Returns:
            type: The Python type corresponding to the property type.
        """
        match self.type:
            case _ if self.enum:
                _click_type = click.Choice(self.enum)
            case "string":
                _click_type = str
            case "number":
                _click_type = float
            case "integer":
                _click_type = int
            case "boolean":
                _click_type = bool
            case "mapping":
                _click_type = StringMapping()
            case "array":
                _click_type = list
            case "path":
                _click_type = click.Path(path_type=Path)
            case _:
                _click_type = str

        return _click_type

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
                else self.default
            ),
            required=self.required,
            help=self.description,
            show_default=not self.secret,
        )