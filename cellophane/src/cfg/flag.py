"""Flag class for command-line options."""

from functools import partial
from typing import Any, Callable, SupportsFloat, Type, get_args

import rich_click as click
from attrs import define, field, setters

from .click_ import (
    FORMATS,
    ITEMS_TYPES,
    SCHEMA_TYPES,
    FormattedString,
    InvertibleParamType,
    ParsedSize,
    StringMapping,
    TypedArray,
    click_type,
)


def _convert_float(value: SupportsFloat | None) -> float | None:
    return float(value) if value is not None else None


@define(slots=False)
class Flag:
    """Represents a flag used for command-line options.

    Attributes
    ----------
        key (list[str] | None): The key associated with the flag.
        type SCHEMA_TYPES: The JSONSchema type of the flag.
        items ITEMS_TYPES: The JSONSchema items type of the flag.
        format_ FORMATS: The JSONSchema format of the flag.
        minimum (int | None): The minimum value of the flag.
        maximum (int | None): The maximum value of the flag.
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

    key: tuple[str, ...] = field(converter=tuple, on_setattr=setters.convert)
    type: SCHEMA_TYPES | None = field(default=None)
    items_type: ITEMS_TYPES | None = field(default=None)
    items_format: FORMATS | None = field(default=None)
    items_min: int | None = field(
        default=None,
        converter=_convert_float,
        on_setattr=setters.convert,
    )
    items_max: int | None = field(
        default=None,
        converter=_convert_float,
        on_setattr=setters.convert,
    )
    min: int | None = field(
        default=None,
        converter=_convert_float,
        on_setattr=setters.convert,
    )
    max: int | None = field(
        default=None,
        converter=_convert_float,
        on_setattr=setters.convert,
    )
    format: FORMATS | None = field(default=None)
    pattern: str | None = field(default=None)
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

    def convert(
        self,
        value: Any,
        ctx: click.Context | None = None,
        param: click.Parameter | None = None,
    ) -> Any:
        """Converts the value to the flag type.

        Args:
        ----
            value (Any): The value to be converted.
            ctx (click.Context | None): The click context.
            param (click.Parameter | None): The click parameter.

        Returns:
        -------
            Any: The converted value.

        """
        _converter: Callable
        if isinstance(self.click_type, click.ParamType):
            _converter = partial(self.click_type.convert, ctx=ctx, param=param)
        else:
            _converter = self.click_type

        return _converter(value)

    @property
    def click_type(
        self,
    ) -> (
        Type
        | click.Path
        | click.Choice
        | click.IntRange
        | click.FloatRange
        | StringMapping
        | TypedArray
        | ParsedSize
        | FormattedString
    ):
        """Translate jsonschema type to Python type.

        Returns
        -------
            type: The Python type corresponding to the property type.

        """
        return click_type(
            type_=self.type,
            format_=self.format,
            pattern=self.pattern,
            min_=self.min,
            max_=self.max,
            enum=self.enum,
            items_type=self.items_type,
            items_format=self.items_format,
            items_min=self.items_min,
            items_max=self.items_max,
        )

    @property
    def flag(self) -> str:
        """Constructs the flag name from the key.

        Raises
        ------
            ValueError: Raised when the key is None.

        Returns
        -------
            str: The flag name.

        """
        return "_".join(self.key)

    @property
    def no_flag(self) -> str:
        """Constructs the no-flag name from the key.

        Raises
        ------
            ValueError: Raised when the key is None.

        Returns
        -------
            str: The flag name.

        """
        return "_".join([*self.key[:-1], "no", self.key[-1]])

    @property
    def click_option(self) -> Callable:
        """Construct a click.option decorator from a Flag

        Returns
        -------
            Callable: A click.option decorator

        """
        return click.option(
            (
                f"--{self.flag}/--{self.no_flag}"
                if self.type == "boolean"
                else f"--{self.flag}"
            ),
            type=self.click_type,
            multiple=self.items_type == "array",
            default=(
                True
                if self.type == "boolean" and self.default is None
                else self.value or self.default
            ),
            required=self.required,
            help=self.description,
            show_default=(
                False
                if self.secret
                else (
                    self.click_type.invert(default)
                    if (default := self.value or self.default)
                    and isinstance(self.click_type, InvertibleParamType)
                    else str(default)
                )
            ),
        )
