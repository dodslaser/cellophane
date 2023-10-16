"""Configuration file handling and CLI generation"""

import re
from copy import deepcopy
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Sequence

import rich_click as click
from attrs import define, field
from jsonschema.validators import Draft7Validator, extend
from ruamel.yaml import YAML, CommentedMap
from ruamel.yaml.compat import StringIO

from . import data, util

_YAML = YAML()


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
        _extra = []
        if isinstance(value, str):
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
        parent_present (bool): Indicates if the parent is present.
        parent_required (bool): Indicates if the parent is required.
        node_required (bool): Indicates if the node is required.
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
        secret (bool): Determines if the value is hidden in the help section.

    Properties:
        required: Determines if the flag is required.
        pytype: Returns the Python type corresponding to the flag type.
        flag: Returns the flag name.
        click_option: Returns the click.option decorator for the flag.
        ```
    """

    parent_present: bool = field(default=False)
    parent_required: bool = field(default=False)
    node_required: bool = field(default=False)
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
    secret: bool = field(default=False)

    @type.validator
    def _type(self, _, value: str | None) -> None:
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
        if not isinstance(value, list) or not all(
            isinstance(v, str) for v in value
        ):
            raise ValueError(f"Invalid key: {value}")

        self._key = value

    @property
    def required(self) -> bool:
        """
        Returns a boolean indicating whether the property is required.

        A property is required if the node is required and the parent
        is present OR required.

        Returns:
            bool: True if the property is required, False otherwise.
        """

        return self.node_required and (self.parent_present or self.parent_required)

    @property
    def pytype(self):
        """
        Translate jsonschema type to Python type.

        Returns:
            type: The Python type corresponding to the property type.
        """
        _click_type: Type | click.Path | click.Choice | StringMapping
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

        return _click_type

    @property
    def flag(self) -> str:
        """
        Constructs the flag name from the key.

        The flag name is constructed by joining the key with underscores.

        Raises:
            ValueError: Raised when the key is None.

        Returns:
            str: The flag name.
        """
        return "_".join(self.key)

    @property
    def click_option(self) -> Callable:
        """
        Construct a click.option decorator from a Flag

        Returns:
            Callable: A click.option decorator
        """
        return click.option(
            f"--{self.flag}/--no-{self.flag}"
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


class _Root:  # pragma: no cover
    """Sentinel to mark the root of a config instance"""


def _properties(
    """Convert properties to flags"""
    del schema  # Unused
    # Instance will only be {} if no property of parent is present
    # Validator will only be ROOT_VALIDATOR if we are at the root (not evolved)
    _parent_present = instance != {} or instance.get(_Root, False)
    for prop, subschema in properties.items():
        match subschema:
            case {"type": "object"}:
                instance[prop] = instance.get(prop, {})
            case _:
                _flag = Flag(
                    parent_present=_parent_present,
                    default=instance.get(prop, None) or subschema.get("default", None),
                    type=subschema.get("type", None),
                    enum=subschema.get("enum", None),
                    description=subschema.get("description", None),
                    secret=subschema.get("secret", False),
                )

                instance[prop] = _flag

        yield from validator.descend(
            instance[prop],
            subschema,
            path=prop,
            schema_path=prop,
        )


def _required(validator, required, instance, _):
    """Mark required flags as required"""

    del schema  # Unused

    for prop in required:
        match instance.get(prop):
            case Flag() as flag:
                flag.node_required = True
            case parent if validator.is_type(parent, "object"):
                for subprop in parent.values():
                    if isinstance(subprop, Flag):
                        subprop.parent_required = True


BaseValidator = extend(
    Draft7Validator,
    validators={
        validator: None
        for validator in Draft7Validator.VALIDATORS
        if validator not in ["properties", "if"]
    },
)
RootValidator = extend(
    BaseValidator,
    validators={
        "properties": _properties,
        "if": None,
    },
)
RequiredValidator = extend(
    BaseValidator,
    validators={
        "required": _required,
    },
)


@define(slots=False, init=False, frozen=True)
class Schema(data.Container):
    """
    Represents a schema for configuration data.

    Class Methods:
        from_file(cls, path: Path | Sequence[Path]) -> Schema:
            Loads the schema from a file or a sequence of files.

    Properties:
        add_options: Decorator that adds click options to a function.
        flags: List of flags extracted from the schema.
        example_config: Example configuration generated from the schema.

    Example:
        ```python
        schema = Schema()

        # Loading schema from a file
        path = Path("schema.yaml")
        loaded_schema = Schema.from_file(path)

        # Adding click options to a function
        @schema.add_options
        @click.command()
        def cli(**kwargs):
            ...

        # Getting the list of flags
        flags = schema.flags

        # Generating an example configuration
        example_config = schema.example_config
        ```
    """

    @classmethod
    def from_file(cls, path: Path | Sequence[Path]):
        """Loads the schema from a file or a sequence of files"""
        if isinstance(path, Path):
            with open(path, "r", encoding="utf-8") as handle:
                return cls(_YAML.load(handle) or {})
        elif isinstance(path, Sequence):
            schema: dict = {}
            for p in path:
                schema = util.merge_mappings(schema, cls.from_file(p))
            return cls(schema)

    @property
    def add_options(self) -> Callable:
        """Decorator that adds click options to a function"""
        def wrapper(func):
            for flag in self.flags:
                func = flag.click_option(func)
            return func

        return wrapper

    @cached_property
    def flags(self) -> list[Flag]:
        """Get flags from schema"""
        _flags: list[Flag] = []
        _data = {_Root: True}
        RootValidator(self.as_dict).validate(_data)
        RequiredValidator(self.as_dict).validate(_data)
        _data.pop(_Root)

        _container = data.Container(_data)
        for key in util.map_nested_keys(_data):
            _container[key].key = key
            _flags.append(_container[key])
        return _flags

    @cached_property
    def example_config(self) -> str:
        """Generate an example configuration from the schema"""
        _map = CommentedMap()
        for flag in self.flags:
            if flag.description:
                _comment = f"{flag.description} ({flag.type})"
            else:
                _comment = f"({flag.type})"

            _node = _map
            for k in flag.key[:-1]:
                _node.setdefault(k, CommentedMap())
                _node = _node[k]

            _node.insert(1, flag.key[-1], flag.default, comment=_comment)

        with StringIO() as handle:
            _YAML.representer.add_representer(
                type(None),
                lambda dumper, *_: dumper.represent_scalar(
                    "tag:yaml.org,2002:null", "~"
                ),
            )
            _YAML.dump(_map, handle)
            return handle.getvalue()


@define(slots=False, kw_only=True, init=False)
class Config(data.Container):
    """
    Represents a configuration object based on a schema.

    Attributes:
        schema (Schema): The schema associated with the configuration.

    Methods:
        __init__(
            schema: Schema,
            allow_empty: bool = False,
            _data: dict | None = None,
            **kwargs,
        ):
            Initializes the Config object with the given schema and data.

        from_file(
            path: str,
            schema: Schema,
            validate: bool = True,
            allow_empty: bool = False,
            **kwargs,
        ):
            Creates a Config object from a file.

        as_dict:
            Returns the configuration as a dictionary.

        flags:
            Returns the flags from the schema that depend on the configuration.

    Args:
        schema (Schema): The schema associated with the configuration.
        allow_empty (bool, optional): Allow empty configuration. Defaults to False.
        _data (dict | None, optional): The data for the configuration. Defaults to None.
        **kwargs: Additional keyword arguments for the configuration.

    Example:
        ```python
        schema = Schema()
        config = Config(schema)

        # Creating a configuration from a file
        path = "config.yaml"
        config = Config.from_file(path, schema)

        # Accessing the configuration as a dictionary
        data = config.as_dict

        # Getting the flags from the schema with config values used for defaults
        flags = config.flags
        ```
    """

    schema: Schema

    def __init__(
        self,
        schema: Schema,
        allow_empty: bool = False,
        _data: dict | None = None,
        **kwargs,
    ):
        if not _data and not kwargs and not allow_empty:
            raise ValueError("Empty configuration")

        self.schema = schema

        _data_container = data.Container(_data or {})
        for flag in [f for f in schema.flags if f.key not in _data_container]:
            if flag.flag in kwargs:
                _data_container[flag.key] = kwargs[flag.flag]
            elif flag.default:
                _data_container[flag.key] = flag.default

        self.data = _data_container.data

    @property
    def as_dict(self):
        """
        Convert the configuration to a dictionary.

        Returns:
            dict: The configuration as a dictionary.
        """
        return {
            k: v.as_dict if isinstance(v, data.Container) else v
            for k, v in self.data.items()
            if k != "schema"
        }

    @classmethod
    def from_file(
        cls,
        path: str,
        schema: Schema,
        validate: bool = True,
        allow_empty: bool = False,
        **kwargs,
    ):
        """Creates a Config object from a file"""
        _path = Path(path)
        _data = _YAML.load(_path.read_bytes())

        return cls(
            schema=schema,
            validate=validate,
            allow_empty=allow_empty,
            _data=_data,
            **kwargs,
        )

    @property
    def flags(self) -> list[Flag]:
        """Get flags from schema with config values used for defaults"""
        _flags: list[Flag] = []
        _data = deepcopy(self.as_dict) | {_Root: True}
        RootValidator(self.schema.as_dict).validate(_data)
        RequiredValidator(self.schema.as_dict).validate(_data)
        _data.pop(_Root)

        _container = data.Container(_data)
        for key in util.map_nested_keys(_data):
            _container[key].key = key
            _flags.append(_container[key])
        return _flags
