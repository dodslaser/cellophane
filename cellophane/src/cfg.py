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
    parent_present: bool = field(default=False)
    parent_required: bool = field(default=False)
    node_required: bool = field(default=False)
    key: list[str] | None = field(default=None)
    type: Literal[
        "string",
        "number",
        "integer",
        "boolean",
        "mapping",
        "array",
        "path",
    ] | None = field(default=None)
    description: str | None = field(default=None)
    default: Any = field(default=None)
    enum: list[Any] | None = field(default=None)
    secret: bool = field(default=False)

    @type.validator
    def _type(self, _, value: str | None) -> None:
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
    def required(self) -> bool:
        """Determines if the flag is required"""
        return self.node_required and (self.parent_present or self.parent_required)

    @property
    def pytype(self):
        match self.type:
            case _ if self.enum:
                return click.Choice(self.enum)
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
                return list
            case "path":
                return click.Path(path_type=Path)

    @property
    def flag(self) -> str:
        return "_".join(self.key)

    @property
    def click_option(self) -> Callable:
        return click.option(
            f"--{self.flag}/--no-{self.flag}"
            if self.type == "boolean"
            else f"--{self.flag}",
            type=self.pytype,
            default=(
                True
                if self.type == "boolean"
                and self.default is None
                else self.default
            ),
            required=self.required,
            help=self.description,
            show_default=not self.secret,
        )

class Root:  #pragma: no cover
    """Sentinel to mark the root of a config instance"""
    def __repr__(self):
        return "ROOT"

ROOT = Root()

def _properties(validator, properties, instance, _):
    """Convert properties to flags"""

    # Instance will only be {} if no property of parent is present
    # Validator will only be ROOT_VALIDATOR if we are at the root (not evolved)
    _parent_present = instance != {} or instance.get(ROOT, False)
    for prop, subschema in properties.items():
        match subschema:
            case {"type": "object"}:
                instance[prop] = instance.get(prop, {})
            case _:
                _flag = Flag(
                    parent_present=_parent_present,
                    default=instance.get(prop, None)
                    or subschema.get("default", None),
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
    for prop in required:
        match instance[prop]:
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
    }
)
RequiredValidator = extend(
    BaseValidator,
    validators={
        "required": _required,
    }
)

@define(slots=False, init=False, frozen=True)
class Schema(data.Container):
    """Schema for validating configuration files"""

    @classmethod
    def from_file(cls, path: Path | Sequence[Path]):
        """Load schema from file"""
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
        def wrapper(func):
            for flag in self.flags:
                func = flag.click_option(func)
            return func

        return wrapper

    @cached_property
    def flags(self) -> list[Flag]:
        """Get flags from schema"""
        _flags: list[Flag] = []
        _data = {ROOT: True}
        RootValidator(self.as_dict).validate(_data)
        RequiredValidator(self.as_dict).validate(_data)
        _data.pop(ROOT)

        _container = data.Container(_data)
        for key in util.map_nested_keys(_data):
            _container[key].key = key
            _flags.append(_container[key])
        return _flags

    @cached_property
    def example_config(self) -> str:
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
    """Configuration file"""

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
        _flags: list[Flag] = []
        _data = deepcopy(self.as_dict) | {ROOT: True}
        RootValidator(self.schema.as_dict).validate(_data)
        RequiredValidator(self.as_dict).validate(_data)
        _data.pop(ROOT)

        _container = data.Container(_data)
        for key in util.map_nested_keys(_data):
            _container[key].key = key
            _flags.append(_container[key])
        return _flags
