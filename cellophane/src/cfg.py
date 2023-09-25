from copy import deepcopy
from functools import cached_property, partial, wraps
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    Mapping,
    MutableMapping,
    Sequence,
)

import re
import rich_click as click
from attrs import define, field
from jsonschema.exceptions import ValidationError
from jsonschema.protocols import Validator
from jsonschema.validators import Draft7Validator, extend
from ruamel.yaml import CommentedMap, YAML
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
    ) -> Mapping | None:
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
    key = field(type=tuple)
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
    required: bool = field(default=False)
    default: Any = field(default=None)
    secret: bool = field(default=False)

    @type.validator
    def _type(self, _, value: str | None):
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
    def pytype(self):
        match self.type:
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
            default=self.default,
            required=self.required,
            help=self.description,
            show_default=not self.secret,
        )


def _set_key(
    schema: MutableMapping,
    flags: dict[tuple, Flag],
    parent: tuple | None = None,
):
    if parent:
        schema["#key"] = parent

    for ikey, item in schema.items():
        if ikey == "properties":
            for pkey, prop in item.items():
                _key = (*(parent or []), pkey)
                if "properties" not in prop:
                    flags[_key] = Flag(
                        key=_key,
                        default=prop.get("default", None),
                        secret=prop.get("secret", False),
                        description=prop.get("description", None),
                        type=prop.get("type", None),
                    )
                prop = _set_key(prop, flags, _key)

    return schema


def _set_required(flags, required_keys, instance_keys):
    for key, flag in flags.items():
        _required_paths = required_keys | (instance_keys - {key})

        flag.required = key not in instance_keys and all(
            key[:i] in _required_paths for i in range(1, len(key) + 1)
        )


def _validator(fn: Callable):
    @wraps(fn)
    def inner(
        validator: Validator,
        property: Any,
        instance: MutableMapping,
        schema: Mapping,
        **kwargs,
    ):
        return fn(
            property,
            validator=validator,
            instance=instance,
            schema=schema,
            **kwargs,
        )

    return inner


@_validator
def _properties(
    properties: Mapping,
    *,
    validator: Validator,
    instance: MutableMapping,
    schema: Mapping,
    store: dict,
    flags: dict[tuple, Flag],
    **_,
):
    """Store the properties in the validator instance"""
    for property, subschema in properties.items():
        _key = (*schema.get("#key", []), property)
        if property in instance:
            for k in range(1, len(_key) + 1):
                store["present"] |= {_key[:k]}
            if _key in flags:
                flags[_key].default = instance[property]
        else:
            instance[property] = {}

        yield from validator.descend(
            instance[property],
            subschema,
            path=property,
            schema_path=property,
        )


@_validator
def _required(
    required: list[str],
    schema: Mapping,
    store: dict,
    **_,
):
    for prop in required:
        store["required"] |= {(*schema.get("#key", []), prop)}


@_validator
def _root(
    subschema: MutableMapping,
    instance: MutableMapping,
    validator: Validator,
    flags: dict[tuple, Flag],
    **_,
):
    _subschema = _set_key(deepcopy(subschema), flags)
    _instance = deepcopy(instance)
    _store: dict[str, set] = {"required": set(), "present": set()}

    _validator = extend(
        validator.__class__,
        validators={
            "type": None,
            "enum": None,
            "properties": partial(_properties, store=_store, flags=flags),
            "required": partial(_required, store=_store, flags=flags),
        },
    )(schema=_subschema)

    yield from _validator.descend(_instance, _subschema)
    _set_required(flags, _store["required"], _store["present"])


def _is_object_or_container(_, instance):
    return any(
        (
            Draft7Validator.TYPE_CHECKER.is_type(instance, "object"),
            isinstance(instance, data.Container),
        )
    )


def _is_array(_, instance):
    return (
        Draft7Validator.TYPE_CHECKER.is_type(instance, "array")
        or isinstance(instance, Sequence)
        and not isinstance(instance, str | bytes)
    )


def _is_path(_, instance):
    return isinstance(instance, Path | click.Path | None)


# def _required_not_none(validator, required, instance, schema):
#     for property in required:
#         if (
#             validator.is_type(instance, "object")
#             and instance.get(property, None) is None
#         ):
#             yield ValidationError(f"{property!r} is a required property")


CellophaneValidator: Validator = extend(
    Draft7Validator,
    # validators=Draft7Validator.VALIDATORS | {"required": _required_not_none},
    type_checker=Draft7Validator.TYPE_CHECKER.redefine_many(
        {
            "object": _is_object_or_container,
            "mapping": _is_object_or_container,
            "array": _is_array,
            "path": _is_path,
        }
    ),
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
    def flags(
        self,
    ) -> list[Flag]:
        """Get flags from schema"""
        _flags: dict[tuple, Flag] = {}
        _validator = extend(
            Draft7Validator,
            validators={"#ROOT": partial(_root, flags=_flags)},
        )({"#ROOT": deepcopy(self.as_dict)})

        _validator.validate({})

        return [*_flags.values()]

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

    def validate(self, config: data.Container):
        """Iterate over validation errors"""
        _validator: Draft7Validator = CellophaneValidator(self.as_dict)
        _validator.validate(config)

    def iter_errors(
        self,
        config: data.Container,
        validator: type[Draft7Validator] = CellophaneValidator,
    ) -> Iterator[ValidationError]:
        """Iterate over validation errors"""
        _validator = validator({**self.data})
        return _validator.iter_errors(config)


@define(slots=False, kw_only=True, init=False)
class Config(data.Container):
    """Configuration file"""

    schema: Schema

    def __init__(
        self,
        schema: Schema,
        validate: bool = True,
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

        if validate:
            schema.validate(_data_container)

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
        """Get flags from schema that depend on configuration"""
        _flags: dict[tuple, Flag] = {}
        _validator = extend(
            Draft7Validator,
            validators={"#ROOT": partial(_root, flags=_flags)},
        )({"#ROOT": deepcopy(self.schema.as_dict)})
        _validator.validate(deepcopy(self.as_dict))

        return [*_flags.values()]
