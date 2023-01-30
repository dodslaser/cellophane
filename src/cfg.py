"""Configuration file handling"""

from copy import deepcopy
from functools import reduce
from pathlib import Path
from typing import Iterable, Optional, Type

import rich_click as click
from jsonschema import Draft7Validator, Validator, validators
from yaml import safe_load

from . import data, util


def _set_options(cls: Type[Validator]) -> Type[Validator]:
    """Set default values when validating"""
    validate_properties = cls.VALIDATORS["properties"]

    def _is_object_or_container(_, instance):
        return cls.TYPE_CHECKER.is_type(instance, "object") or isinstance(
            instance, data.Container
        )

    def _is_array(_, instance):
        return cls.TYPE_CHECKER.is_type(instance, "array") or isinstance(
            instance, Iterable
        )

    def _is_path(_, instance):
        return isinstance(instance, Optional[Path | click.Path])

    def _set(cls, properties, instance, schema):
        for prop, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(prop, subschema["default"])
            if subschema.get("type", None) == "path":
                instance[prop] = Path(instance[prop]) if prop in instance else None

        for error in validate_properties(cls, properties, instance, schema):
            yield error

    type_checker = cls.TYPE_CHECKER.redefine_many(
        {
            "object": _is_object_or_container,
            "array": _is_array,
            "path": _is_path,
        }
    )
    return validators.extend(cls, {"properties": _set}, type_checker=type_checker)


def _get_options(cls: Type[Validator]) -> Type[Validator]:
    """Parse options from schema and ignore validation errors"""
    validate_properties = cls.VALIDATORS["properties"]

    def func(cls, properties, instance, schema):
        for prop, subschema in properties.items():
            if "properties" in subschema:
                instance[prop] = {}
            else:
                instance[prop] = []
                match subschema:
                    case {"default": default}:
                        instance[prop].append(default)
                    case _:
                        instance[prop].append(None)

                match subschema:
                    case {"description": description}:
                        instance[prop].append(description)
                    case _:
                        instance[prop].append("")

                match subschema:
                    case {"enum": enum}:
                        instance[prop].append(click.Choice(enum, case_sensitive=False))
                    case {"type": "boolean"}:
                        instance[prop].append(bool)
                    case {"type": "path"}:
                        instance[prop].append(click.Path())
                    case {"type": "string"}:
                        instance[prop].append(str)
                    case {"type": "integer"}:
                        instance[prop].append(int)
                    case {"type": "number"}:
                        instance[prop].append(float)
                    case {"type": "array"}:
                        instance[prop].append(list)
                    case _:
                        instance[prop].append(None)

        for error in validate_properties(cls, properties, instance, schema):
            yield error

    return validators.extend(
        cls,
        {k: None for k in cls.VALIDATORS}
        | {
            "properties": func,
            "if": None,
            "then": None,
            "else": None,
        },
    )


class Schema(data.Container):
    """Schema for validating configuration files"""

    @classmethod
    def from_file(cls, path: Path | Iterable[Path]):
        """Load schema from file"""
        path = [path] if isinstance(path, Path) else path
        schema: dict = {}
        for file in path:
            with open(file, "r", encoding="utf-8") as handle:
                schema |= safe_load(handle) or {}
        cls(schema)

    @property
    def properties(self) -> dict:
        """Get properties from schema"""
        _properties: dict = {}
        _get_options(Draft7Validator)({**self.data}).validate(_properties)
        return _properties

    @property
    def key_map(self) -> list[list[str]]:
        """Get key map from schema"""
        return util.map_nested_keys(self.properties)

    @property
    def flags(self):
        """Get flags from schema"""
        for key in self.key_map:  # pylint: disable=not-an-iterable
            flag = "_".join(key)
            default, description, _type = reduce(
                lambda x, y: x[y], key, self.properties
            )
            yield flag, key, default, description, _type

    def validate(self, config: data.Container) -> data.Container:
        """Validate configuration"""
        _config = deepcopy(config)
        _set_options(Draft7Validator)({**self.data}).validate(_config)
        return _config


class Config(data.Container):
    """Configuration file"""

    def __init__(self, path: Optional[Path | click.Path], schema: Schema, **kwargs):

        if path is not None:
            with open(str(path), "r", encoding="utf-8") as handle:
                _data = data.Container(safe_load(handle))
        else:
            _data = data.Container({})

        for flag, key, *_ in schema.flags:
            if flag not in _data and kwargs[flag] is not None:
                _data[key] = kwargs[flag]

        super().__init__(_data)


def set_defaults(ctx: click.Context, config_path: Path | click.Path, schema: Schema):
    """Set click defaults from schema and configuration file"""
    with open(str(config_path), "r", encoding="utf-8") as handle:
        config = data.Container(safe_load(handle))

    ctx.default_map = {
        flag: config[key] if key in config else default
        for flag, key, default, _, _ in schema.flags
    }
