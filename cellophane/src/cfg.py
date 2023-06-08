"""Configuration file handling"""

from attrs import define
from copy import deepcopy, copy
from functools import reduce, cached_property, partial
from pathlib import Path
from typing import Sequence, Optional, Iterator, Mapping, Any

import rich_click as click
from jsonschema import Draft7Validator, validators, ValidationError
from yaml import safe_load

from . import data, util


def _is_object_or_container(_, instance):
    return Draft7Validator.TYPE_CHECKER.is_type(instance, "object") or isinstance(
        instance, data.Container
    )


def _is_array(_, instance):
    return Draft7Validator.TYPE_CHECKER.is_type(instance, "array") or isinstance(
        instance, Sequence
    )


def _is_path(_, instance):
    return isinstance(instance, Optional[Path | click.Path])


def _is_mapping(_, instance):
    return isinstance(instance, Sequence) and all(
        isinstance(i, Mapping) for i in instance
    )


def _set_type(cls, properties, instance, schema):
    for property, subschema in properties.items():
        if "type" in subschema and property in instance:
            match subschema["type"] if "type" in subschema else None:
                case "boolean":
                    instance[property] = bool(instance[property])
                case "path":
                    instance[property] = Path(instance[property])
                case "string":
                    instance[property] = str(instance[property])
                case "integer":
                    instance[property] = int(instance[property])
                case "number":
                    instance[property] = float(instance[property])
                case "array":
                    instance[property] = list(instance[property])
                case "mapping":
                    instance[property] = list(dict(d) for d in instance[property])
                case _:
                    pass

    for error in Draft7Validator.VALIDATORS["properties"](
        cls,
        properties,
        instance,
        schema,
    ):
        yield error


def _get_schema_properties(cls, properties, instance, schema, flag_mapping):
    for key, prop in schema["properties"].items():
        _parent = prop.get("_parent", [])
        for subprop in prop.get("properties", {}).values():
            subprop["_parent"] = [*_parent, key]

    for prop, subschema in properties.items():

        _key = [*subschema.get("_parent", []), prop]

        if "properties" in subschema:
            if prop not in instance:
                instance[prop] = {}
                flag_mapping[*_key] = {}
            continue

        flag_mapping[*_key] = []

        match subschema:
            case {"_required": required}:
                flag_mapping[*_key].append(required)
            case _:
                flag_mapping[*_key].append(False)

        match subschema:
            case {"default": default}:
                flag_mapping[*_key].append(instance.get(prop, default))
            case _:
                flag_mapping[*_key].append(instance.get(prop, None))

        match subschema:
            case {"description": description}:
                flag_mapping[*_key].append(description)
            case _:
                flag_mapping[*_key].append("")

        match subschema:
            case {"secret": True}:
                flag_mapping[*_key].append(True)
            case _:
                flag_mapping[*_key].append(False)

        match subschema:
            case {"enum": enum}:
                flag_mapping[*_key].append(click.Choice(enum, case_sensitive=False))
            case {"type": "boolean"}:
                flag_mapping[*_key].append(bool)
            case {"type": "skip"}:
                flag_mapping[*_key].append(bool)
            case {"type": "path"}:
                flag_mapping[*_key].append(click.Path())
            case {"type": "string"}:
                flag_mapping[*_key].append(str)
            case {"type": "integer"}:
                flag_mapping[*_key].append(int)
            case {"type": "number"}:
                flag_mapping[*_key].append(float)
            case {"type": "array"}:
                flag_mapping[*_key].append(list)
            case {"type": "mapping"}:
                flag_mapping[*_key].append(dict)
            case _:
                flag_mapping[*_key].append(None)

        flag_mapping[*_key].append(subschema.get("type", None))

    for error in Draft7Validator.VALIDATORS["properties"](
        cls,
        properties,
        instance,
        schema,
    ):
        yield error


def _get_schema_required(cls, properties: list[str], instance: dict, schema: dict):
    """Get required properties from schema"""
    if any(
        instance.get(k, False)
        for k, p in schema["properties"].items()
        if p.type == "skip"
    ):
        for key, prop in schema["properties"].items():
            prop["_skip"] = True

    for key in properties:
        match prop := schema["properties"].get(key, {}):
            case {"_skip": True} | {"default": _}:
                prop["_required"] = False
            case {"_parent_required": True} | {"_parent_present": True}:
                prop["_required"] = key not in instance
            case _ if "_parent_required" not in prop:
                prop["_required"] = key not in instance
            case _:
                prop["_required"] = False

    for key, prop in schema["properties"].items():
        if "properties" in prop:
            # Add empty requirements to enforce validation
            prop.setdefault("required", [])
            # Set _parent_required if prop has children
            for subprop in prop["properties"].values():
                subprop["_parent_required"] = prop.get("_required", False)
                subprop["_parent_present"] = key in instance
                subprop["_skip"] = prop.get("_skip", False)


BaseValidator = validators.extend(
    Draft7Validator,
    type_checker=Draft7Validator.TYPE_CHECKER.redefine_many(
        {
            "object": _is_object_or_container,
            "array": _is_array,
            "path": _is_path,
            "mapping": _is_mapping,
        }
    ),
)

CellophaneValidator = validators.extend(
    BaseValidator,
    validators={"properties": _set_type},
)


def parse_mapping(string_mapping: dict | Sequence[str] | str) -> list[dict[str, Any]]:
    match string_mapping:
        case dict(mapping):
            return [mapping]
        case [*strings]:
            return [m for s in strings for m in parse_mapping(s)]
        case str(string):
            _mapping: dict[str, Any] = {}
            for kv in string.split():
                for k, v in [kv.split("=")]:
                    identifier = k.strip("{}")
                    if not identifier.isidentifier():
                        raise ValueError(f"{identifier} is not a valid identifier")
                    else:
                        _mapping[identifier] = v
            return [_mapping]
        case _:
            raise ValueError("format must be 'key=value ...'")


@define(slots=False, init=False, frozen=True)
class Schema(data.Container):
    """Schema for validating configuration files"""

    @classmethod
    def from_file(cls, path: Path | Sequence[Path]):
        """Load schema from file"""
        if isinstance(path, Path):
            with open(path, "r", encoding="utf-8") as handle:
                return cls(safe_load(handle) or {})
        elif isinstance(path, Sequence):
            schema: dict = {}
            for file in path:
                with open(file, "r", encoding="utf-8") as handle:
                    schema = util.merge_mappings(
                        safe_load(handle) or {},
                        deepcopy(schema),
                    )
            return cls(schema)

    @cached_property
    def schema_properties(self) -> dict:
        """Get properties from schema"""
        _flag_mapping = data.Container()
        _validator = validators.extend(
            BaseValidator,
            {k: None for k in Draft7Validator.VALIDATORS}
            | {
                "properties": partial(
                    _get_schema_properties,
                    flag_mapping=_flag_mapping,
                ),
                "required": _get_schema_required,
            },
        )

        _validator({**self.data}).validate({})
        return _flag_mapping.as_dict

    @cached_property
    def key_map(self) -> list[list[str]]:
        """Get key map from schema"""
        return util.map_nested_keys(self.schema_properties)

    @property
    def flags(
        self,
    ) -> Iterator[tuple[str, list[str], bool, Any, str, bool, type, str]]:
        """Get flags from schema"""
        for key in self.key_map:  # pylint: disable=not-an-iterable
            flag = "_".join(key)
            required, default, description, secret, _type, json_type = reduce(
                lambda x, y: x[y], key, self.schema_properties
            )
            yield flag, key, required, default, description, secret, _type, json_type

    def example_config(self, extra: str) -> str:
        """Create an example configuration file"""
        config: list[str] = []
        visited = []
        for _, key, required, default, description, _, _, json_type in self.flags:
            # Print parent keys
            cur: list[str] = []
            config.append("")
            for k in key[:-1]:
                indent = "  " * len(cur)
                cur.append(k)
                if cur not in visited:
                    config.append(f"{indent}{k}:")
                    visited.append(copy(cur))

            # Print current key, value, and comment
            comment = (
                (f"{description} " if description else "")
                + f"({json_type}"
                + (" REQUIRED)" if required else ")")
            )

            indent = "  " * (len(key) - 1)

            config.append(f"{indent}# {comment}")
            match json_type:
                case "array":
                    config.append(f"{indent}{key[-1]}:")
                    example = (
                        f"{indent}# - value\n{indent}# - ..."
                        or f"{indent}- " f"\n{indent}- ".join(default.split(" "))
                    )
                    config.append(example)
                case "mapping":
                    config.append(f"{indent}{key[-1]}")
                    example = f"{indent}# - key=value\n{indent}#   ...=..."
                    config.append(example)
                case "boolean":
                    example = f" {default}".lower() if default else ""
                    config.append(f"{indent}{key[-1]}:{example}")
                case "string":
                    lines = (default or "").split("\n")
                    example = ("\n" + indent + "  ").join(f"{li}" for li in lines)
                    if len(lines) > 1:
                        config.append(f"{indent}{key[-1]}: |")
                        config.append(f"{indent}  {example}")
                    elif example:
                        config.append(f'{indent}{key[-1]}: "{example}"')
                    else:
                        config.append(f"{indent}{key[-1]}:")
                case _:
                    example = f" {default}" if default else ""
                    config.append(f"{indent}{key[-1]}:{example}")

        config.append("\n" + extra)
        return "\n".join(config)

    def validate(self, config: data.Container):
        """Iterate over validation errors"""
        _validator = CellophaneValidator({**self.data})
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
        self.schema = schema
        if _data is None:
            _c_data = data.Container()
        else:
            _c_data = data.Container(_data)

        for flag, key, *_ in schema.flags:
            if flag not in _c_data:
                if (value := kwargs.get(flag, None)) is not None:
                    _c_data[key] = value
                elif allow_empty and not validate:
                    _c_data[key] = value

        if validate:
            schema.validate(_c_data)

        self.data = _c_data.data

    @classmethod
    def from_file(
        cls,
        path: Path,
        schema: Schema,
        validate: bool = True,
        allow_empty: bool = False,
        **kwargs,
    ):
        with open(path, "r", encoding="utf-8") as handle:
            _data = safe_load(handle)

        return cls(
            schema=schema,
            validate=validate,
            allow_empty=allow_empty,
            _data=_data,
            **kwargs,
        )

    @cached_property
    def properties(self) -> dict:
        """Get properties from schema that depend on configuration"""
        _flag_mapping = data.Container()
        _validator = validators.extend(
            BaseValidator,
            {k: None for k in Draft7Validator.VALIDATORS}
            | {
                "properties": partial(
                    _get_schema_properties,
                    flag_mapping=_flag_mapping,
                ),
                "required": _get_schema_required,
            },
        )

        _validator({**self.schema.data}).validate(deepcopy(self.data))
        return _flag_mapping.as_dict

    @cached_property
    def flags(self) -> Iterator[tuple[str, list[str], bool, Any, str, bool, type, str]]:
        """Get flags from schema that depend on configuration"""
        for key in self.schema.key_map:
            flag = "_".join(key)
            required, default, description, secret, _type, json_type = reduce(
                lambda x, y: x[y], key, self.properties
            )
            yield flag, key, required, default, description, secret, _type, json_type
