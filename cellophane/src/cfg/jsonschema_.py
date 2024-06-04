"""JSON Schema validators for Cellophane configuration files."""

from copy import deepcopy
from functools import cache, partial, reduce, singledispatch
from pathlib import Path
from typing import Callable, Generator, Mapping

from frozendict import frozendict
from jsonschema.protocols import Validator
from jsonschema.validators import Draft7Validator, create, extend

from cellophane.src import data, util

from .flag import Flag

_cellophane_type_checker = Draft7Validator.TYPE_CHECKER.redefine_many(
    {
        "mapping": lambda _, instance: isinstance(instance, Mapping),
        "path": lambda _, instance: isinstance(instance, Path | str),
        "size": lambda _, instance: isinstance(instance, str | int),
    }
)

BaseValidator: type[Validator] = extend(
    Draft7Validator,
    type_checker=_cellophane_type_checker,
)

NullValidator = create(
    meta_schema=BaseValidator.META_SCHEMA,
    type_checker=_cellophane_type_checker,
    format_checker=Draft7Validator.FORMAT_CHECKER,
)

def _uptate_validators(
    validators: dict[str, Callable],
    compiled: dict | None = None,
    _path: tuple[str, ...] | None = None,
) -> None:
    for _validator in validators.values():
        if isinstance(_validator, partial):
            if "compiled" in _validator.keywords:
                _validator.keywords.update({"compiled": compiled})
            if "_path" in _validator.keywords:
                _validator.keywords.update({"_path": _path})


def properties_(
    validator: Draft7Validator,
    properties: dict[str, dict],
    instance: dict,
    schema: dict,
    flags: dict[tuple[str, ...], Flag] | None = None,
    compiled: dict | None = None,
    _path: tuple[str, ...] | None = None
) -> Generator:
    """Iterate over the properties of a JSON schema and yield validation results.

    Args:
        validator (Draft7Validator): The JSON schema validator.
        properties (dict[str, dict]): The properties of the schema.
        instance (dict): The instance to validate.
        schema (dict): The JSON schema.
        flags (dict | None, optional): The flags for validation. Defaults to None.
        compiled (dict | None, optional): The compiled schema. Defaults to None.

    Yields:
        Validation results for each property.

    Examples:
        >>> schema = {
        ...     "type": "object",
        ...     "properties": {
        ...         "name": {"type": "string"},
        ...         "age": {"type": "integer"},
        ...     },
        ... }
        >>> instance = {"name": "John", "age": 30}
        >>> validator = Draft7Validator(schema)
        >>> for result in properties(validator, schema["properties"], instance, schema):
        ...     print(result)
    """
    for prop, subschema in properties.items():
        instance_ = instance or {}
        required = prop in schema.get("required", []) and instance is not None

        if "properties" in subschema and subschema.get("type") != "mapping":
            _uptate_validators(
                validator.VALIDATORS,
                compiled=(compiled or {}).get("properties", {}).get(prop),
                _path=(*(_path or ()), prop),
            )

            yield from validator.descend(
                instance_.get(prop, {} if required else None),
                subschema,
                path=prop,
                schema_path=prop,
            )

            _uptate_validators(
                validator.VALIDATORS,
                compiled=compiled,
                _path=_path,
            )
        elif flags is not None and "properties" not in subschema:
            key = (*(_path or ()), prop)
            _flag_kwargs = {
                "key": (_path or ()) + (prop,),
                "value": instance_.get(prop),
                "type": subschema.get("type"),
                "enum": subschema.get("enum"),
                "description": subschema.get("description"),
                "secret": subschema.get("secret", False),
                "items_type": subschema.get("items", {}).get("type"),
                "items_format": subschema.get("items", {}).get("format"),
                "items_min": subschema.get("items", {}).get("minimum"),
                "items_max": subschema.get("items", {}).get("maximum"),
                "format": subschema.get("format"),
                "min": subschema.get("minimum"),
                "max": subschema.get("maximum"),
            }
            if key in flags:
                for k, v in _flag_kwargs.items():
                    setattr(flags[key], k, v)
            else:
                flag = Flag(**_flag_kwargs)
                flags[key] = flag

            if (default := subschema.get("default")) is not None:
                try:
                    flags[key].default = flags[key].convert(default)
                except Exception:  # pylint: disable=broad-except
                    flags[key].default = default


def required_(
    validator: Draft7Validator,
    required: list[str],
    instance: dict,
    schema: dict,
    flags: dict[tuple[str, ...], Flag],
    _path: tuple[str, ...] | None = None,
) -> None:
    """Mark required flags as required"""
    del validator  # Unused
    if instance is not None:
        for prop in required:
            subschema = schema.get("properties", {}).get(prop)
            if not (
                subschema is None
                or "default" in subschema
                or "properties" in subschema
                or prop in instance
            ):
                key = (*(_path or ()), prop)
                flags[key] = flags.get(key, Flag(key=key))
                flags[key].required = True

def dependent_required_(
    validator: Draft7Validator,
    dependencies: dict[str, list[str]],
    instance: dict,
    schema: dict,
    flags: dict[tuple[str, ...], Flag],
    _path: tuple[str, ...] | None = None,
) -> None:
    """Mark dependent flags as required"""
    if instance is not None:
        for dep, req in dependencies.items():
            if dep in instance:
                required_(validator, req, instance, schema, flags, _path)


def dependent_schemas_(
    validator: Draft7Validator,
    dependencies: dict[str, dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    """Merge dependent schemas into the compiled schema"""
    del validator, schema  # Unused

    if instance is None:
        subschema = reduce(util.merge_mappings, dependencies.values())
    elif valid := [s for d, s in dependencies.items() if d in instance]:
        subschema = reduce(util.merge_mappings, valid)
    else:
        subschema = {}
    compiled |= util.merge_mappings(compiled, subschema)
    compiled.pop("dependentSchemas")


def all_of_(
    validator: Draft7Validator,
    all_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    """Merge all subschemas into the compiled schema"""
    del validator, instance, schema  # Unused
    subschema = reduce(util.merge_mappings, all_of)
    compiled |= util.merge_mappings(compiled, subschema)
    compiled.pop("allOf")


def any_of_(
    validator: Draft7Validator,
    any_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    """Merge all valid subschemas into the compiled schema"""
    del validator, schema  # Unused

    if instance is None:
        subschema = reduce(util.merge_mappings, any_of)
    elif _valid := [s for s in any_of if BaseValidator(s).is_valid(instance)]:
        subschema = reduce(util.merge_mappings, _valid)
    else:
        subschema = {}

    compiled |= util.merge_mappings(compiled, subschema)
    compiled.pop("anyOf")


def one_of_(
    validator: Draft7Validator,
    one_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    """Merge the first valid subschema into the compiled schema"""
    del validator, schema  # Unused

    if instance is None:
        subschema = reduce(util.merge_mappings, one_of)
    else:
        try:
            subschema = next(s for s in one_of if BaseValidator(s).is_valid(instance))
        except StopIteration:
            subschema = {}

    compiled |= util.merge_mappings(compiled, subschema)
    compiled.pop("oneOf")


def if_(
    validator: Draft7Validator,
    if_schema: dict,
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    """Check if the instance is valid for the if schema and merge the then or else"""
    del validator  # Unused

    if instance is None:
        subschema = util.merge_mappings(schema.get("then", {}), schema.get("else", {}))
    elif BaseValidator(if_schema).is_valid(instance):
        subschema = schema.get("then", {})
    else:
        subschema = schema.get("else", {})

    compiled |= util.merge_mappings(compiled, subschema)
    compiled.pop("if")

@singledispatch
def get_flags(schema: data.Container, _data: Mapping | None = None) -> list[Flag]:
    """Get the flags for a configuration schema."""
    return get_flags(util.freeze(data.as_dict(schema)), util.freeze(_data))


@get_flags.register
@cache
def _(schema: frozendict, _data: frozendict | None = None) -> list[Flag]:
    data_thawed = util.unfreeze(_data)
    schema_thawed = util.unfreeze(schema)
    flags: dict[tuple[str, ...], Flag] = {}

    while any(
        keyword in (kw for node in util.map_nested_keys(schema_thawed) for kw in node)
        for keyword in [
            "if",
            "anyOf",
            "oneOf",
            "allOf",
            "dependentSchemas",
        ]
    ):
        compiled = deepcopy(schema_thawed)
        compile_conditional = extend(
            NullValidator,
            validators={
                "properties": partial(properties_, compiled=compiled),
                "if": partial(if_, compiled=compiled),
                "anyOf": partial(any_of_, compiled=compiled),
                "oneOf": partial(one_of_, compiled=compiled),
                "allOf": partial(all_of_, compiled=compiled),
                "dependentSchemas": partial(dependent_schemas_, compiled=compiled),
            },
        )

        compile_conditional(schema_thawed).validate(data_thawed)
        schema_thawed = compiled

    extend(
        NullValidator,
        validators={
            "required": partial(required_, flags=flags, _path=None),
            "dependentRequired": partial(dependent_required_, flags=flags, _path=None),
            "properties": partial(properties_, flags=flags, _path=None),
        },
    )(schema_thawed).validate(data_thawed)

    return [*flags.values()]
