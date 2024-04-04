"""JSON Schema validators for Cellophane configuration files."""

from copy import deepcopy
from functools import cache, partial, reduce, singledispatch
from pathlib import Path
from typing import Callable, Generator, Mapping

from frozendict import frozendict
from jsonschema.validators import Draft7Validator, extend

from cellophane.src import data, util

from .flag import Flag

_cellophane_type_checker = Draft7Validator.TYPE_CHECKER.redefine_many(
    {
        "mapping": lambda _, instance: isinstance(instance, Mapping),
        "path": lambda _, instance: isinstance(instance, Path | str),
        "size": lambda _, instance: isinstance(instance, str | int),
    }
)

BaseValidator = extend(
    Draft7Validator,
    type_checker=_cellophane_type_checker,
)

NullValidator = extend(
    BaseValidator,
    validators={v: None for v in Draft7Validator.VALIDATORS},
)


def _uptate_validators(
    validators: dict[str, Callable],
    flags: dict | None = None,
    compiled: dict | None = None,
) -> None:
    for _validator in validators.values():
        if isinstance(_validator, partial):
            if "flags" in _validator.keywords:
                _validator.keywords.update({"flags": flags})
            if "compiled" in _validator.keywords:
                _validator.keywords.update({"compiled": compiled})


def properties_(
    validator: Draft7Validator,
    properties: dict[str, dict],
    instance: dict,
    schema: dict,
    flags: dict | None = None,
    compiled: dict | None = None,
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
        _instance = instance or {}
        _required = prop in schema.get("required", []) and instance is not None

        if "properties" in subschema and subschema.get("type") != "mapping":
            if flags is not None:
                flags[prop] = flags.get(prop, {})

            _uptate_validators(
                validator.VALIDATORS,
                flags=(flags or {}).get(prop),
                compiled=(compiled or {}).get("properties", {}).get(prop),
            )

            yield from validator.descend(
                _instance.get(prop, {} if _required else None),
                subschema,
                path=prop,
                schema_path=prop,
            )

            _uptate_validators(
                validator.VALIDATORS,
                flags=flags,
                compiled=compiled,
            )
        elif flags is not None and "properties" not in subschema:
            _flag_kwargs = {
                "value": _instance.get(prop),
                "type": subschema.get("type"),
                "enum": subschema.get("enum"),
                "description": subschema.get("description"),
                "secret": subschema.get("secret", False),
                "items_type": subschema.get("items", {}).get("type"),
                "items_format": subschema.get("items", {}).get("format"),
                "items_minimum": subschema.get("items", {}).get("minimum"),
                "items_maximum": subschema.get("items", {}).get("maximum"),
                "format_": subschema.get("format"),
                "minimum": subschema.get("minimum"),
                "maximum": subschema.get("maximum"),
            }
            if prop in flags:
                for k, v in _flag_kwargs.items():
                    setattr(flags[prop], k, v)
            else:
                flags[prop] = Flag(**_flag_kwargs)

            if (default := subschema.get("default")) is not None:
                try:
                    flags[prop].default = flags[prop].convert(default)
                except Exception:  # pylint: disable=broad-except
                    flags[prop].default = default


def required_(
    validator: Draft7Validator,
    required: list[str],
    instance: dict,
    schema: dict,
    flags: dict,
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
                flags[prop] = flags.get(prop, Flag())
                flags[prop].required = True


def dependent_required_(
    validator: Draft7Validator,
    dependencies: dict[str, list[str]],
    instance: dict,
    schema: dict,
    flags: dict,
) -> None:
    """Mark dependent flags as required"""
    if instance is not None:
        for dep, req in dependencies.items():
            if dep in instance:
                required_(validator, req, instance, schema, flags)


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
        _subschema = reduce(util.merge_mappings, dependencies.values())
    elif _valid := [s for d, s in dependencies.items() if d in instance]:
        _subschema = reduce(util.merge_mappings, _valid)
    else:
        _subschema = {}
    compiled |= util.merge_mappings(compiled, _subschema)
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
    _subschema = reduce(util.merge_mappings, all_of)
    compiled |= util.merge_mappings(compiled, _subschema)
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
        _subschema = reduce(util.merge_mappings, any_of)
    elif _valid := [s for s in any_of if BaseValidator(s).is_valid(instance)]:
        _subschema = reduce(util.merge_mappings, _valid)
    else:
        _subschema = {}

    compiled |= util.merge_mappings(compiled, _subschema)
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
        _subschema = reduce(util.merge_mappings, one_of)
    else:
        try:
            _subschema = next(s for s in one_of if BaseValidator(s).is_valid(instance))
        except StopIteration:
            _subschema = {}

    compiled |= util.merge_mappings(compiled, _subschema)
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
        _subschema = util.merge_mappings(schema.get("then", {}), schema.get("else", {}))
    elif BaseValidator(if_schema).is_valid(instance):
        _subschema = schema.get("then", {})
    else:
        _subschema = schema.get("else", {})

    compiled |= util.merge_mappings(compiled, _subschema)
    compiled.pop("if")

@singledispatch
def get_flags(schema: data.Container, _data: Mapping | None = None) -> list[Flag]:
    """Get the flags for a configuration schema."""
    return get_flags(util.freeze(data.as_dict(schema)), util.freeze(_data))


@get_flags.register
@cache
def _(schema: frozendict, _data: frozendict | None = None) -> list[Flag]:
    _data_thawed = util.unfreeze(_data)
    _schema_thawed = util.unfreeze(schema)
    _flags_mapping: dict = {}

    while any(
        keyword in (kw for node in util.map_nested_keys(_schema_thawed) for kw in node)
        for keyword in [
            "if",
            "anyOf",
            "oneOf",
            "allOf",
            "dependentSchemas",
        ]
    ):
        _compiled = deepcopy(_schema_thawed)
        _compile_conditional = extend(
            NullValidator,
            validators={
                "properties": partial(properties_, compiled=_compiled),
                "if": partial(if_, compiled=_compiled),
                "anyOf": partial(any_of_, compiled=_compiled),
                "oneOf": partial(one_of_, compiled=_compiled),
                "allOf": partial(all_of_, compiled=_compiled),
                "dependentSchemas": partial(dependent_schemas_, compiled=_compiled),
            },
        )

        _compile_conditional(_schema_thawed).validate(_data_thawed)
        _schema_thawed = _compiled

    extend(
        NullValidator,
        validators={
            "required": partial(required_, flags=_flags_mapping),
            "dependentRequired": partial(dependent_required_, flags=_flags_mapping),
            "properties": partial(properties_, flags=_flags_mapping),
        },
    )(_schema_thawed).validate(_data_thawed)

    _flags: list[Flag] = []
    _container = data.Container(_flags_mapping)
    for key in util.map_nested_keys(_flags_mapping):
        _flag = _container[key]
        _flag.key = key
        _flags.append(_flag)

    return _flags
