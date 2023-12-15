"""JSON Schema validators for Cellophane configuration files."""

from functools import partial, reduce
from pathlib import Path
from typing import Callable, Generator, Mapping

from jsonschema.validators import Draft7Validator, extend

from cellophane.src import util

from ._click import Flag

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


def properties(
    validator: Draft7Validator,
    properties: dict[str, dict],
    instance: dict,
    schema: dict,
    flags: dict | None = None,
    compiled: dict | None = None,
) -> Generator:
    for property, subschema in properties.items():
        _instance = instance or {}
        _required = property in schema.get("required", []) and instance is not None

        if "properties" in subschema and subschema.get("type") != "mapping":
            if flags is not None:
                flags[property] = flags.get(property, {})

            _uptate_validators(
                validator.VALIDATORS,
                flags=(flags or {}).get(property),
                compiled=(compiled or {}).get("properties", {}).get(property),
            )

            yield from validator.descend(
                _instance.get(property, {} if _required else None),
                subschema,
                path=property,
                schema_path=property,
            )

            _uptate_validators(
                validator.VALIDATORS,
                flags=flags,
                compiled=compiled,
            )
        elif flags is not None and "properties" not in subschema:
            _flag_kwargs = {
                "default": subschema.get("default"),
                "value": _instance.get(property),
                "type": subschema.get("type"),
                "enum": subschema.get("enum"),
                "description": subschema.get("description"),
                "secret": subschema.get("secret", False),
                "items": subschema.get("items", {}).get("type"),
            }
            if property in flags:
                for k, v in _flag_kwargs.items():
                    setattr(flags[property], k, v)
            else:
                flags[property] = Flag(**_flag_kwargs)


def required(
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


def dependent_required(
    validator: Draft7Validator,
    dependencies: dict[str, list[str]],
    instance: dict,
    schema: dict,
    flags: dict,
) -> None:
    if instance is not None:
        for dep, req in dependencies.items():
            if dep in instance:
                required(validator, req, instance, schema, flags)


def dependent_schemas(
    validator: Draft7Validator,
    dependencies: dict[str, dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    del validator, schema  # Unused

    if instance is not None:
        if _valid := [s for d, s in dependencies.items() if d in instance]:
            _subschema = reduce(util.merge_mappings, _valid)
            compiled |= util.merge_mappings(compiled, _subschema)
        compiled.pop("dependentSchemas")


def all_of(
    validator: Draft7Validator,
    all_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    del validator, instance, schema  # Unused
    _subschema = reduce(util.merge_mappings, all_of)
    compiled |= util.merge_mappings(compiled, _subschema)
    compiled.pop("allOf")


def any_of(
    validator: Draft7Validator,
    any_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    del validator, schema  # Unused

    try:
        _subschema = reduce(
            util.merge_mappings,
            (s for s in any_of if BaseValidator(s).is_valid(instance or {})),
        )
    except TypeError:
        _subschema = {}
    compiled |= util.merge_mappings(compiled, _subschema)
    compiled.pop("anyOf")


def one_of(
    validator: Draft7Validator,
    one_of: list[dict],
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    del validator, schema  # Unused

    try:
        _subschema = next(s for s in one_of if BaseValidator(s).is_valid(instance or {}))
    except StopIteration:
        _subschema = reduce(util.merge_mappings, one_of)
    compiled |= util.merge_mappings(compiled, _subschema)
    compiled.pop("oneOf")


def if_(
    validator: Draft7Validator,
    if_schema: dict,
    instance: dict,
    schema: dict,
    compiled: dict,
) -> None:
    del validator  # Unused

    _instance = instance or {}
    _path = "then" if BaseValidator(if_schema).is_valid(_instance) else "else"
    _subschema = schema.get(_path, {})
    compiled |= util.merge_mappings(compiled, _subschema)
    compiled.pop("if")
