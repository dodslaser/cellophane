"""Configuration file handling and CLI generation"""

import time
from copy import copy, deepcopy
from functools import cache, cached_property, partial, singledispatch
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import rich_click as click
from attrs import define, field
from frozendict import frozendict
from jsonschema.validators import extend
from ruamel.yaml import YAML, CommentedMap
from ruamel.yaml.compat import StringIO

from cellophane.src import data, util

from ._click import Flag
from ._jsonschema import (
    NullValidator,
    all_of_,
    any_of_,
    dependent_required_,
    dependent_schemas_,
    if_,
    one_of_,
    properties_,
    required_,
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
    def from_file(cls, path: Path | Sequence[Path]) -> "Schema":
        """Loads the schema from a file or a sequence of files"""
        if isinstance(path, Path):
            with open(path, "r", encoding="utf-8") as handle:
                return cls(YAML(typ="safe").load(handle) or {})
        elif isinstance(path, Sequence):
            schema: dict = {}
            for p in path:
                schema = util.merge_mappings(schema, data.as_dict(cls.from_file(p)))
            return cls(schema)

    @cached_property
    def example_config(self) -> str:
        """Generate an example configuration from the schema"""
        _map = CommentedMap()
        for flag in _get_flags(self):
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
            yaml = YAML(typ="rt")
            yaml.representer.add_representer(
                type(None),
                lambda dumper, *_: dumper.represent_scalar(
                    "tag:yaml.org,2002:null", "~"
                ),
            )
            yaml.dump(_map, handle)
            return handle.getvalue()


@define(init=False, slots=False)
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

    Args:
        schema (Schema): The schema associated with the configuration.
        allow_empty (bool, optional): Allow empty configuration. Defaults to False.
        __data__ (dict | None, optional): The data for the configuration.
            Defaults to None.
        **kwargs: Additional keyword arguments for the configuration.

    Example:
        ```python
        schema = Schema()
        config = Config(schema)

        # Creating a configuration from a file
        path = "config.yaml"
        config = Config.from_file(path, schema)
        ```
    """

    __schema__: Schema = field(repr=False, factory=Schema, init=False)

    def __init__(
        self,
        schema: Schema,
        allow_empty: bool = False,
        _data: dict | None = None,
        include_defaults: bool = True,
        **kwargs: Any,
    ) -> None:
        if not _data and not kwargs and not allow_empty:
            raise ValueError("Empty configuration")

        self.__schema__ = schema

        for flag in _get_flags(schema, _data):
            _converter: partial | Callable = (
                partial(flag.click_type.convert, ctx=None, param=None)
                if isinstance(flag.click_type, click.ParamType)
                else flag.click_type
            )
            if flag.flag in kwargs:
                self[flag.key] = _converter(kwargs[flag.flag])
            elif flag.value is not None:
                self[flag.key] = _converter(flag.value)
            elif flag.default is not None and include_defaults:
                self[flag.key] = _converter(flag.default)


def _set_defaults(config: Config) -> None:
    """Updates the configuration from keyword arguments"""
    for flag in _get_flags(config.__schema__):
        if flag.default is not None and flag.key not in config:
            config[flag.key] = flag.default


@singledispatch
def _get_flags(schema: Schema, _data: Mapping | None = None) -> list[Flag]:
    return _get_flags(util.freeze(data.as_dict(schema)), util.freeze(_data or {}))


@_get_flags.register
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


def options(schema: Schema) -> Callable:
    """
    Creates a decorator for adding command-line interface from a schema.

    The callback will be passed a Config object as the first argument.

    Args:
        schema (Schema): The schema object defining the command-line interface.

    Returns:
        Callable: The decorated callback function.

    Examples:
        @options(schema)
        def cli(config: Config, **kwargs):
            ...
    """

    def wrapper(callback: Callable) -> Callable:
        start_time = time.time()
        timestamp = time.strftime(
            "%Y%m%d_%H%M%S",
            time.localtime(start_time),
        )

        @click.command(
            add_help_option=False,
            context_settings={
                "allow_extra_args": True,
                "ignore_unknown_options": True,
            },
        )
        @click.pass_context
        def inner(ctx: click.Context) -> None:
            _args = deepcopy(ctx.args)

            @click.command(context_settings={"resilient_parsing": True})
            def _update_ctx(**kwargs: Any) -> None:
                config_file: Path = kwargs.pop("config_file", None)
                config_data = YAML(typ="safe").load(config_file) if config_file else {}

                if kwargs.get("workdir") is not None:
                    kwargs["resultdir"], kwargs["logdir"] = (
                        kwargs["resultdir"] or (kwargs["workdir"] / "results"),
                        kwargs["logdir"] or (kwargs["workdir"] / "logs"),
                    )

                ctx.obj = Config(  # type: ignore[call-arg]
                    schema=schema,
                    tag=kwargs.pop("tag", None) or timestamp,
                    include_defaults=False,
                    _data=config_data,
                    **{
                        k: v
                        for k, v in kwargs.items()
                        if v is not None
                        and (source := ctx.get_parameter_source(k))
                        and source.name != "DEFAULT"
                        or k in ("resultdir", "logdir")
                        and v is not None
                    },
                )

            for flag in _get_flags(schema):
                _update_ctx = flag.click_option(_update_ctx)

            ctx = _update_ctx.make_context(ctx.info_name, copy(ctx.args))
            ctx.forward(_update_ctx)

            nonlocal callback
            config = ctx.obj

            callback = click.make_pass_decorator(Config)(callback)
            _callback = click.command(callback)
            for flag in _get_flags(schema, data.as_dict(config)):
                _callback = flag.click_option(_callback)
            config.start_time = start_time
            config.timestamp = timestamp
            _set_defaults(config)
            ctx = _callback.make_context(ctx.info_name, _args)
            ctx.obj = config
            ctx.forward(_callback)

        return inner

    return wrapper
