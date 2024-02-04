"""Configuration file handling and CLI generation"""

import time
from copy import deepcopy
from functools import cache, cached_property, partial, singledispatch
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import rich_click as click
from attrs import define, field
from frozendict import frozendict
from jsonschema.validators import extend
from ruamel.yaml import YAML, CommentedMap, CommentToken
from ruamel.yaml.compat import StringIO
from ruamel.yaml.error import CommentMark
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

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


class Blank:
    ...


def _dump_yaml(data_: Any) -> str:
    """Dumps data to a YAML string"""
    with StringIO() as handle:
        yaml = YAML(typ="rt")
        # Representer for preserved dicts

        yaml.representer.add_representer(
            data.dict_,
            lambda dumper, data: dumper.represent_dict(data),
        )
        # Representer for null as ~
        yaml.representer.add_representer(
            type(None),
            lambda dumper, *_: dumper.represent_scalar("tag:yaml.org,2002:null", "~"),
        )
        yaml.representer.add_representer(
            Blank,
            lambda dumper, *_: dumper.represent_scalar("tag:yaml.org,2002:null", ""),
        )
        yaml.dump(data_, handle)
        return handle.getvalue()


def _comment_yaml_block(
    yaml: CommentedMap,
    key: tuple[str, ...] | list[str],
    level: int = 0,
) -> None:
    """
    Recursively comment a YAML node in-place.

    This will comment all sequence and mapping items in the node, as well as
    multi-line strings.

    Args:
        node (CommentedBase): The YAML node to comment.
        index (Any): The index of the node.
        level (int, optional): The level of the node. Defaults to 0.
    """

    # Get the parent node and the index of the leaf node
    node = yaml.mlget([*key[:-1]]) if len(key) > 1 else yaml
    index = key[-1]

    # Dump the current value as a YAML string
    lines = _dump_yaml({"DUMMY": node[index]}).splitlines()

    # Use DUMMY as a placeholder key to extract any additional content on the first line
    # eg. "key: |" -> "DUMMY: |" -> "|"
    lines[0] = lines[0].removeprefix("DUMMY:").removeprefix(" ")

    # Comment and pad all lines to the current level
    for idx in range(1, len(lines)):
        lines[idx] = f"#{' ' * (level - 1) * 2} {lines[idx]}"
    commented_block = "\n".join(lines)

    # Replace current value with a blank placeholder
    node[index] = Blank()

    # Get the current comment token(s) for the node
    node_ca = node.ca.items.setdefault(index, [None, []])

    # If there are comments, pad them
    for comment in node_ca[1]:
        comment.value = f"#{' ' * comment.column} {comment.value}"
        comment.start_mark = CommentMark(0)

    # Add a comment token before the node key
    node_ca[1].append(CommentToken("# ", CommentMark(0)))

    # Add back value as a comment token
    node_ca.append(CommentToken(commented_block, CommentMark(0)))


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

        # Generate a list of flags and all of their parent nodes
        _nodes_flags = [
            ({(*flag.key[: i + 1],) for i in range(len(flag.key))}, flag)
            for flag in _get_flags(self, {})
        ]

        # Keep track of nodes to be commented
        to_be_commented = {i for n, _ in _nodes_flags for i in n}

        for node_keys, flag in _nodes_flags:
            current_node = _map
            for k in flag.key[:-1]:
                current_node.setdefault(k, CommentedMap())
                current_node = current_node[k]

            # FIXME: Can this be moved somewhere else?
            _default: Any
            if isinstance(flag.default, str):
                if "\n" in flag.default:
                    # If the string is multi-line, use | to preserve newlines
                    _default = LiteralScalarString(flag.default)
                else:
                    # Otherwise, use " to preserve whitespace
                    _default = DoubleQuotedScalarString(flag.default)
            else:
                # For all other types, use the default string representation
                _default = flag.default

            # Add the flag to the current node
            current_node.insert(1, flag.key[-1], _default)

            # Construct item description as a comment token
            if flag.description:
                _comment = f"{flag.description} [{flag.type}]"
            else:
                _comment = f"[{flag.type}]"

            # Add a REQUIRED tag to the comment if the flag is required
            # and remove the node from the list of nodes to be commented
            if flag.required:
                _comment = f"{_comment} (REQUIRED)"
                to_be_commented -= node_keys

            # Add a comment token before the flag
            current_node.ca.items.setdefault(flag.key[-1], [None, []])
            current_node.ca.items[flag.key[-1]][1] = [
                CommentToken(f"# {_comment}\n", CommentMark((len(flag.key) - 1) * 2))
            ]

        # raise Exception(to_be_commented)
        for node_key in to_be_commented:
            # Only comment the top-most node
            if node_key[:-1] not in to_be_commented:
                _comment_yaml_block(_map, node_key, level=len(node_key))

        # Dump the map to a string
        return _dump_yaml(_map)


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
            if flag.flag in kwargs:
                self[flag.key] = flag.convert(kwargs[flag.flag])
            elif flag.value is not None:
                self[flag.key] = flag.convert(flag.value)
            elif flag.default is not None and include_defaults:
                self[flag.key] = flag.convert(flag.default)


def _set_defaults(config: Config) -> None:
    """Updates the configuration from keyword arguments"""
    for flag in _get_flags(config.__schema__):
        if flag.default is not None and flag.key not in config:
            config[flag.key] = flag.convert(flag.default)


@singledispatch
def _get_flags(schema: Schema, _data: Mapping | None = None) -> list[Flag]:
    return _get_flags(util.freeze(data.as_dict(schema)), util.freeze(_data))


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
        @click.option(
            "--config_file",
            type=Path,
            default=None,
        )
        @click.pass_context
        def inner(ctx: click.Context, config_file: Path | None) -> None:
            nonlocal callback

            # Create a dummy command to collect any flags that are passed
            _dummy_cmd = click.command()(lambda: None)
            for flag in _get_flags(schema):
                _dummy_cmd = flag.click_option(_dummy_cmd)
            _dummy_ctx = _dummy_cmd.make_context(
                ctx.info_name,
                ctx.args.copy(),
                resilient_parsing=True,
            )
            _dummy_params = {
                param: value
                for param, value in _dummy_ctx.params.items()
                if (src := _dummy_ctx.get_parameter_source(param))
                and src.name != "DEFAULT"
            }

            # Merge config file and the commandline arguments into a single config
            config = Config(
                schema=schema,
                tag=_dummy_params.pop("tag", None) or timestamp,
                include_defaults=False,
                _data=YAML(typ="safe").load(config_file) if config_file else {},
                **_dummy_params,
            )

            # Set timestamp and start time
            config["timestamp"] = timestamp
            config["start_time"] = start_time

            # Set the workdir, resultdir, and logdir (if possible)
            if "workdir" in config:
                (
                    config["resultdir"],
                    config["logdir"],
                ) = (
                    config.get("resultdir", config.workdir / "results"),
                    config.get("logdir", config.workdir / "logs"),
                )

            # Add flags to the callback with the values from the dummy command
            callback = click.make_pass_decorator(Config)(callback)
            _callback = click.command(callback)
            for flag in _get_flags(schema, data.as_dict(config)):
                _callback = flag.click_option(_callback)

            # Create the callback context and forward arguments
            callback_ctx = _callback.make_context(
                ctx.info_name,
                ctx.args.copy(),
            )

            # Inner function expects a Config object as the first argument
            callback_ctx.obj = config

            # Ensure that the configuration is complete
            _set_defaults(callback_ctx.obj)

            # Invoke the callback
            callback_ctx.forward(_callback)

        return inner

    return wrapper
