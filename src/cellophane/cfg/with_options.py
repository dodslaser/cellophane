"""Configuration file handling and CLI generation"""
from __future__ import annotations

from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING

import rich_click as click

from cellophane.data import Container, as_dict

from .config import Config
from .jsonschema_ import get_flags
from .util import inclusive_yaml

if TYPE_CHECKING:
    from typing import Any, Callable

    from .schema import Schema


def with_options(schema: Schema, root: Path) -> Callable:
    """Creates a decorator for adding command-line interface from a schema.

    The callback will be passed a Container object with the config as the first argument.

    Args:
    ----
        schema (Schema): The schema object defining the command-line interface.
        root (Path): The root path for resolving paths relative to the project root.

    Returns:
    -------
        Callable: The decorated callback function.

    Examples:
    --------
        @with_options(schema, root)
        def cli(config: Container, **kwargs):
            ...

    """



    def wrapper(callback: Callable) -> Callable:
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

            _root = config_file.parent if config_file is not None else None
            yaml = inclusive_yaml(_root)

            try:
                config_container = Container(
                    yaml.load(config_file)
                    if config_file is not None
                    else {}
                )
            except Exception as exc:
                raise click.FileError(str(config_file), str(exc))

            # Create a dummy command to collect any flags that are passed
            _dummy_cmd = click.command()(lambda: None)
            _flags = {flag.flag: flag for flag in get_flags(schema)}
            for flag in _flags.values():
                _dummy_cmd = flag.dummy_click_option(_dummy_cmd)

            _dummy_ctx = _dummy_cmd.make_context(
                ctx.info_name,
                ctx.args.copy(),
                resilient_parsing=True,
            )
            for param, value in _dummy_ctx.params.items():
                if (
                    value is not None
                    and (src := _dummy_ctx.get_parameter_source(param))
                    and src.name != "DEFAULT"
                    and param in _flags
                ):
                    config_container[_flags[param].key] = value

            # Set the config file path
            config_container.config_file = config_file

            # Set the executor environment cache path
            config_container.executor = config_container.get("executor", {})
            config_container.executor.environment = config_container.executor.get("environment", {})
            config_container.executor.environment.cache = config_container.executor.environment.get("cache", root / ".environments")

            # Set the workdir, resultdir, and logdir (if possible)
            if "workdir" in config_container:
                workdir = Path(config_container.workdir)
                config_container.resultdir = config_container.get("resultdir", workdir / "results")
                config_container.logdir = config_container.get("logdir", workdir / "logs")

            # Add flags to the callback with the values from the dummy command
            _final_flags = {flag.flag: flag for flag in get_flags(schema, as_dict(config_container))}

            @click.command()
            @wraps(callable)
            def _callback(**kwargs: Any) -> Any:
                config = Config(schema=schema)
                for kwarg, flag in _final_flags.items():
                    if kwarg not in kwargs:
                        continue
                    if kwargs[kwarg] is not None:
                        config[flag.key] = kwargs[kwarg]
                return callback(config)

            for flag in _final_flags.values():
                _callback = flag.click_option(_callback)

            # Create the callback context and forward arguments
            callback_ctx = _callback.make_context(
                ctx.info_name,
                ctx.args.copy(),
            )
            # Invoke the callback
            callback_ctx.forward(_callback)

        return inner

    return wrapper

