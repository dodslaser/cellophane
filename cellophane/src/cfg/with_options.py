"""Configuration file handling and CLI generation"""

import time
from pathlib import Path
from typing import Callable

import rich_click as click
from ruamel.yaml import YAML

from cellophane.src import data

from .config import Config
from .jsonschema_ import get_flags
from .schema import Schema


def with_options(schema: Schema) -> Callable:
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

            try:
                config_data = (
                    YAML(typ="safe").load(config_file)
                    if config_file is not None
                    else {}
                )
            except Exception as exc:
                raise click.FileError(str(config_file), str(exc))

            # Create a dummy command to collect any flags that are passed
            _dummy_cmd = click.command()(lambda: None)
            for flag in get_flags(schema):
                _dummy_cmd = flag.click_option(_dummy_cmd)
            _dummy_ctx = _dummy_cmd.make_context(
                ctx.info_name,
                ctx.args.copy(),
                resilient_parsing=True,
            )
            _dummy_params = {
                param: value
                for param, value in _dummy_ctx.params.items()
                if value is not None
                and (src := _dummy_ctx.get_parameter_source(param))
                and src.name != "DEFAULT"
            }

            # Merge config file and the commandline arguments into a single config
            config = Config(
                schema=schema,
                tag=_dummy_params.pop("tag", None),
                include_defaults=False,
                _data=config_data,
                **_dummy_params,
            )

            # Set the workdir, resultdir, and logdir (if possible)
            if "workdir" in config:
                (config["resultdir"], config["logdir"]) = (
                    config.get("resultdir", config.workdir / "results"),
                    config.get("logdir", config.workdir / "logs"),
                )

            # Add flags to the callback with the values from the dummy command
            callback = click.make_pass_decorator(Config)(callback)
            _callback = click.command(callback)
            for flag in get_flags(schema, data.as_dict(config)):
                _callback = flag.click_option(_callback)

            # Create the callback context and forward arguments
            callback_ctx = _callback.make_context(
                ctx.info_name,
                ctx.args.copy(),
            )

            # Inner function expects a Config object as the first argument
            callback_ctx.obj = config

            # Ensure that the configuration is complete
            callback_ctx.obj.set_defaults()

            # Invoke the callback
            callback_ctx.forward(_callback)

        return inner

    return wrapper
