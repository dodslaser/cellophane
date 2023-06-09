"""Cellophane: A library for writing modular wrappers"""
import logging
import multiprocessing as mp
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable
from uuid import UUID
from queue import Queue
from functools import partial

import rich_click as click
from attrs import define
from humanfriendly import format_timespan
from jsonschema.exceptions import ValidationError

from .src import cfg, data, logs, modules, sge, util  # noqa: F401


_PROCS: dict[UUID, modules.Runner] = {}
CELLOPHANE_ROOT = Path(__file__).parent


class _SAMPLES(data.Samples):
    pass


class _SAMPLE(data.Sample):
    pass


def _cleanup(logger):
    for proc in _PROCS.values():
        if proc.exitcode is None:
            logger.debug(f"Terminating {proc.label}")
            try:
                proc.terminate()
                proc.join()
            # Handle weird edge cases when terminating processes
            except Exception as exception:
                logger.debug(f"Failed to terminate {proc.label}: {exception}")


def _click_mapping(ctx, param, value):
    try:
        return cfg.parse_mapping(value)
    except Exception as exception:
        raise click.BadParameter(f"Invalid mapping: {exception}")


def _main(
    logger: logging.LoggerAdapter,
    config: cfg.Config,
    log_queue: Queue,
    output_queue: mp.Queue,
    root: Path,
    timestamp: str,
) -> None:
    """Run cellophane"""

    # Load modules
    hooks: list[type[modules.Hook]] = []
    runners: list[type[modules.Runner]] = []
    sample_mixins: list[type[data.Sample]] = []
    samples_mixins: list[type[data.Samples]] = []
    for base, obj in modules.load_modules(root / "modules"):
        if issubclass(obj, modules.Hook) and not obj == modules.Hook:
            logger.debug(f"Found hook {obj.__name__} ({base})")
            hooks.append(obj)
        elif issubclass(obj, data.Sample) and obj is not data.Sample:
            logger.debug(f"Found mixin {obj.__name__} ({base})")
            sample_mixins.append(obj)
        elif issubclass(obj, data.Samples) and obj is not data.Samples:
            logger.debug(f"Found mixin {obj.__name__} ({base})")
            samples_mixins.append(obj)
        elif issubclass(obj, modules.Runner) and not obj == modules.Runner:
            logger.debug(f"Found runner {obj.__name__} ({base})")
            runners.append(obj)

    # Resolve hook dependencies using topological sort
    try:
        hooks = modules.resolve_hook_dependencies(hooks)
    except Exception as exception:
        logger.error(f"Failed to resolve hook dependencies: {exception}")
        raise SystemExit(1)

    # Add mixins to data classes
    global _SAMPLES
    global _SAMPLE
    if samples_mixins:
        _SAMPLES.__bases__ = (*samples_mixins,)
    _SAMPLES = define(_SAMPLES, init=False, slots=False)  # type: ignore[misc]

    if sample_mixins:
        _SAMPLE.__bases__ = (*sample_mixins,)
    _SAMPLE = define(_SAMPLE, init=False, slots=False)  # type: ignore[misc]

    _SAMPLES.sample_class = _SAMPLE

    # Load samples from file, or create empty samples object
    if "samples_file" in config:
        samples = _SAMPLES.from_file(config.samples_file)
    else:
        samples = _SAMPLES()

    # Run pre-hooks
    for hook in [h() for h in hooks if h.when == "pre"]:
        result = hook(
            samples=deepcopy(samples),
            config=config,
            timestamp=timestamp,
            log_queue=log_queue,
            log_level=config.log_level,
            root=root,
        )

        if issubclass(type(result), data.Samples):
            samples = result

    # Validate samples
    for invalid_sample in samples.validate():
        logger.warning(f"Removed invalid sample {invalid_sample.id}")

    # Start all loaded runners
    result_samples: data.Samples = _SAMPLES()
    sample_pids: dict[str, set[UUID]] = {s.id: set() for s in samples}

    try:
        for runner in runners if samples else []:
            logger.info(f"Starting runner {runner.__name__} for {len(samples)} samples")

            for _samples in (
                samples.split(link_by=runner.link_by)
                if runner.individual_samples
                else [samples]
            ):
                proc = runner(
                    samples=_samples,
                    config=config,
                    timestamp=timestamp,
                    output_queue=output_queue,
                    log_queue=log_queue,
                    log_level=config.log_level,
                    root=root,
                )
                _PROCS[proc.id] = proc
                for sample in _samples:
                    sample_pids[sample.id] |= {proc.id}

        for proc in _PROCS.values():
            proc.start()

        # Wait for all runners to finish
        while not all(proc.done for proc in _PROCS.values()):
            result, pid = output_queue.get()
            result_samples += result
            _PROCS[pid].join()
            _PROCS[pid].done = True

    except KeyboardInterrupt:
        logger.critical("Received SIGINT, telling runners to shut down...")
        _cleanup(logger)

    except Exception as e:
        logger.critical(f"Unhandled exception in runner: {e}")
        _cleanup(logger)

    finally:
        # Run post-hooks
        for hook in [h() for h in hooks if h.when == "post"]:
            logger.debug(f"Running hook {hook.name}")
            match hook.condition:
                case "complete":
                    hook_samples = result_samples.complete
                case "failed":
                    hook_samples = result_samples.failed
                case "always":
                    hook_samples = result_samples
            if hook_samples:
                hook(
                    samples=hook_samples,
                    config=config,
                    timestamp=timestamp,
                    log_queue=log_queue,
                    log_level=config.log_level,
                    root=root,
                )


def _add_config_flags(
    ctx: click.Context,
    config_path: Path | None,
    schema: cfg.Schema,
) -> Path | None:
    if config_path is not None:
        _config = cfg.Config.from_file(
            schema=schema,
            path=config_path,
            validate=False,
        )

        ctx.default_map = {}
        for flag, key, required, default, *_ in _config.flags:
            ctx.default_map[flag] = default
            idx = next(i for i, p in enumerate(ctx.command.params) if p.name == flag)
            ctx.command.params[idx].required = required

    return config_path


def cellophane(
    label: str,
    root: Path,
) -> Callable:
    """Generate a cellophane CLI from a schema file"""
    click.rich_click.DEFAULT_STRING = "[{}]"

    # _schema_path = schema_path or root / "schema.yaml"
    _manager = mp.Manager()
    _log_queue = logs.get_log_queue(_manager)
    _output_queue: mp.Queue = mp.Queue()

    schema = cfg.Schema.from_file(
        path=[
            CELLOPHANE_ROOT / "schema.base.yaml",
            root / "schema.yaml",
            *(root / "modules").glob("*/schema.yaml"),
        ],
    )

    logger = logs.get_logger(
        label=label,
        level=logging.INFO,
        queue=_log_queue,
    )

    @click.command()
    @click.option(
        "config_path",
        "--config",
        type=click.Path(exists=True),
        help="Path to config file",
        is_eager=True,
        callback=lambda ctx, _, value: _add_config_flags(ctx, value, schema),
    )
    @click.option(
        "--log_level",
        type=click.Choice(
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            case_sensitive=False,
        ),
        default="INFO",
        help="Log level",
        show_default=True,
    )
    @logs.log_exceptions(
        logger=logger,
        exit=False,
        cleanup_fn=partial(_cleanup, logger),
    )
    def inner(config_path, log_level, logger, **kwargs) -> Any:
        """Run cellophane"""
        try:
            start_time = time.time()
            timestamp: str = time.strftime("%Y%m%d_%H%M%S", time.localtime(start_time))
            config = cfg.Config(
                schema=schema,
                validate=True,
                **kwargs,
            )
            config["analysis"] = label
            config["log_level"] = log_level
            outprefix = config.get("outprefix", timestamp)

            logger.setLevel(config.log_level)
            logs.add_file_handler(
                logger=logger.logger,
                path=config.logdir / f"{label}.{outprefix}.log",
            )

            _main(
                config=config,
                logger=logger,
                log_queue=_log_queue,
                output_queue=_output_queue,
                root=root,
                timestamp=timestamp,
            )
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling active processes to shut down...")
            _cleanup(logger)
        except ValidationError as exception:
            _config = cfg.Config(
                schema=schema,
                path=config_path,
                validate=False,
                **kwargs,
            )
            for error in schema.iter_errors(_config):
                logger.critical(f"Invalid configuration: {error.message}")
            raise SystemExit(1) from exception

        finally:
            time_elapsed = format_timespan(time.time() - start_time)
            logger.info(f"Execution complete in {time_elapsed}")

    for (
        flag,
        _,
        required,
        default,
        description,
        secret,
        _type,
        *_,
    ) in schema.flags:
        inner = click.option(
            f"--{flag}",
            type=str if _type in (list, dict) else _type,
            callback=_click_mapping if _type == dict else None,
            is_flag=_type == bool,
            multiple=_type in (list, dict),
            default=default,
            help=description,
            show_default=not secret,
            required=required,
        )(inner)

    return inner
