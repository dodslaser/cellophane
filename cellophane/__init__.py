"""Cellophane: A library for writing modular wrappers"""
import logging
import time
from copy import deepcopy
from functools import reduce
from multiprocessing import Queue
from pathlib import Path
from shutil import copyfile, copytree
from typing import Any, Literal, Sequence

import rich_click as click
from cloudpickle import dumps, loads
from humanfriendly import format_timespan
from mpire import WorkerPool
from ruamel.yaml.scanner import ScannerError

from .src import cfg, data, logs, modules, sge, util

__all__ = [
    "CELLOPHANE_ROOT",
    "cellophane",
    "cfg",
    "data",
    "logs",
    "modules",
    "sge",
    "util",
]

CELLOPHANE_ROOT = Path(__file__).parent


def _run_hooks(
    hooks: Sequence[modules.Hook],
    when: Literal["pre", "post"],
    samples: data.Samples,
    **kwargs: Any,
) -> data.Samples:
    _samples = deepcopy(samples)
    for hook in [h for h in hooks if h.when == when]:
        match hook.condition:
            case "complete":
                _samples = _samples.complete
            case "failed":
                _samples = _samples.failed

        if _samples or when == "pre":
            _samples = hook(samples=_samples, **kwargs)

    return _samples


def _start_runners(
    runners: Sequence[modules.Runner],
    samples: data.Samples,
    logger: logging.LoggerAdapter,
    log_queue: Queue,
    **kwargs: Any,
) -> data.Samples:
    if not runners:
        logger.warning("No runners to execute")
        return samples

    with WorkerPool(
        use_dill=True, daemon=False, start_method="fork", shared_objects=log_queue
    ) as pool:
        try:
            results = []
            for runner, _samples in (
                (r, s)
                for r in runners
                for s in (
                    samples.split(link_by=r.link_by)
                    if r.individual_samples
                    else [samples]
                )
            ):
                result = pool.apply_async(
                    runner,
                    kwargs={
                        **kwargs,
                        "samples_pickle": dumps(_samples),
                    },
                )
                results.append(result)

            pool.stop_and_join(keep_alive=True)
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling runners to shut down...")
            pool.terminate()
            return samples

        except Exception as exception:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception in runner: {exception}")
            pool.terminate()
            return samples

        try:
            return reduce(lambda a, b: a | b, (loads(r.get()) for r in results))
        except Exception as exception:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception when collecting results: {exception}")
            return samples


def _copy_outputs(
    outputs: list[data.Output], logger: logging.LoggerAdapter, config: cfg.Config
) -> None:
    logger.info(f"Copying {len(outputs)} outputs")
    for output in outputs:
        if not output.dst.is_relative_to(config.resultdir):
            logger.error(f"{output.dst} is not relative to {config.resultdir}")
        elif output.dst.exists():
            logger.warning(f"{output.dst} already exists")
        elif not output.src.exists():
            logger.error(f"{output.src} does not exist")
        elif output.src.is_dir():
            logger.debug(f"Copying {output.src} to {output.dst}")
            copytree(output.src, output.dst)

        else:
            logger.debug(f"Copying {output.src} to {output.dst}")
            output.dst.parent.mkdir(parents=True, exist_ok=True)
            copyfile(output.src, output.dst)


def _main(
    hooks: list[modules.Hook],
    runners: list[modules.Runner],
    samples_class: type[data.Samples],
    logger: logging.LoggerAdapter,
    log_queue: Queue,
    config: cfg.Config,
    root: Path,
) -> None:
    """Run cellophane"""
    common_kwargs = {"config": config, "root": root}

    # Load samples from file, or create empty samples object
    if "samples_file" in config:
        logger.debug(f"Loading samples from {config.samples_file}")
        samples = samples_class.from_file(config.samples_file)
    else:
        logger.debug("No samples file specified, creating empty samples object")
        samples = samples_class()

    # Run pre-hooks
    samples = _run_hooks(hooks, "pre", samples, **common_kwargs)

    # Validate samples
    _samples_orig = deepcopy(samples)
    samples.remove_invalid()
    for removed in {s.id for s in _samples_orig} ^ {s.id for s in samples}:
        logger.warning(f"Removed invalid sample {removed}")

    if not samples:
        logger.info("No samples to process")
        raise SystemExit(0)

    samples = _start_runners(runners, samples, logger, log_queue, **common_kwargs)
    samples = _run_hooks(hooks, "post", samples, **common_kwargs)

    # If not post-hook has copied the outputs, do it here
    if missing_outputs := [o for o in samples.output if not o.dst.exists()]:
        _copy_outputs(missing_outputs, logger, config)


def cellophane(
    label: str,
    root: Path,
) -> click.Command:
    """
    Creates a click command for running the Cellophane application.

    Defines a click command that represents the Cellophane application.
    Sets up the necessary configurations, initializes the logger, and executes
    the main logic of the application.

    The log level, config path, and output prefix, are hard-coded as command
    line options. The remaining options are dynamically generated from the
    schema files.

    Args:
        label (str): The label for the application.
        root (Path): The root path for the application.

    Returns:
        click.BaseCommand: The click command for the Cellophane application.
    """
    click.rich_click.DEFAULT_STRING = "[{}]"

    console_handler = logs.setup_logging()
    logger = logging.LoggerAdapter(logging.getLogger(), {"label": label})

    try:
        schema = cfg.Schema.from_file(
            path=[
                CELLOPHANE_ROOT / "schema.base.yaml",
                root / "schema.yaml",
                *(root / "modules").glob("*/schema.yaml"),
            ],
        )
    except ScannerError as exc:
        logger.critical(f"Failed to load schema: {exc}")
        raise SystemExit(1) from exc

    try:
        (
            hooks,
            runners,
            sample_mixins,
            samples_mixins,
        ) = modules.load(root / "modules")

        _SAMPLE = data.Sample.with_mixins(sample_mixins)
        _SAMPLES = data.Samples.with_sample_class(_SAMPLE).with_mixins(samples_mixins)

        @cfg.options(schema)
        def inner(config: cfg.Config, **_: Any) -> None:
            """Run cellophane"""
            console_handler.setLevel(config.log_level)
            logger.debug(f"Found {len(hooks)} hooks")
            logger.debug(f"Found {len(runners)} runners")
            logger.debug(f"Found {len(sample_mixins)} sample mixins")
            logger.debug(f"Found {len(samples_mixins)} samples mixins")

            config.analysis = label  # type: ignore[attr-defined]
            logs.add_file_handler(
                config.logdir / f"{label}.{config.tag}.log", logger.logger
            )
            log_queue = logs.start_queue_listener()
            try:
                _main(
                    hooks=hooks,
                    runners=runners,
                    samples_class=_SAMPLES,
                    config=config,
                    logger=logger,
                    log_queue=log_queue,
                    root=root,
                )

            except Exception as exception:
                logger.critical(f"Unhandled exception: {exception}", exc_info=True)
                raise SystemExit(1) from exception

            else:
                time_elapsed = format_timespan(time.time() - config.start_time)
                logger.info(f"Execution complete in {time_elapsed}")

    except Exception as exc:
        logger.critical(exc)
        raise SystemExit(1) from exc

    return inner
