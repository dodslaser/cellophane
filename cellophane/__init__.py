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

from .src import cfg, data, executors, logs, modules, util
from .src.cfg import Config
from .src.data import Output, OutputGlob, Sample, Samples
from .src.executors import Executor
from .src.modules import output, post_hook, pre_hook, runner

__all__ = [
    "CELLOPHANE_ROOT",
    "cellophane",
    "cfg",
    "data",
    "logs",
    "modules",
    "util",
    "executors",
    # modules
    "output",
    "post_hook",
    "pre_hook",
    "runner",
    # data
    "Output",
    "OutputGlob",
    "Sample",
    "Samples",
    # executors
    "Executor",
    # cfg
    "Config",
]

CELLOPHANE_ROOT = Path(__file__).parent


def _run_hooks(
    hooks: Sequence[modules.Hook],
    *,
    when: Literal["pre", "post"],
    samples: data.Samples,
    **kwargs: Any,
) -> data.Samples:
    samples = deepcopy(samples)

    for hook in [h for h in hooks if h.when == when]:
        if hook.when == "pre" or hook.condition == "always":
            samples = hook(samples=samples, **kwargs)
        elif hook.condition == "complete" and (s := samples.complete):
            samples = hook(s, **kwargs) | samples.failed
        elif hook.condition == "failed" and (s := samples.failed):
            samples = hook(s, **kwargs) | samples.complete

    return samples


def _start_runners(
    *,
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
        use_dill=True,
        daemon=False,
        start_method="fork",
        shared_objects=log_queue,
    ) as pool:
        try:
            results = []
            for runner_, samples_ in (
                (r, s)
                for r in runners
                for _, s in (
                    samples.split(by=r.split_by) if r.split_by else [(None, samples)]
                )
            ):
                result = pool.apply_async(
                    runner_,
                    kwargs={
                        **kwargs,
                        "samples_pickle": dumps(samples_),
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
            return reduce(lambda a, b: a & b, (loads(r.get()) for r in results))
        except Exception as exception:  # pylint: disable=broad-except
            logger.critical(
                f"Unhandled exception when collecting results: {exception}",
                exc_info=True,
            )
            return samples


def _main(
    hooks: list[modules.Hook],
    runners: list[modules.Runner],
    samples_class: type[data.Samples],
    logger: logging.LoggerAdapter,
    log_queue: Queue,
    config: cfg.Config,
    root: Path,
    executor_cls: type[executors.Executor],
) -> None:
    """Run cellophane"""
    common_kwargs = {
        "config": config,
        "root": root,
        "executor_cls": executor_cls,
    }

    # Load samples from file, or create empty samples object
    if "samples_file" in config:
        logger.debug(f"Loading samples from {config.samples_file}")
        samples = samples_class.from_file(config.samples_file)
    else:
        logger.debug("No samples file specified, creating empty samples object")
        samples = samples_class()

    # Run pre-hooks
    samples = _run_hooks(
        hooks,
        when="pre",
        samples=samples,
        log_queue=log_queue,
        **common_kwargs,
    )

    # Validate sample files
    for sample in samples:
        if sample not in samples.with_files:
            logger.warning(f"Sample {sample} will be skipped as it has no files")

    if not samples.with_files:
        logger.info("No samples to process")
        raise SystemExit(0)

    samples = _start_runners(
        runners=runners,
        samples=samples.with_files,
        logger=logger,
        log_queue=log_queue,
        **common_kwargs,
    )
    samples = _run_hooks(
        hooks,
        when="post",
        samples=samples,
        log_queue=log_queue,
        **common_kwargs,
    )

    # If not post-hook has copied the outputs, warn the user
    if missing_outputs := [
        o for o in samples.output if isinstance(o, OutputGlob) or not o.dst.exists()
    ]:
        logger.warning(
            "One or more outputs were not copied "
            "(This should be done by a post-hook)"
        )
        for output_ in missing_outputs:
            logger.debug(f"Missing output: {output_}")


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
            executors_,
        ) = modules.load(root / "modules")

        _SAMPLE = data.Sample.with_mixins(sample_mixins)
        _SAMPLES = data.Samples.with_sample_class(_SAMPLE).with_mixins(samples_mixins)
        schema.properties.executor.properties.name.enum = [e.name for e in executors_]

        @cfg.options(schema)
        def inner(config: cfg.Config, **_: Any) -> None:
            """Run cellophane"""
            console_handler.setLevel(config.log_level)
            logger.debug(f"Found {len(hooks)} hooks")
            logger.debug(f"Found {len(runners)} runners")
            logger.debug(f"Found {len(sample_mixins)} sample mixins")
            logger.debug(f"Found {len(samples_mixins)} samples mixins")
            logger.debug(f"Found {len(executors_)} executors")

            executor_cls = next(e for e in executors_ if e.name == config.executor.name)
            executors.EXECUTOR = executor_cls
            logger.debug(f"Using {executor_cls.name} executor")

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
                    executor_cls=executor_cls,
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
