"""Main cellophane entry point wrapper."""

import time
from importlib.metadata import version
from importlib.util import find_spec
from logging import LoggerAdapter, getLogger
from multiprocessing import Queue
from pathlib import Path
from typing import Any

import rich_click as click
from humanfriendly import format_timespan
from ruamel.yaml.error import YAMLError

from cellophane.src import executors
from cellophane.src.cfg import Config, Schema, with_options
from cellophane.src.cleanup import Cleaner
from cellophane.src.data import OutputGlob, Sample, Samples
from cellophane.src.executors import Executor
from cellophane.src.logs import (
    ExternalFilter,
    handle_warnings,
    setup_console_handler,
    setup_file_handler,
    start_logging_queue_listener,
)
from cellophane.src.modules import Hook, Runner, load, run_hooks, start_runners

spec = find_spec("cellophane")
CELLOPHANE_ROOT = Path(spec.origin).parent  # type: ignore[union-attr, arg-type]
CELLOPHANE_VERSION = version("cellophane")


def cellophane(label: str, root: Path) -> click.Command:
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
    click.rich_click.REQUIRED_LONG_STRING = "(REQUIRED)"
    click.rich_click.DEFAULT_STRING = "{}"
    click.rich_click.STYLE_OPTION_DEFAULT = "green"
    external_filter = ExternalFilter((CELLOPHANE_ROOT, root))
    console_handler = setup_console_handler(filters=(external_filter,))
    logger = LoggerAdapter(getLogger(), {"label": label})

    try:
        schema = Schema.from_file(
            path=[
                CELLOPHANE_ROOT / "schema.base.yaml",
                root / "schema.yaml",
                *(root / "modules").glob("*/schema.yaml"),
            ],
        )
    except YAMLError as exc:
        logger.critical(f"Failed to load schema: {exc!r}")
        raise SystemExit(1) from exc

    try:
        (
            hooks,
            runners,
            sample_mixins,
            samples_mixins,
            executors_,
        ) = load(root)

        _SAMPLE = Sample.with_mixins(sample_mixins)
        _SAMPLES = Samples.with_sample_class(_SAMPLE).with_mixins(samples_mixins)
        schema.properties.executor.properties.name.enum = [e.name for e in executors_]

        @with_options(schema)
        def inner(config: Config, **_: Any) -> None:
            """Run cellophane"""
            start_time = time.time()
            timestamp = time.strftime(
                "%Y%m%d_%H%M%S",
                time.localtime(start_time),
            )
            config.tag = config.tag or timestamp
            handle_warnings()
            console_handler.setLevel(config.log.level)
            file_handler = setup_file_handler(
                config.logdir / f"{label}.{config.tag}.log",
                logger.logger,
                filters=(external_filter,),
            )

            if config.log.external:
                file_handler.removeFilter(external_filter)
                console_handler.removeFilter(external_filter)

            log_queue, log_listener = start_logging_queue_listener()

            logger.debug(f"Found {len(hooks)} hooks")
            logger.debug(f"Found {len(runners)} runners")
            logger.debug(f"Found {len(sample_mixins)} sample mixins")
            logger.debug(f"Found {len(samples_mixins)} samples mixins")
            logger.debug(f"Found {len(executors_)} executors")
            executor_cls = next(
                e for e in executors_ if e.name == config.executor.name
            )  # pragma: no cover
            # StopIteration is never raised as config.executor.name has already been validated
            executors.EXECUTOR = executor_cls
            logger.debug(f"Using {executor_cls.name} executor")

            config.analysis = label  # type: ignore[attr-defined]

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
                    timestamp=timestamp,
                )

            except Exception as exc:
                logger.critical(f"Unhandled exception: {exc!r}", exc_info=True)
                log_listener.stop()
                raise SystemExit(1) from exc

            time_elapsed = format_timespan(time.time() - start_time)
            logger.info(f"Execution complete in {time_elapsed}")
            log_listener.stop()

    except Exception as exc:
        logger.critical(exc)
        raise SystemExit(1) from exc

    return inner


def _main(
    hooks: list[Hook],
    runners: list[Runner],
    samples_class: type[Samples],
    logger: LoggerAdapter,
    log_queue: Queue,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    timestamp: str,
) -> None:
    """Run cellophane"""
    # Load samples from file, or create empty samples object
    if "samples_file" in config:
        logger.debug(f"Loading samples from {config.samples_file}")
        samples = samples_class.from_file(config.samples_file)
    else:
        logger.debug("No samples file specified, creating empty samples object")
        samples = samples_class()

    cleaner  = Cleaner(root=config.workdir / config.tag)
    cleaner.register(config.workdir / config.tag)

    # Run pre-hooks
    samples = run_hooks(
        hooks,
        when="pre",
        samples=samples,
        log_queue=log_queue,
        config=config,
        root=root,
        executor_cls=executor_cls,
        timestamp=timestamp,
        cleaner=cleaner,
    )

    # Validate sample files
    for sample in samples:
        if sample not in samples.with_files:
            logger.warning(f"Sample {sample} will be skipped as it has no files")

    if not samples.with_files:
        logger.info("No samples to process")
        raise SystemExit(0)

    samples = start_runners(
        runners=runners,
        samples=samples.with_files,
        logger=logger,
        log_queue=log_queue,
        config=config,
        root=root,
        executor_cls=executor_cls,
        timestamp=timestamp,
        cleaner=cleaner,
    )
    samples = run_hooks(
        hooks,
        when="post",
        samples=samples,
        config=config,
        log_queue=log_queue,
        root=root,
        executor_cls=executor_cls,
        timestamp=timestamp,
        cleaner=cleaner,
    )
    if samples.failed:
        cleaner.unregister(config.workdir / config.tag)
    cleaner.clean(logger=logger)
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
