"""Cellophane: A library for writing modular wrappers"""
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal, Sequence

import rich_click as click
from cloudpickle import dumps, loads
from humanfriendly import format_timespan
from mpire import WorkerPool
from ruamel.yaml.scanner import ScannerError

from .src import cfg, data, logs, modules, sge, util  # pylint: disable=unused-import

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
                _samples = samples.complete
            case "failed":
                _samples = samples.failed
            case "always":
                _samples = samples

        if _samples or when == "pre":
            _samples = hook(samples=_samples, **kwargs)

    return _samples


def _start_runners(
    runners: Sequence[modules.Runner],
    samples: data.Samples,
    logger: logging.LoggerAdapter,
    **kwargs: Any,
) -> data.Samples:
    if not runners:
        logger.warning("No runners to execute")
        return samples

    with WorkerPool(use_dill=True, daemon=False, start_method="fork") as pool:
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
                    kwargs=kwargs
                    | {
                        "samples_pickle": dumps(_samples),
                        "root_logger": logging.getLogger(),
                    },
                )
                results.append(result)

            pool.stop_and_join(keep_alive=True)

            return samples.__class__([s for r in results for s in loads(r.get())])

        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling runners to shut down...")
            pool.terminate()
            return samples

        except Exception as exception:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception in runner: {exception}")
            pool.terminate()
            return samples


def _main(
    hooks: list[modules.Hook],
    runners: list[modules.Runner],
    samples_class: type[data.Samples],
    logger: logging.LoggerAdapter,
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

    samples = _start_runners(runners, samples, logger, **common_kwargs)
    samples = _run_hooks(hooks, "post", samples, **common_kwargs)


def _add_config_defaults(
    ctx: click.Context,
    config_path: str,
    schema: cfg.Schema,
    logger: logging.LoggerAdapter,
) -> str:
    if config_path is not None:
        try:
            _config = cfg.Config.from_file(
                schema=schema,
                path=config_path,
                validate=False,
            )

        except Exception as exception:
            logger.critical(f"Failed to load config: {exception}")
            raise SystemExit(1) from exception

        else:
            ctx.default_map = {}
            for flag in _config.flags:
                ctx.default_map |= {flag.flag: flag.default}
                param = next(p for p in ctx.command.params if p.name == flag.flag)
                param.required = flag.required and not flag.default

    return config_path


def cellophane(
    label: str,
    root: Path,
) -> click.BaseCommand:
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
    except Exception as exc:
        logger.error(exc)
        raise SystemExit(1) from exc

    _SAMPLE = data.Sample.with_mixins(sample_mixins)
    _SAMPLES = data.Samples.with_sample_class(_SAMPLE).with_mixins(samples_mixins)

    @schema.add_options
    @click.command()
    @click.option(
        "config_path",
        "--config",
        type=click.Path(exists=True),
        help="Path to config file",
        is_eager=True,
        callback=lambda ctx, _, value: _add_config_defaults(ctx, value, schema, logger),
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
    def inner(log_level: str, outprefix: str, **kwargs: Any) -> None:
        """Run cellophane"""
        start_time = time.time()
        console_handler.setLevel(log_level)
        logger.debug(f"Found {len(hooks)} hooks")
        logger.debug(f"Found {len(runners)} runners")
        logger.debug(f"Found {len(sample_mixins)} sample mixins")
        logger.debug(f"Found {len(samples_mixins)} samples mixins")

        config = cfg.Config(
            schema=schema,
            **{k: v for k, v in kwargs.items() if v is not None},
        )
        config.analysis = label  # type: ignore[attr-defined]
        config.log_level = log_level  # type: ignore[attr-defined]
        config.timestamp = time.strftime(  # type: ignore[attr-defined]
            "%Y%m%d_%H%M%S",
            time.localtime(start_time),
        )
        config.outprefix = outprefix or config.timestamp  # type: ignore[attr-defined]
        logs.add_file_handler(
            logger, path=config.logdir / f"{label}.{config.outprefix}.log"
        )
        try:
            _main(
                hooks=hooks,
                runners=runners,
                samples_class=_SAMPLES,
                config=config,
                logger=logger,
                root=root,
            )
        except ImportError as exc:
            logger.critical(exc)
            raise SystemExit(1) from exc

        except Exception as exception:
            logger.critical(f"Unhandled exception: {exception}", exc_info=True)
            raise SystemExit(1) from exception

        else:
            time_elapsed = format_timespan(time.time() - start_time)
            logger.info(f"Execution complete in {time_elapsed}")
            raise SystemExit(0)

    return inner
