"""Cellophane: A library for writing modular wrappers"""
import sys
import inspect
import logging
import multiprocessing as mp
from copy import deepcopy
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Any, Optional, Type, Callable
import time

import rich_click as click
import yaml
from jsonschema.exceptions import ValidationError

from .src import cfg, data, logs, modules, util, sge

_MP_MANAGER = mp.Manager()
_LOG_QUEUE = logs.get_log_queue(_MP_MANAGER)
_RUNNERS: list[Type[modules.Runner]] = []
_PROCS: list[modules.Runner] = []
_HOOKS: list[modules.Hook] = []
_MIXINS: list[Type[data.Mixin]] = []
_TIMESTAMP: str = time.strftime("%Y%m%d_%H%M%S")
CELLOPHANE_ROOT = Path(__file__).parent

click.rich_click.DEFAULT_STRING = "[{}]"


def _main(
    logger: logging.LoggerAdapter,
    config: cfg.Config,
    modules_path: Path,
    root: Path,
) -> None:
    """Run cellophane"""
    logger.setLevel(config.log_level)
    if "samples_file" in config:
        samples = data.Samples.from_file(config.samples_file)
    else:
        samples = data.Samples()

    _log_handlers = logging.root.handlers.copy()
    for path in [*modules_path.glob("*.py"), *modules_path.glob("*/__init__.py")]:
        base = path.stem if path.stem != "__init__" else path.parent.name
        name = f"_cellophane_module_{base}"
        spec = spec_from_file_location(name, path)
        if spec is not None:
            try:
                module = module_from_spec(spec)
                if spec.loader is not None:
                    sys.modules[name] = module
                    spec.loader.exec_module(module)
                    # Reset logging handlers to avoid duplicate messages
                    for handler in logging.root.handlers:
                        if handler not in _log_handlers:
                            handler.close()
                            logging.root.removeHandler(handler)
            except ImportError as exception:
                logger.error(f"Failed to import module {name}: {exception}")
            else:
                for obj in [getattr(module, a) for a in dir(module)]:
                    match obj:
                        case modules.Hook() as hook:
                            logger.debug(f"Found hook {hook.name} ({base})")
                            _HOOKS.append(hook)

                        case type() as mixin if (
                            issubclass(mixin, data.Mixin)
                            and mixin != data.Mixin
                            and mixin.__module__ == name
                        ):
                            logger.debug(f"Found mixin {mixin.__name__} ({base})")
                            _MIXINS.append(mixin)

                        case type() as runner if (
                            issubclass(runner, modules.Runner)
                            and runner != modules.Runner
                        ):
                            logger.debug(f"Found runner {runner.name} ({base})")
                            _RUNNERS.append(obj)
                        case _:
                            pass

    _HOOKS.sort(key=lambda h: h.priority)

    try:
        for mixin in [m for m in _MIXINS]:
            logger.debug(f"Adding {mixin.__name__} mixin to samples")
            data.Samples.__bases__ = (*data.Samples.__bases__, mixin)
            if mixin.sample_mixin is not None:
                data.Sample.__bases__ = (*data.Sample.__bases__, mixin.sample_mixin)

        for hook in [h for h in _HOOKS if h.when == "pre"]:
            logger.debug(f"Running pre-hook {hook.label}")
            result = hook(
                samples=deepcopy(samples),
                config=config,
                timestamp=_TIMESTAMP,
                log_queue=_LOG_QUEUE,
                log_level=config.log_level,
                root=root,
            )

            if issubclass(type(result), data.Samples):
                samples = result

        for invalid_sample in samples.validate():
            logger.warning(f"Removed invalid sample {invalid_sample.id}")

        if samples:
            for runner in _RUNNERS:
                for _samples in (
                    samples.split() if runner.individual_samples else [samples]
                ):
                    proc = runner(
                        samples=_samples,
                        config=config,
                        timestamp=_TIMESTAMP,
                        log_queue=_LOG_QUEUE,
                        log_level=config.log_level,
                        output=mp.Queue(),
                        root=root,
                    )
                    _PROCS.append(proc)

            for proc in _PROCS:
                logger.debug(f"Starting {proc.label} for {len(samples)} samples")
                proc.start()
            for proc in _PROCS:
                proc.join()

    except KeyboardInterrupt:
        logger.critical("Received SIGINT, shutting down...")

    except Exception as exception:
        logger.critical(
            f"Unhandled exception: {exception}",
            exc_info=config.log_level == "DEBUG",
            stacklevel=2,
        )

    finally:
        result_samples = data.Samples()

        for proc in _PROCS:
            if proc.exitcode is None:
                logger.debug(f"Terminating {proc.label}")
                try:
                    proc.terminate()
                    proc.join()
                # Handle weird edge cases when terminating processes
                except Exception as exception:
                    logger.debug(f"Failed to terminate {proc.label}: {exception}")

            result_samples += proc.output.get()

        failed_samples = {sample for sample in result_samples if not sample.complete}
        complete_samples = {
            sample
            for sid in {s.id for s in result_samples}
            for sample in result_samples
            if sample.id == sid
            if all(s.complete for s in result_samples if s.id == sid)
        }
        partial_samples = {
            sample
            for sample in result_samples
            if sample.complete and sample.id not in [s.id for s in complete_samples]
        }

        for hook in [h for h in _HOOKS if h.when == "post"]:

            hook(
                samples=data.Samples(
                    [
                        *complete_samples,
                        *(partial_samples if hook.condition != "complete" else []),
                        *(failed_samples if hook.condition == "always" else []),
                    ]
                ),
                config=config,
                timestamp=_TIMESTAMP,
                log_queue=_LOG_QUEUE,
                log_level=config.log_level,
                root=root,
            )

        original_ids = [s.id for s in samples]
        for runner in _RUNNERS:
            n_completed = sum(
                s.runner == runner.label and s.id in original_ids
                for s in [*complete_samples, *partial_samples]
            )
            n_failed = sum(
                s.runner == runner.label and s.id in original_ids
                for s in failed_samples
            )
            n_extra = sum(
                s.id not in original_ids for s in [*complete_samples, *failed_samples]
            )
            if n_completed:
                logger.info(f"Runner {runner.label} completed {n_completed} samples")
            if n_extra:
                logger.info(f"Runner {runner.label} introduced {n_extra} extra samples")
            if n_failed:
                logger.warning(f"Runner {runner.label} failed for {n_failed} samples")


def cellophane(
    label: str,
    wrapper_log: Optional[Path] = None,
    schema_path: Optional[Path] = None,
    modules_path: Optional[Path] = None,
) -> Callable:
    """Generate a cellophane CLI from a schema file"""
    root = Path(inspect.stack()[1].filename).parent
    _wrapper_log = wrapper_log or root / "pipeline.log"
    _schema_path = schema_path or root / "schema.yaml"
    _modules_path = modules_path or root / "modules"

    with (
        open(CELLOPHANE_ROOT / "schema.base.yaml", encoding="utf-8") as base_handle,
        open(_schema_path, "r", encoding="utf-8") as custom_handle,
    ):
        base = yaml.safe_load(base_handle)
        custom = yaml.safe_load(custom_handle)

    for module_schema_path in _modules_path.glob("*/schema.yaml"):
        with open(module_schema_path, "r", encoding="utf-8") as module_handle:
            module = yaml.safe_load(module_handle)
            custom = util.merge_mappings(custom, module)

    merged = util.merge_mappings(custom, base)
    schema = cfg.Schema(merged)

    @click.command()
    @logs.handle_logging(
        label=label,
        level=logging.INFO,
        path=_wrapper_log,
        queue=_LOG_QUEUE,
        propagate_exceptions=False,
    )
    def inner(config_path, logger, **kwargs) -> Any:
        try:
            _config = cfg.Config(config_path, schema, **kwargs)
        except ValidationError as exception:
            _config = cfg.Config(config_path, schema, validate=False, **kwargs)
            for error in schema.iter_errors(_config):
                logger.critical(f"Invalid configuration: {error.message}")
            raise SystemExit(1) from exception
        return _main(
            config=_config,
            logger=logger,
            modules_path=_modules_path,
            root=root,
        )

    for flag, _, default, description, secret, _type in schema.flags:
        inner = click.option(
            f"--{flag}",
            type=str if _type == list else _type,
            is_flag=_type == bool,
            multiple=_type == list,
            default=default,
            help=description,
            show_default=not secret,
        )(inner)

    inner = click.option(
        "config_path",
        "--config",
        type=click.Path(exists=True),
        help="Path to config file",
        is_eager=True,
        callback=lambda ctx, _, value: cfg.set_defaults(ctx, value, schema),
    )(inner)

    return inner
