"""Cellophane: A library for writing modular wrappers"""
import sys
import inspect
import logging
import multiprocessing as mp
from copy import deepcopy
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Any, Optional, Type, Callable

import rich_click as click
import yaml
from jsonschema.exceptions import ValidationError

from .src import cfg, data, logs, modules, util, sge

_MP_MANAGER = mp.Manager()
_LOG_QUEUE = logs.get_log_queue(_MP_MANAGER)

_RUNNERS: list[Type[modules.Runner]] = []
_PROCS: list[modules.Runner] = []
_HOOKS: list[modules.Hook] = []

CELLOPHANE_ROOT = Path(__file__).parent


def _main(
    logger: logging.LoggerAdapter,
    config: cfg.Config,
    modules_path: Path,
    root: Path,
) -> None:
    """Run cellophane"""
    logger.setLevel(config.log_level)
    if config.samples_file:
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
                            logger.debug(f"Found hook {hook.label} ({name})")
                            _HOOKS.append(hook)
                        case type() as runner if (
                            issubclass(runner, modules.Runner)
                            and runner != modules.Runner
                        ):
                            logger.debug(f"Found runner {runner.label} ({name})")
                            _RUNNERS.append(obj)
                        case _:
                            pass

    _HOOKS.sort(key=lambda h: h.priority)

    try:
        for hook in [h for h in _HOOKS if h.when == "pre"]:
            logger.info(f"Running pre-hook {hook.label}")
            result = hook(
                config=config,
                samples=samples,
                log_queue=_LOG_QUEUE,
                log_level=config.log_level,
                root=root,
            )

            if issubclass(type(result), data.Samples):
                samples = result

        if samples:
            for runner in _RUNNERS:
                for _samples in (
                    [data.Samples([s]) for s in samples]
                    if runner.individual_samples
                    else [samples]
                ):
                    proc = runner(
                        config=deepcopy(config),
                        samples=deepcopy(_samples),
                        log_queue=_LOG_QUEUE,
                        log_level=config.log_level,
                        output=mp.Queue(),
                        root=root,
                    )
                    _PROCS.append(proc)

            for proc in _PROCS:
                logger.info(f"Starting {proc.label} for {len(samples)} samples")
                proc.start()
            for proc in _PROCS:
                proc.join()

    except KeyboardInterrupt:
        logger.critical("Received SIGINT, shutting down...")

    except Exception as exception:
        logger.critical(
            f"Unhandled exception: {exception}",
            exc_info=config.log_level == "DEBUG",
        )

    finally:
        results: dict[str, data.Samples] = {
            r.label: samples.__class__() for r in _RUNNERS
        }
        completed_samples: list[data.Sample] = []
        for proc in _PROCS:
            if proc.exitcode is None:
                logger.debug(f"Terminating {proc.label}")
                try:
                    proc.terminate()
                    proc.join()
                # Handle weird edge cases
                except Exception as exception:
                    logger.debug(f"Failed to terminate {proc.label}: {exception}")
                    pass

            if (runner_samples := proc.output.get_nowait()) is not None:
                for sample in runner_samples:
                    sample.complete = True
                results[proc.label].extend(runner_samples)
                completed_samples.extend(runner_samples)

        failed_samples = [
            s for s in samples if s.id not in [c.id for c in completed_samples]
        ]
        for sample in failed_samples:
            sample.complete = False

        for hook in [h for h in _HOOKS if h.when == "post"]:
            logger.info(f"Running post-hook {hook.label}")
            hook(
                config=config,
                samples=samples.__class__([*completed_samples, *failed_samples]),
                log_queue=_LOG_QUEUE,
                log_level=config.log_level,
                root=root,
            )

        for name, processed in results.items():
            if processed:
                _n_processed = sum(p.id in [s.id for s in samples] for p in processed)
                _n_failed = sum(s.id not in [p.id for p in processed] for s in samples)
                _n_extra = len(processed) - _n_processed
                if _n_processed:
                    logger.info(f"Runner {name} completed {_n_processed} samples")
                if _n_extra:
                    logger.info(f"Runner {name} introduced {_n_extra} extra samples")
                if _n_failed:
                    logger.info(f"Runner {name} failed for {_n_failed} samples")


def cellophane(
    label: str,
    wrapper_log: Optional[Path] = None,
    schema_path: Optional[Path] = None,
    modules_path: Optional[Path] = None,
) -> Callable:
    """Generate a cellophane CLI from a schema file"""
    _root = Path(inspect.stack()[1].filename).parent
    _wrapper_log = wrapper_log or _root / "pipeline.log"
    _schema_path = schema_path or _root / "schema.yaml"
    _modules_path = modules_path or _root / "modules"

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
    @click.option(
        "config_path",
        "--config",
        type=click.Path(exists=True),
        help="Path to config file",
    )
    def inner(config_path, logger, **kwargs) -> Any:
        try:
            _config = cfg.Config(config_path, schema, **kwargs)
        except ValidationError as exception:
            logger.critical(f"Invalid configuration: {exception.message}")
            raise SystemExit(1) from exception
        return _main(
            config=_config,
            logger=logger,
            modules_path=_modules_path,
            root=_root,
        )

    for flag, _, default, description, _type in schema.flags:
        inner = click.option(
            f"--{flag}",
            type=str if _type == list else _type,
            is_flag=_type == bool,
            multiple=_type == list,
            default=default,
            help=description,
            show_default=True,
        )(inner)

    return inner
