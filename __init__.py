"""Cellophane: A library for writing modular wrappers"""

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
    scripts_path: Path,
) -> None:
    """Run cellophane"""
    logger.setLevel(config.log_level)
    # FIXME: Make slims optional if a samples_file is provided
    if config.samples_file:
        samples = data.Samples.from_file(config.samples_file)
    else:
        samples = data.Samples()

    _log_handlers = logging.root.handlers.copy()
    for path in [*modules_path.glob("*.py"), *modules_path.glob("*/__init__.py")]:
        name = path.stem if path.stem != "__init__" else path.parent.name
        spec = spec_from_file_location(name, path)
        if spec is not None:
            try:
                module = module_from_spec(spec)
                if spec.loader is not None:
                    spec.loader.exec_module(module)
                    for handler in [h for h in logging.root.handlers if h not in _log_handlers]:
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
                        case type() as runner if issubclass(
                            runner, modules.Runner
                        ) and runner != modules.Runner:
                            logger.debug(f"Found runner {runner.label} ({name})")
                            _RUNNERS.append(obj)
                        case _:
                            pass

    _HOOKS.sort(key=lambda h: h.priority)

    for hook in [h for h in _HOOKS if h.when == "pre"]:
        logger.info(f"Running pre-hook {hook.label}")
        result = hook(
            config=config,
            samples=samples,
            log_queue=_LOG_QUEUE,
            log_level=config.log_level,
            scripts_path=scripts_path,
        )

        if issubclass(type(result), data.Samples):
            samples = result

    if samples and _RUNNERS:
        try:
            for runner in _RUNNERS:
                logger.info(f"Starting {runner.label} for {len(samples)} samples")

                for _samples in (
                    [data.Samples([s]) for s in samples]
                    if runner.individual_samples
                    else [samples]
                ):
                    proc = runner(
                        config=deepcopy(config),
                        kwargs={
                            "log_queue": _LOG_QUEUE,
                            "log_level": config.log_level,
                            "samples": deepcopy(_samples),
                            "scripts_path": scripts_path,
                        },
                    )
                    proc.start()
                    _PROCS.append(proc)

            for proc in _PROCS:
                proc.join()
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, shutting down...")
            for proc in _PROCS:
                logger.debug(f"Terminating {proc.label}")
                proc.terminate()
                proc.join()
        finally:
            for runner in _RUNNERS:
                n_ok = sum(p.exitcode == 0 for p in _PROCS if p.label == runner.label)
                n_fail = sum(p.exitcode != 0 for p in _PROCS if p.label == runner.label)

                if n_ok:
                    logger.info(f"{n_ok} {runner.label} jobs completed successfully")
                if n_fail:
                    logger.warning(f"{n_fail} {runner.label} jobs failed")

    for hook in [h for h in _HOOKS if h.when == "post"]:
        logger.info(f"Running post-hook {hook.label}")
        hook(
            config=config,
            samples=samples,
            log_queue=_LOG_QUEUE,
            log_level=config.log_level,
            scripts_path=scripts_path,
        )


def cellophane(
    label: str,
    wrapper_log: Optional[Path] = None,
    schema_path: Optional[Path] = None,
    modules_path: Optional[Path] = None,
    scripts_path: Optional[Path] = None,
) -> Callable:
    """Generate a cellophane CLI from a schema file"""
    wrapper_root = Path(inspect.stack()[1].filename).parent
    _wrapper_log = wrapper_log or wrapper_root / "pipeline.log"
    _schema_path = schema_path or wrapper_root / "schema.yaml"
    _modules_path = modules_path or wrapper_root / "modules"
    _scripts_path = scripts_path or wrapper_root / "scripts"

    with (
        open(CELLOPHANE_ROOT / "schema.base.yaml", encoding="utf-8") as base_handle,
        open(_schema_path, "r", encoding="utf-8") as custom_handle,
    ):
        base = yaml.safe_load(base_handle)
        custom = yaml.safe_load(custom_handle)

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
            scripts_path=_scripts_path,
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
