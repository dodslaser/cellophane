"""Cellophane: A library for writing modular wrappers"""
import sys
import inspect
import logging
import multiprocessing as mp
from copy import deepcopy
from importlib.util import spec_from_file_location, module_from_spec
from pathlib import Path
from typing import Any, Optional, Type, Callable, Iterator
import time

import rich_click as click
import yaml
from jsonschema.exceptions import ValidationError
from humanfriendly import format_timespan
from graphlib import TopologicalSorter, CycleError

from .src import cfg, data, logs, modules, util, sge

_MP_MANAGER = mp.Manager()
_LOG_QUEUE = logs.get_log_queue(_MP_MANAGER)
_OUTPUT_QUEUE: mp.Queue = mp.Queue()
_PROCS: dict[str, modules.Runner] = {}
_STARTTIME = time.time()
_TIMESTAMP: str = time.strftime("%Y%m%d_%H%M%S", time.localtime(_STARTTIME))
CELLOPHANE_ROOT = Path(__file__).parent

click.rich_click.DEFAULT_STRING = "[{}]"

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


def _convert_mapping(ctx, param, value):
    if isinstance(value, dict):
        return value
    try:
        return {k: v for k, v in [kv.split("=") for kv in value]}
    except:
        raise click.BadParameter("format must be 'key=value'")


def _load_modules(path: Path) -> Iterator[tuple[str, modules.Hook | modules.Runner |data.Mixin]]:
    for file in [*path.glob("*.py"), *path.glob("*/__init__.py")]:
        base = file.stem if file.stem != "__init__" else file.parent.name
        name = f"_cellophane_module_{base}"
        spec = spec_from_file_location(name, file)
        original_handlers = logging.root.handlers.copy()
        if spec is not None:
            module = module_from_spec(spec)
            if spec.loader is not None:
                try:
                    sys.modules[name] = module
                    spec.loader.exec_module(module)
                except ImportError:
                    pass
                else:
                    # Reset logging handlers to avoid duplicate messages
                    for handler in logging.root.handlers:
                        if handler not in original_handlers:
                            handler.close()
                            logging.root.removeHandler(handler)
                    
                    for obj in [getattr(module, a) for a in dir(module)]:
                        if (
                            isinstance(obj, modules.Hook) or
                            (isinstance(obj, type) and issubclass(obj, data.Mixin)) or
                            (isinstance(obj, type) and issubclass(obj, modules.Runner)) and
                            obj not in (modules.Runner, data.Mixin) and obj.__module__ == name
                        ):
                            yield base, obj

def _resolve_hook_dependencies(hooks: list[modules.Hook]) -> list[modules.Hook]:
    deps = {
        name: {
            *[d for h in hooks if h.name == name for d in h.after],
            *[h.name for h in hooks if name in h.before],
        }
        for name in {
            *[n for h in hooks for n in h.before + h.after],
            *[h.name for h in hooks],
        }
    }

    order = [*TopologicalSorter(deps).static_order()]
    return [*sorted(hooks, key=lambda h: order.index(h.name))]


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

    hooks: list[modules.Hook] = []
    runners: list[Type[modules.Runner]] = []
    mixins: list[Type[data.Mixin]] = []
    for base, obj in _load_modules(modules_path):
        if isinstance(obj, modules.Hook):
            logger.debug(f"Found hook {hook.name} ({base})")
            hooks.append(obj)
        elif issubclass(obj, data.Mixin):
            logger.debug(f"Found mixin {mixin.__name__} ({base})")
            mixins.append(obj)
        elif issubclass(obj, modules.Runner) and obj not in (modules.Runner, data.Mixin):
            logger.debug(f"Found runner {runner.name} ({base})")
            runners.append(obj)

    hooks = _resolve_hook_dependencies(hooks)

    for mixin in [m for m in mixins]:
        logger.debug(f"Adding {mixin.__name__} mixin to samples")
        data.Samples.__bases__ = (*data.Samples.__bases__, mixin)
        if mixin.sample_mixin is not None:
            data.Sample.__bases__ = (*data.Sample.__bases__, mixin.sample_mixin)

    for hook in [h for h in hooks if h.when == "pre"]:
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

    result_samples = data.Samples()
    if samples:
        for runner in runners:
            for _samples in (
                samples.split() if runner.individual_samples else [samples]
            ):
                proc = runner(
                    samples=_samples,
                    config=config,
                    timestamp=_TIMESTAMP,
                    output_queue=_OUTPUT_QUEUE,
                    log_queue=_LOG_QUEUE,
                    log_level=config.log_level,
                    root=root,
                )
                _PROCS[proc.id] = proc

        for proc in _PROCS.values():
            proc.start()

        while not all(proc.done for proc in _PROCS.values()):
            result, pid = _OUTPUT_QUEUE.get()
            result_samples += result
            logger.debug(f"Received result from {_PROCS[pid].name}")
            _PROCS[pid].join()
            _PROCS[pid].done = True

    failed_samples = data.Samples(
        sample for sample in result_samples if not sample.complete
    )
    complete_samples = data.Samples(
        sample
        for sid in {s.id for s in result_samples}
        for sample in result_samples
        if sample.id == sid
        if all(s.complete for s in result_samples if s.id == sid)
    )
    partial_samples = data.Samples(
        sample
        for sample in result_samples
        if sample.complete and sample.id not in [s.id for s in complete_samples]
    )

    for hook in [h for h in hooks if h.when == "post"]:

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
    for runner in runners:
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

    logger.info(f"Execution complete in {format_timespan(time.time() - _STARTTIME)}")


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
            return _main(
                config=_config,
                logger=logger,
                modules_path=_modules_path,
                root=root,
            )
        except CycleError as exception:
            logger.error(f"Circular dependency in hooks: {exception}")
            raise SystemExit(1)

        except KeyboardInterrupt:
            logger.critical("Received SIGINT, shutting down...")
            _cleanup(logger)
        
        except ValidationError as exception:
            _config = cfg.Config(config_path, schema, validate=False, **kwargs)
            for error in schema.iter_errors(_config):
                logger.critical(f"Invalid configuration: {error.message}")
            raise SystemExit(1) from exception

        except Exception as exception:
            logger.critical(
                f"Unhandled exception: {exception}",
                exc_info=_config.log_level == "DEBUG",
                stacklevel=2,
            )
            _cleanup(logger)

    for flag, _, default, description, secret, _type in schema.flags:
        inner = click.option(
            f"--{flag}",
            type=str if _type in (list, dict) else _type,
            callback=_convert_mapping if _type == dict else None,
            is_flag=_type == bool,
            multiple=_type in (list, dict),
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