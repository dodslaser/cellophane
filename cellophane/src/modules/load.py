import logging
import sys
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

from cellophane.src import data, executors, util

from .hook import Hook, resolve_dependencies
from .runner_ import Runner


def load(
    path: Path,
) -> tuple[
    list[Hook],
    list[Runner],
    list[type[data.Sample]],
    list[type[data.Samples]],
    list[type[executors.Executor]],
]:
    """
    Loads module(s) from the specified path and returns the hooks, runners,
    sample mixins, and samples mixins found within.

    Args:
        path (Path): The path to the directory containing the modules.

    Returns:
        tuple[
            list[Hook],
            list[Runner],
            list[type[data.Sample]],
            list[type[data.Samples]],
        ]: A tuple containing the lists of hooks, runners, sample mixins,
            and samples mixins.
    """

    hooks: list[Hook] = []
    runners: list[Runner] = []
    sample_mixins: list[type[data.Sample]] = []
    samples_mixins: list[type[data.Samples]] = []
    executors_: list[type[executors.Executor]] = [executors.SubprocesExecutor]

    for file in [*path.glob("*.py"), *path.glob("*/__init__.py")]:
        base = file.stem if file.stem != "__init__" else file.parent.name
        name = f"_cellophane_module_{base}"
        spec = spec_from_file_location(name, file)
        original_handlers = logging.root.handlers.copy()

        try:
            module = module_from_spec(spec)  # type: ignore[arg-type]
            sys.modules[name] = module
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except Exception as exc:
            raise ImportError(f"Unable to import module '{base}': {exc}") from exc

        # Reset logging handlers to avoid duplicate messages
        for handler in {*logging.root.handlers} ^ {*original_handlers}:
            handler.close()
            logging.root.removeHandler(handler)

        for obj in [getattr(module, a) for a in dir(module)]:
            if util.is_instance_or_subclass(obj, Hook):
                hooks.append(obj)
            elif util.is_instance_or_subclass(obj, data.Sample):
                sample_mixins.append(obj)
            elif util.is_instance_or_subclass(obj, data.Samples):
                samples_mixins.append(obj)
            elif util.is_instance_or_subclass(obj, Runner):
                runners.append(obj)
            elif util.is_instance_or_subclass(obj, executors.Executor):
                executors_.append(obj)
    try:
        hooks = resolve_dependencies(hooks)
    except Exception as exc:  # pylint: disable=broad-except
        raise ImportError(f"Unable to resolve hook dependencies: {exc}") from exc

    return hooks, runners, sample_mixins, samples_mixins, executors_
