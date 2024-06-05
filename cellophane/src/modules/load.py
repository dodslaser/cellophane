"""Module loader for cellophane modules."""

from site import addsitedir
from importlib import import_module
from pathlib import Path

from cellophane.src.data import Sample, Samples
from cellophane.src.executors import Executor, SubprocessExecutor
from cellophane.src.util import is_instance_or_subclass, freeze_logs

from .hook import Hook, resolve_dependencies
from .runner_ import Runner


def load(root: Path) -> tuple[
    list[Hook],
    list[Runner],
    list[type[Sample]],
    list[type[Samples]],
    list[type[Executor]],
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
    sample_mixins: list[type[Sample]] = []
    samples_mixins: list[type[Samples]] = []
    executors_: list[type[Executor]] = [SubprocessExecutor]
    error = None

    addsitedir(str(root))
    with freeze_logs():
        for file in [*(root / "modules").glob("*.py"), *(root / "modules").glob("*/__init__.py")]:
            if (base := file.stem if file.stem != "__init__" else file.parent.name) == "modules":
                continue

            try:
                module = import_module(f".{base}", "modules")
            except Exception as exc:
                error = f"Unable to import module '{base}': {exc!r}"
                break

            for obj in [getattr(module, a) for a in dir(module)]:
                if is_instance_or_subclass(obj, Hook):
                    hooks.append(obj)
                elif is_instance_or_subclass(obj, Sample):
                    sample_mixins.append(obj)
                elif is_instance_or_subclass(obj, Samples):
                    samples_mixins.append(obj)
                elif is_instance_or_subclass(obj, Runner):
                    runners.append(obj)
                elif is_instance_or_subclass(obj, Executor):
                    executors_.append(obj)

    try:
        hooks = resolve_dependencies(hooks)
    except Exception as exc:  # pylint: disable=broad-except
        error = f"Unable to resolve hook dependencies: {exc!r}"

    if error is not None:
        raise ImportError(error)

    return hooks, runners, sample_mixins, samples_mixins, executors_
