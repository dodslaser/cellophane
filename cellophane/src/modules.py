"""Base classes and functions for cellophane modules."""
import inspect
import logging
import sys
from copy import deepcopy
from graphlib import TopologicalSorter
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from signal import SIGTERM, signal
from typing import Any, Callable, Literal

import psutil
from cloudpickle import dumps, loads

from . import cfg, data


def _cleanup(logger: logging.LoggerAdapter) -> Callable:
    def inner(*args: Any) -> None:
        del args  # Unused
        for proc in psutil.Process().children(recursive=True):
            try:
                logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
                proc.terminate()
                proc.wait(10)
            except psutil.TimeoutExpired:
                logger.warning(
                    f"Killing unresponsive process {proc.name()} ({proc.pid})"
                )
                proc.kill()
                proc.wait()
        raise SystemExit(1)

    return inner


def _is_instance_or_subclass(obj: Any, cls: type) -> bool:
    if isinstance(obj, type):
        return issubclass(obj, cls) and obj != cls
    else:
        return isinstance(obj, cls)


class Runner:
    """
    A runner for executing a function as a job.

    Args:
        func (Callable): The function to be executed as a job.
        label (str | None): The label for the runner.
            Defaults to the name of the function.
        individual_samples (bool): Whether to process samples individually.
            Defaults to False.
        link_by (str | None): The attribute to link samples by. Defaults to None.
    """

    label: str
    individual_samples: bool
    link_by: str | None
    func: Callable
    wait: bool
    main: Callable
    done: bool = False

    def __init__(
        self,
        func: Callable,
        label: str | None = None,
        individual_samples: bool = False,
        link_by: str | None = None,
    ) -> None:
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__module__ = func.__module__
        self.name = func.__name__
        self.label = label or func.__name__
        self.main = staticmethod(func)
        self.label = label or self.__name__
        self.individual_samples = individual_samples
        self.link_by = link_by
        super().__init_subclass__()

    def __call__(
        self,
        config: cfg.Config,
        root: Path,
        root_logger: logging.Logger,
        samples_pickle: str,
    ) -> None:
        samples = loads(samples_pickle)
        logger = logging.LoggerAdapter(root_logger, {"label": self.label})

        signal(SIGTERM, _cleanup(logger))
        outdir = config.outdir / config.get("outprefix", config.timestamp) / self.label
        if self.individual_samples:
            outdir /= samples[0].id
        try:
            match self.main(
                samples=deepcopy(samples),
                config=config,
                timestamp=config.timestamp,
                label=self.label,
                logger=logger,
                root=root,
                outdir=outdir,
            ):
                case None:
                    logger.debug("Runner did not return any samples")
                    for sample in samples:
                        sample.done = True

                case returned if isinstance(returned, data.Samples):
                    samples = returned
                    for sample in samples:
                        sample.done = True if sample.done is None else sample.done

                case returned:
                    logger.warning(f"Unexpected return type {type(returned)}")

        except Exception as exc:  # pylint: disable=broad-except
            logger.critical(
                f"Unhandled Exception: {repr(exc)}",
                exc_info=config.log_level == "DEBUG",
            )

        finally:
            if n_complete := len(samples.complete):
                logger.debug(f"Completed {n_complete} samples")
            if n_failed := len(samples.failed):
                logger.warning(f"Failed for {n_failed} samples")

        return dumps(samples)


class Hook:
    """Base class for cellophane pre/post-hooks."""

    name: str
    label: str
    func: Callable
    when: Literal["pre", "post"]
    condition: Literal["always", "complete", "failed"]
    before: list[str]
    after: list[str]

    def __init__(
        self,
        func: Callable,
        when: Literal["pre", "post"],
        label: str | None = None,
        condition: Literal["always", "complete", "failed"] = "always",
        before: Literal["all"] | list[str] | None = None,
        after: Literal["all"] | list[str] | None = None,
    ) -> None:
        before = before or []
        after = after or []
        match before, after:
            case "all", list(after):
                self.before = ["before_all"]
                self.after = after
            case list(before), "all":
                self.before = before
                self.after = ["after_all"]
            case list(before), list(after):
                self.before = [*before, "after_all"]
                self.after = [*after, "before_all"]
            case _:
                raise ValueError(f"{func.__name__}: {before=}, {after=}")
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__module__ = func.__module__
        self.name = func.__name__
        self.label = label or func.__name__
        self.condition = condition
        self.func = staticmethod(func)
        self.when = when

    def __call__(
        self,
        samples: data.Samples,
        config: cfg.Config,
        root: Path,
    ) -> data.Samples:
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": self.label})
        logger.debug(f"Running {self.label} hook")

        match self.func(
            samples=samples,
            config=config,
            timestamp=config.timestamp,
            logger=logger,
            root=root,
            outdir=config.outdir / config.get("outprefix", config.timestamp),
        ):
            case returned if isinstance(returned, data.Samples):
                _ret = returned
            case None:
                logger.debug("Hook did not return any samples")
                _ret = samples
            case _:
                logger.warning(f"Unexpected return type {type(returned)}")
                _ret = samples

        return _ret


def resolve_hook_dependencies(
    hooks: list[Hook],
) -> list[Hook]:
    """
    Resolves hook dependencies and returns the hooks in the resolved order.
    Uses a topological sort to resolve dependencies. If the order of two hooks
    cannot be determined, the order is not guaranteed.

    # FIXME: It should be possible to determine the order of all hooks

    Args:
        hooks (list[Hook]): The list of hooks to resolve.

    Returns:
        list[Hook]: The hooks in the resolved order.
    """

    deps = {
        name: {
            *[d for h in hooks if h.__name__ == name for d in h.after],
            *[h.__name__ for h in hooks if name in h.before],
        }
        for name in {
            *[n for h in hooks for n in h.before + h.after],
            *[h.__name__ for h in hooks],
        }
    }

    order = [*TopologicalSorter(deps).static_order()]
    return [*sorted(hooks, key=lambda h: order.index(h.__name__))]


def load(
    path: Path,
) -> tuple[
    list[Hook],
    list[Runner],
    list[type[data.Sample]],
    list[type[data.Samples]],
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
            if inspect.getmodule(obj) != module:
                continue
            elif _is_instance_or_subclass(obj, Hook):
                hooks.append(obj)
            elif _is_instance_or_subclass(obj, data.Sample):
                sample_mixins.append(obj)
            elif _is_instance_or_subclass(obj, data.Samples):
                samples_mixins.append(obj)
            elif _is_instance_or_subclass(obj, Runner):
                runners.append(obj)

    return hooks, runners, sample_mixins, samples_mixins


def pre_hook(
    label: str | None = None,
    before: list[str] | Literal["all"] | None = None,
    after: list[str] | Literal["all"] | None = None,
) -> Callable:
    """
    Decorator for creating a pre-hook.

    Args:
        label (str | None): The label for the pre-hook. Defaults to None.
        before (list[str] | Literal["all"] | None): List of pre-hooks guaranteed to
            execute after the resulting pre-hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of pre-hooks guaratneed to
            execute before the resulting pre-hook. Defaults to an empty list.

    Returns:
        Callable: The decorator function.
    """

    def wrapper(func: Callable) -> Hook:
        return Hook(
            label=label,
            func=func,
            when="pre",
            condition="always",
            before=before or [],
            after=after or [],
        )

    return wrapper


def post_hook(
    label: str | None = None,
    condition: Literal["always", "complete", "failed"] = "always",
    before: list[str] | Literal["all"] | None = None,
    after: list[str] | Literal["all"] | None = None,
) -> Callable:
    """
    Decorator for creating a post-hook.

    Args:
        label (str | None): The label for the pre-hook. Defaults to None.
        condition (Literal["always", "complete", "failed"]): The condition for
            the post-hook to execute.
            - "always": The post-hook will always execute.
            - "complete": The post-hook will recieve only completed samples.
            - "failed": The post-hook will recieve only failed samples.
            Defaults to "always".
        before (list[str] | Literal["all"] | None): List of post-hooks guaranteed to
            execute after the resulting pre-hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of post-hooks guaratneed to
            execute before the resulting pre-hook. Defaults to an empty list.

    Returns:
        Callable: The decorator function.
    """
    if condition not in ["always", "complete", "failed"]:
        raise ValueError(f"{condition=} must be one of 'always', 'complete', 'failed'")

    def wrapper(func: Callable) -> Hook:
        return Hook(
            label=label,
            func=func,
            when="post",
            condition=condition,
            before=before or [],
            after=after or [],
        )

    return wrapper


def runner(
    label: str | None = None,
    individual_samples: bool = False,
    link_by: str | None = None,
) -> Callable:
    """
    Decorator for creating a runner.

    Args:
        label (str | None): The label for the runner. Defaults to None.
        individual_samples (bool): Whether to process samples individually.
            Defaults to False.
        link_by (str | None): The attribute to link samples by. Defaults to None.

    Returns:
        Callable: The decorator function.
    """

    def wrapper(func: Callable) -> Runner:
        return Runner(
            label=label,
            func=func,
            individual_samples=individual_samples,
            link_by=link_by,
        )

    return wrapper
