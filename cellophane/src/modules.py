"""Base classes and functions for cellophane modules."""
import inspect
import logging
import os
import sys
from copy import deepcopy
from graphlib import TopologicalSorter
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from signal import SIGTERM, signal
from typing import Callable, Literal, Optional

import psutil

from . import cfg, data, logs


def _cleanup(logger: logging.LoggerAdapter) -> Callable:
    def inner(*_):
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


def _is_instance_or_subclass(obj, cls):
    if isinstance(obj, type):
        return issubclass(obj, cls) and obj != cls
    else:
        return isinstance(obj, cls)


class Runner:
    """Base class for cellophane runners."""

    label: str
    individual_samples: bool
    link_by: Optional[str]
    func: Callable
    wait: bool
    main: Callable
    done: bool = False

    def __init__(
        self,
        func: Callable,
        label: Optional[str] = None,
        individual_samples: bool = False,
        link_by: Optional[str] = None,
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
        worker_state: dict,
        config: cfg.Config,
        root: Path,
    ) -> None:
        logger = logs.get_labeled_adapter(self.label)

        signal(SIGTERM, _cleanup(logger))
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
        sys.stderr = open(os.devnull, "w", encoding="utf-8")

        outdir = config.outdir / config.get("outprefix", config.timestamp) / self.label
        if self.individual_samples:
            outdir /= worker_state["samples"][0].id

        try:
            match self.main(
                samples=deepcopy(worker_state["samples"]),
                config=config,
                timestamp=config.timestamp,
                label=self.label,
                logger=logger,
                root=root,
                outdir=outdir,
            ):
                case None:
                    logger.debug(f"Runner {self.label} did not return any samples")
                    for sample in worker_state["samples"]:
                        sample.done = True

                case returned if isinstance(returned, data.Samples):
                    worker_state["samples"] = returned
                    for sample in worker_state["samples"]:
                        sample.done = True if sample.done is None else sample.done

                case returned:
                    logger.warning(f"Unexpected return type {type(returned)}")

        except Exception as exc:
            logger.critical(exc, exc_info=config.log_level == "DEBUG")

        finally:
            if n_complete := len(worker_state["samples"].complete):
                logger.debug(f"Completed {n_complete} samples")
            if n_failed := len(worker_state["samples"].failed):
                logger.warning(f"Failed for {n_failed} samples")


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
        logger = logs.get_labeled_adapter(self.label)
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
            case _:
                logger.warning(f"Unexpected return type {type(returned)}")
                _ret = samples

        return _ret


def resolve_hook_dependencies(
    hooks: list[Hook],
) -> list[Hook]:
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
    hooks: list[Hook] = []
    runners: list[Runner] = []
    sample_mixins: list[type[data.Sample]] = []
    samples_mixins: list[type[data.Samples]] = []

    for file in [*path.glob("*.py"), *path.glob("*/__init__.py")]:
        base = file.stem if file.stem != "__init__" else file.parent.name
        name = f"_cellophane_module_{base}"
        spec = spec_from_file_location(name, file)
        original_handlers = logging.root.handlers.copy()

        # FIXME: Does removing this check break anything?
        # It fixes Mypy errors, but it is never reached in tests
        # if spec is None or spec.loader is None:
        #     continue

        try:
            module = module_from_spec(spec)  # type: ignore[arg-type]
            sys.modules[name] = module
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except ImportError:
            continue

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
    label: Optional[str] = None,
    before: list[str] | Literal["all"] = [],
    after: list[str] | Literal["all"] = [],
) -> Callable:

    """Decorator for hooks that will run before all runners."""

    def wrapper(func):
        return Hook(
            label=label,
            func=func,
            when="pre",
            condition="always",
            before=before,
            after=after,
        )

    return wrapper


def post_hook(
    label: Optional[str] = None,
    condition: Literal["always", "complete", "failed"] = "always",
    before: list[str] | Literal["all"] = [],
    after: list[str] | Literal["all"] = [],
):
    """Decorator for hooks that will run after all runners."""
    if condition not in ["always", "complete", "failed"]:
        raise ValueError(f"{condition=} must be one of 'always', 'complete', 'failed'")

    def wrapper(func):
        return Hook(
            label=label,
            func=func,
            when="post",
            condition=condition,
            before=before,
            after=after,
        )

    return wrapper


def runner(
    label: Optional[str] = None,
    individual_samples: bool = False,
    link_by: Optional[str] = None,
):
    """Decorator for runners."""

    def wrapper(func):
        return Runner(
            label=label,
            func=func,
            individual_samples=individual_samples,
            link_by=link_by,
        )

    return wrapper
