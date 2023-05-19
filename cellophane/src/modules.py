"""Base classes and functions for cellophane modules."""

import multiprocessing as mp
import os
import sys
import logging
from signal import SIGTERM, signal
from typing import Callable, Optional, ClassVar, Literal, Iterator
from pathlib import Path
from queue import Queue
from uuid import uuid4, UUID
from copy import deepcopy
from graphlib import TopologicalSorter
from importlib.util import module_from_spec, spec_from_file_location

import inspect
import psutil

from . import cfg, data, logs


def _cleanup(
    logger: logging.LoggerAdapter
) -> Callable:
    def inner(*_):
        for proc in psutil.Process().children(recursive=True):
            logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
            proc.terminate()
            try:
                proc.wait(10)
            except psutil.TimeoutExpired:
                logger.warning(
                    f"Killing unresponsive process {proc.name()} ({proc.pid})"
                )
                proc.kill()
                proc.wait()
        raise SystemExit(1)

    return inner


class Runner(mp.Process):
    """Base class for cellophane runners."""

    label: ClassVar[str]
    individual_samples: ClassVar[bool]
    link_by: ClassVar[Optional[str]]
    func: ClassVar[Callable]
    wait: ClassVar[bool]
    main: ClassVar[Callable]
    id: UUID
    done: bool = False

    def __init_subclass__(
        cls,
        func: Callable,
        label: Optional[str],
        individual_samples: bool = False,
        link_by: Optional[str] = None,
    ) -> None:
        cls.__name__ = func.__name__
        cls.__qualname__ = func.__qualname__
        cls.__module__ = func.__module__
        cls.name = func.__name__
        cls.label = label or func.__name__
        cls.main = staticmethod(func)
        cls.label = label or cls.__name__
        cls.individual_samples = individual_samples
        cls.link_by = link_by
        super().__init_subclass__()

    def __init__(
        self,
        config: cfg.Config,
        samples: data.Samples,
        timestamp: str,
        log_queue: Queue,
        log_level: int,
        output_queue: mp.Queue,
        root: Path,
    ):
        self.output_queue = output_queue
        self.log_queue = log_queue
        self.log_level = log_level
        self.n_samples = len(samples)
        self.id = uuid4()

        super().__init__(
            target=self._main,
            kwargs={
                "config": config,
                "samples": deepcopy(samples),
                "timestamp": timestamp,
                "root": root,
            },
        )

    def _main(
        self,
        config: cfg.Config,
        samples: data.Samples[data.Sample],
        timestamp: str,
        root: Path,
    ) -> None:
        for sample in samples:
            sample.done = None
            sample.runner = self.label  # type: ignore[attr-defined]

        logger = logs.get_logger(
            label=self.label,
            level=self.log_level,
            queue=self.log_queue,
        )

        signal(SIGTERM, _cleanup(logger))
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
        sys.stderr = open(os.devnull, "w", encoding="utf-8")

        outdir = config.outdir / config.get("outprefix", timestamp) / self.label
        if self.individual_samples:
            outdir /= samples[0].id

        try:
            returned: data.Samples = self.main(
                samples=samples,
                config=config,
                timestamp=timestamp,
                label=self.label,
                logger=logger,
                root=root,
                outdir=outdir,
            )

        except Exception as exception:
            logger.critical(exception, exc_info=config.log_level == "DEBUG")
            self.output_queue.put((samples, self.id))
            self.output_queue.close()
            raise SystemExit(1)

        else:
            match returned:
                case None:
                    logger.debug(f"Runner {self.label} did not return any samples")
                    for sample in samples:
                        sample.done = True
                    self.output_queue.put((samples, self.id))

                case returned if issubclass(type(returned), data.Samples):
                    for sample in returned:
                        sample.done = True if sample.done is None else sample.done
                    if n_complete := len(returned.complete):
                        logger.info(
                            f"Runner {self.label} completed {n_complete} samples"
                        )
                    if n_failed := len(returned.failed):
                        logger.warning(
                            f"Runner {self.label} failed for {n_failed} samples"
                        )
                    self.output_queue.put((returned, self.id))

                case _:
                    logger.warning(f"Unexpected return type {type(returned)}")
                    self.output_queue.put((samples, self.id))

            self.output_queue.close()
            raise SystemExit(0)


class Hook:
    """Base class for cellophane pre/post-hooks."""

    name: ClassVar[str]
    label: ClassVar[str]
    func: ClassVar[Callable]
    when: ClassVar[Literal["pre", "post"]]
    condition: ClassVar[Literal["always", "complete", "failed"]]
    before: ClassVar[list[str]]
    after: ClassVar[list[str]]

    def __init_subclass__(
        cls,
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
                cls.before = ["before_all"]
                cls.after = after
            case list(before), "all":
                cls.before = before
                cls.after = ["after_all"]
            case list(before), list(after):
                cls.before = [*before, "after_all"]
                cls.after = [*after, "before_all"]
            case _:
                raise ValueError(f"{func.__name__}: {before=}, {after=}")
        cls.__name__ = func.__name__
        cls.__qualname__ = func.__qualname__
        cls.__module__ = func.__module__
        cls.name = func.__name__
        cls.label = label or func.__name__
        cls.condition = condition
        cls.func = staticmethod(func)
        cls.when = when
        super().__init_subclass__()

    def __call__(
        self,
        samples: data.Samples,
        config: cfg.Config,
        timestamp: str,
        log_queue: Queue,
        log_level: int,
        root: Path,
    ) -> data.Samples:
        if self.when == "pre" or self.condition == "always" or samples:
            _logger = logs.get_logger(
                label=self.label,
                level=log_level,
                queue=log_queue,
            )
            _logger.debug(f"Running {self.label} hook")

            outdir = config.outdir / config.get("outprefix", timestamp)

            return self.func(
                samples=samples,
                config=config,
                timestamp=timestamp,
                logger=_logger,
                root=root,
                outdir=outdir,
            )
        else:
            return samples


def resolve_hook_dependencies(
    hooks: list[type[Hook]],
) -> list[type[Hook]]:
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


def load_modules(
    path: Path,
) -> Iterator[tuple[str, type[Hook] | type[Runner] | type[data.Sample | data.Samples]]]:
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
                            isinstance(obj, type)
                            and (
                                issubclass(obj, Hook)
                                or issubclass(obj, data.Sample)
                                or issubclass(obj, data.Samples)
                                or issubclass(obj, Runner)
                            )
                            and inspect.getmodule(obj) == module
                        ):
                            yield base, obj


def pre_hook(
    label: Optional[str] = None,
    before: list[str] | Literal["all"] = [],
    after: list[str] | Literal["all"] = []
):

    """Decorator for hooks that will run before all runners."""

    def wrapper(func):
        class _hook(
            Hook,
            label=label,
            func=func,
            when="pre",
            condition="always",
            before=before,
            after=after,
        ):
            pass
        return _hook
    return wrapper


def post_hook(
    label: Optional[str] = None,
    condition: Literal["always", "complete", "failed"] = "always",
    before: list[str] | Literal["all"] = [],
    after: list[str] | Literal["all"] = []
):
    """Decorator for hooks that will run after all runners."""

    def wrapper(func):
        class _hook(
            Hook,
            label=label,
            func=func,
            when="post",
            condition=condition,
            before=before,
            after=after,
        ):
            pass
        return _hook
    return wrapper


def runner(
    label: Optional[str] = None,
    individual_samples: bool = False,
    link_by: Optional[str] = None,
):
    """Decorator for runners."""

    def wrapper(func):
        class _runner(
            Runner,
            label=label,
            func=func,
            individual_samples=individual_samples,
            link_by=link_by,
        ):
            pass
        return _runner
    return wrapper
