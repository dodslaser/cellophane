"""Base classes and functions for cellophane modules."""

import multiprocessing as mp
import os
import sys
import logging
from signal import SIGTERM, signal
from typing import Callable, Optional, ClassVar, Literal
from pathlib import Path
from queue import Queue
from uuid import uuid4, UUID

import psutil

from . import cfg, data, logs


def _cleanup(
    logger: logging.LoggerAdapter
) -> Callable:
    def inner(*_):
        for proc in psutil.Process().children(recursive=True):
            logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
            proc.terminate()
            proc.wait()
        raise SystemExit(1)

    return inner


class Runner(mp.Process):
    """Base class for cellophane runners."""

    label: ClassVar[str]
    individual_samples: ClassVar[bool]
    func: ClassVar[Callable]
    wait: ClassVar[bool]
    id: UUID
    done: bool = False

    def __init_subclass__(
        cls,
        func: Callable,
        label: Optional[str],
        individual_samples: bool = False,
    ) -> None:
        cls.__name__ = func.__name__
        cls.__qualname__ = func.__qualname__
        cls.__module__ = func.__module__
        cls.name = func.__name__
        cls.label = label or func.__name__
        cls.func = staticmethod(func)
        cls.label = label or cls.__name__
        cls.individual_samples = individual_samples
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
                "samples": samples,
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
    ) -> Optional[data.Samples[data.Sample]]:
        for sample in samples:
            sample.complete = False
            sample.runner = self.label

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

        logger.debug(f"Passing {self.n_samples} samples to {self.label}")
        try:
            returned = self.main(
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

        else:
            logger.info(f"Finished {self.label} runner")
            match returned:
                case None:
                    for sample in samples:
                        sample.complete = True
                    self.output_queue.put((samples, self.id))
                case returned if issubclass(type(returned), data.Samples):
                    self.output_queue.put((returned, self.id))
                case _:
                    logger.warning(f"Unexpected return type {type(returned)}")
                    self.output_queue.put((samples, self.id))


    @staticmethod
    def main(**_) -> Optional[data.Samples[data.Sample]]:
        """Main function for the runner."""
        raise NotImplementedError


class Hook:
    """Base class for cellophane pre/post-hooks."""

    name: ClassVar[str]
    label: ClassVar[str]
    func: ClassVar[Callable]
    when: ClassVar[Literal["pre", "post"]]
    condition: ClassVar[Literal["complete", "partial", "always"]]
    before: ClassVar[list[str]]
    after: ClassVar[list[str]]

    def __init_subclass__(
        cls,
        func: Callable,
        when: Literal["pre", "post"],
        label: str | None = None,
        condition: Literal["complete", "partial", "always"] = "always",
        before: list[str] = [],
        after: list[str] = [],
    ) -> None:
        
        cls.__name__ = func.__name__
        cls.__qualname__ = func.__qualname__
        cls.__module__ = func.__module__
        cls.name = func.__name__
        cls.label = label or func.__name__
        cls.condition = condition
        cls.before = before
        cls.after = after
        cls.func = staticmethod(func)
        cls.when = when
        super().__init_subclass__()

    def __call__(
        self,
        samples: data.Samples,
        config: cfg.Config,
        timestamp: str,
        log_queue: mp.Queue,
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
            return self.func(
                samples=samples,
                config=config,
                timestamp=timestamp,
                logger=_logger,
                root=root,
            )
        else:
            return samples


def pre_hook(
    label: Optional[str] = None,
    before: list[str] | Literal["all"] = [],
    after: list[str] | Literal["all"]= []
):

    """Decorator for hooks that will run before all runners."""

    match before, after:
        case "all", list():
            before = ["before_all"]
        case list(), "all":
            after = ["after_all"]
        case list(before), list(after):
            before=[*before, "after_all"]
            after=[*after, "before_all"]
        case _:
            raise ValueError("Invalid dependencies: {before=}, {after=}")

    def wrapper(func):
        class _hook(
                Hook,
                label=label,
                func=func,
                when="pre",
                condition="always",
                # FIXME: Figure out if this is a bug in mypy
                before=before,  # type: ignore
                after=after,  # type: ignore
            ):
                pass
        return _hook
    return wrapper


def post_hook(
    label: Optional[str] = None,
    condition: Literal["complete", "partial", "always"] = "always",
):
    """Decorator for hooks that will run after all runners."""

    def wrapper(func):
        class _hook(
            Hook,
            label=label,
            func=func,
            when="post",
            condition=condition,
        ):
            pass
        return _hook
    return wrapper


def runner(
    label: Optional[str] = None,
    individual_samples: bool = False,
):
    """Decorator for runners."""

    def wrapper(func):
        class _runner(
            Runner,
            label=label,
            func=func,
            individual_samples=individual_samples,
        ):
            pass
        return _runner
    return wrapper
