"""Base classes and functions for cellophane modules."""

import multiprocessing as mp
import os
import sys
from copy import deepcopy
from dataclasses import dataclass
import logging
from signal import SIGTERM, signal
from typing import Callable, Optional, ClassVar, Literal
from pathlib import Path
from queue import Queue
from uuid import uuid4

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
    name: ClassVar[str]
    individual_samples: ClassVar[bool]
    wait: ClassVar[bool]
    id: str
    done: bool = False

    def __init_subclass__(
        cls,
        name: str,
        label: Optional[str] = None,
        individual_samples: bool = False,
    ) -> None:
        cls.label = label or name
        cls.name = name
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


@dataclass
class Hook:
    """Base class for cellophane pre/post-hooks."""

    label: str
    name: str
    func: Callable
    overwrite: bool
    when: Literal["pre", "post"]
    condition: Literal["complete", "partial", "always"] = "always"
    before: list[str] = []
    after: list[str] = []

    def __post_init__(self) -> None:
        if "all" in self.before:
            self.before = ["before-all"]
        if "all" in self.after:
            self.after = ["after-all"]
        if self.before or self.after:
            self.before += ["after-all"]
            self.after += ["before-all"]
        else:
            self.after = ["after-all"]

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
    overwrite: bool = False,
    before: list[str] = [],
    after: list[str] = [],
):
    """Decorator for hooks that will run before all runners."""

    def wrapper(func):
        return Hook(
            label=label or func.__name__,
            name=func.__name__,
            func=func,
            overwrite=overwrite,
            when="pre",
            condition="always",
            before=before,
            after=after,
        )

    return wrapper


def post_hook(
    label: Optional[str] = None,
    overwrite: bool = False,
    condition: Literal["complete", "partial", "always"] = "always",
):
    """Decorator for hooks that will run after all runners."""

    def wrapper(func):
        return Hook(
            label=label or func.__name__,
            name=func.__name__,
            func=func,
            overwrite=overwrite,
            when="post",
            condition=condition,
            priority=priority,
        )

    return wrapper


def runner(
    label: Optional[str] = None,
    individual_samples: bool = False,
):
    """Decorator for runners."""

    def wrapper(func):
        class _runner(
            Runner,
            name=func.__name__,
            label=label,
            individual_samples=individual_samples,
        ):
            def __init__(self, *args, **kwargs):
                self.main = staticmethod(func)
                super().__init__(*args, **kwargs)

        return _runner

    return wrapper
