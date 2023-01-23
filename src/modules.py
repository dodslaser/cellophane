"""Base classes and functions for cellophane modules."""

import multiprocessing as mp
import os
import sys
from copy import deepcopy
from dataclasses import dataclass
import logging
from signal import SIGTERM, signal
from typing import Callable, Optional, ClassVar
from pathlib import Path
from queue import Queue

import psutil

from . import cfg, data, logs


def _cleanup(logger: logging.LoggerAdapter):
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
    wait: ClassVar[bool]

    def __init_subclass__(
        cls,
        label: Optional[str] = None,
        individual_samples: bool = False,
    ) -> None:
        cls.label = label or cls.__name__
        cls.individual_samples = individual_samples
        super().__init_subclass__()

    def __init__(
        self,
        config: cfg.Config,
        samples: data.Samples,
        log_queue: Queue,
        log_level: int,
        output: mp.Queue,
        root: Path,
    ):
        self.output = output
        self.log_queue = log_queue
        self.log_level = log_level
        super().__init__(
            target=self._main,
            kwargs={
                "config": config,
                "samples": samples,
                "root": root,
            },
        )

    def _main(
        self,
        config: cfg.Config,
        samples: data.Samples,
        root: Path,
    ) -> None:
        logger = logs.get_logger(
            label=self.label,
            level=self.log_level,
            queue=self.log_queue,
        )
        signal(SIGTERM, _cleanup(logger))
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
        sys.stderr = open(os.devnull, "w", encoding="utf-8")
        try:
            original = deepcopy(samples)
            returned = self.main(
                samples=samples,
                config=config,
                label=self.label,
                logger=logger,
                root=root,
            )

            match returned:
                case None if any(s.id not in [o.id for o in original] for s in samples):
                    logger.warning("Runner returned None, but samples were modified")
                    self.output.put(original)
                case None:
                    logger.debug("Runner did not modify samples")
                    self.output.put(original)
                case data.Samples:
                    self.output.put(returned)
                case _:
                    logger.warning(
                        f"Runner returned an unexpected type {type(returned)}"
                    )
                    self.output.put(original)
            
            self.output.close()
            self.output.join_thread()

        except Exception as exception:
            logger.critical(
                "Caught an exception",
                exc_info=config.log_level == "DEBUG",
            )
            self.output.put(None)
            self.output.close()
            self.output.join_thread()
            raise SystemExit(1) from exception

    @staticmethod
    def main(*args, **kwargs) -> None:
        """Main function for the runner."""
        raise NotImplementedError


@dataclass
class Hook:
    """Base class for cellophane pre/post-hooks."""

    label: str
    func: Callable
    overwrite: bool
    when: str
    priority: int | float = float("inf")

    def __call__(
        self,
        config: cfg.Config,
        samples: data.Samples,
        log_queue: mp.Queue,
        log_level: int,
        root: Path,
    ) -> data.Samples:
        _adapter = logs.get_logger(
            label=self.label,
            level=log_level,
            queue=log_queue,
        )
        return self.func(
            config=config,
            samples=samples,
            logger=_adapter,
            root=root,
        )


def pre_hook(
    label: Optional[str] = None,
    overwrite: bool = False,
    priority: int | float = float("inf"),
):
    """Decorator for hooks that will run before all runners."""

    def wrapper(func):
        return Hook(
            label=label or func.__name__,
            func=func,
            overwrite=overwrite,
            when="pre",
            priority=priority,
        )

    return wrapper


def post_hook(
    label: Optional[str] = None,
    overwrite: bool = False,
    priority: int | float = float("inf"),
):
    """Decorator for hooks that will run after all runners."""

    def wrapper(func):
        return Hook(
            label=label or func.__name__,
            func=func,
            overwrite=overwrite,
            when="post",
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
            label=label or func.__name__,
            individual_samples=individual_samples,
        ):
            @staticmethod
            def main(*args, **kwargs):
                return func(*args, **kwargs)

        return _runner

    return wrapper
