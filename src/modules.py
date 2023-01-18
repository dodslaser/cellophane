"""Base classes and functions for cellophane modules."""

import multiprocessing as mp
import os
import sys
from dataclasses import dataclass
from logging import LoggerAdapter
from signal import SIGTERM, signal
from typing import Callable, Optional
from pathlib import Path


import psutil

from . import cfg, data, logs


def _cleanup(logger: LoggerAdapter):
    def inner(*_):
        for proc in psutil.Process().children(recursive=True):
            logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
            proc.terminate()
            proc.wait()
        raise SystemExit(1)

    return inner


class Runner(mp.Process):
    """Base class for cellophane runners."""

    label: str
    individual_samples: bool
    wait: bool

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
        kwargs: Optional[dict] = None,
    ):
        super().__init__(
            target=self._main,
            kwargs={
                "label": self.label,
                "config": config,
                **(kwargs or {}),
            },
        )

    def _main(
        self,
        label: str,
        config: cfg.Config,
        samples: data.Samples,
        log_queue: mp.Queue,
        log_level: int,
        root: Path,
    ) -> None:
        _adapter = logs.get_logger(
            label=label,
            level=log_level,
            queue=log_queue,
        )
        signal(SIGTERM, _cleanup(_adapter))
        sys.stdout = open(os.devnull, "w", encoding="utf-8")
        sys.stderr = open(os.devnull, "w", encoding="utf-8")
        try:
            self.main(
                samples=samples,
                config=config,
                label=label,
                logger=_adapter,
                root=root,
            )

        except Exception as exception:
            _adapter.critical("Caught an exception", exc_info=True)
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
