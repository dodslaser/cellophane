"""Logging utilities"""

import atexit
import logging
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Callable
from queue import Queue
from functools import wraps

from rich.logging import RichHandler


def get_log_queue(manager) -> Queue:
    """Create a queue for logging and a listener to handle it."""
    console_handler = RichHandler(show_path=True)
    console_handler.setFormatter(
        logging.Formatter("%(label)s: %(message)s", datefmt="%H:%M:%S")
    )
    queue = manager.Queue(-1)
    listener = QueueListener(queue, console_handler)
    listener.start()
    atexit.register(listener.stop)

    return queue


def add_file_handler(logger, path: Path) -> None:
    """Add a file handler to a logger."""
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s : %(label)s : %(message)s")
    )
    logger.addHandler(file_handler)


def get_logger(
    label: str,
    level: int,
    queue: Queue,
    path: Path | None = None,
) -> logging.LoggerAdapter:
    """Create a logger with a queue handler and a file handler if specified."""
    logger = logging.getLogger(label)
    logger.setLevel(level)
    queue_handler = QueueHandler(queue)
    logger.addHandler(queue_handler)

    if path is not None:
        add_file_handler(logger, path)

    adapter = logging.LoggerAdapter(logger, {"label": label})

    return adapter


def log_exceptions(
    logger: logging.LoggerAdapter,
    exit: bool = True,
    cleanup_fn: Callable | None = lambda: None,
):
    """Decorator to log exceptions."""

    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, logger=logger, **kwargs)
            except Exception as e:
                logger.critical(f"Unhandled exception: {e}", exc_info=True)
                cleanup_fn()
                if exit:
                    raise SystemExit(1)

        return inner

    return wrapper
