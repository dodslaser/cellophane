"""Logging utilities"""

import atexit
import logging
from functools import wraps
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
from typing import Callable, Optional
from queue import Queue

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


def get_logger(
    label: str,
    level: int,
    queue: Queue,
    path: Optional[Path] = None,
) -> logging.LoggerAdapter:
    """Create a logger with a queue handler and a file handler if specified."""
    logger = logging.getLogger(label)
    logger.setLevel(level)
    queue_handler = QueueHandler(queue)
    logger.addHandler(queue_handler)

    if path is not None:
        file_handler = logging.FileHandler(path)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s : %(label)s : %(message)s")
        )

        logger.addHandler(file_handler)

    adapter = logging.LoggerAdapter(logger, {"label": label})

    return adapter


def handle_logging(
    label: str,
    queue: Queue,
    path: Optional[Path | str] = None,
    level: int = logging.INFO,
    propagate_exceptions: bool = True,
) -> Callable:
    """Decorator to handle logging for a function."""
    if path is not None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

    logger = get_logger(label, level, queue, path=path)

    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs) -> None:
            try:
                func(*args, logger=logger, **kwargs)
            except Exception as exception:
                logger.critical(
                    "Caught an unhandeled exception",
                    exc_info=True,
                    stacklevel=2,
                )
                if propagate_exceptions:
                    raise Exception from exception

        return inner

    return wrapper
