"""Logging utilities"""

import inspect
import logging
import warnings
from functools import cache
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from pathlib import Path
from typing import Any, Callable

from attrs import define
from rich.logging import RichHandler


@define
class ExternalFilter(logging.Filter):
    """Filter for log records coming from external libraries."""

    internal_roots: tuple[Path, ...]

    def filter(self, record: logging.LogRecord) -> bool:
        return self._check_relative(Path(record.pathname), self.internal_roots)

    @staticmethod
    @cache
    def _check_relative(path: Path, roots: tuple[Path, ...]) -> bool:
        return any(path.is_relative_to(r) for r in roots)

def _showwarning(showwarning_orig: Callable) -> Callable:
    def inner(
        message: Warning | str,
        category: type[Warning],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        if category is not UserWarning:
            showwarning_orig(message, category, *args, **kwargs)
            return

        logger = logging.getLogger()
        if isinstance(message, Warning):
            message = message.args[0]

        stack = inspect.stack()

        record = logger.makeRecord(
            name=logger.name,
            level=logging.WARNING,
            fn=stack[2].filename,
            lno=stack[2].lineno,
            msg=message.args[0] if isinstance(message, Warning) else message,
            func=stack[2].function,
            args=(),
            exc_info=None,
        )
        logger.handle(record)

    return inner


def handle_warnings() -> None:
    _warnings_showwarning = warnings.showwarning
    warnings.showwarning = _showwarning(_warnings_showwarning)


def redirect_logging_to_queue(
    queue: Queue,
    logger: logging.Logger = logging.getLogger(),
) -> QueueHandler:
    """Set up queue-based logging for a logger.

    Args:
        queue (Queue): The queue to store log records.
        logger (logging.Logger, optional): The logger to set up.
            Defaults to the root logger.

    Returns:
        QueueHandler: The queue handler.
    """
    queue_handler = QueueHandler(queue)
    logger.handlers = [queue_handler]

    return queue_handler


def start_logging_queue_listener() -> tuple[Queue, QueueListener]:
    """
    Starts a queue listener that listens to the specified queue and passes
    log records to the specified handlers.

    Args:
        queue (Queue): The queue to listen to.
        handlers (logging.Handler): The handlers to pass log records to.

    Returns:
        QueueListener: The queue listener.
    """
    queue: Queue = Queue()
    listener = QueueListener(
        queue,
        *logging.getLogger().handlers,
        respect_handler_level=True,
    )
    listener.start()

    return queue, listener


def setup_console_handler(
    logger: logging.Logger = logging.getLogger(),
    filters: tuple[logging.Filter, ...] | None = None,
) -> RichHandler:
    """
    Sets up logging for the cellophane module.

    Removes any existing handlers, creates a logger, sets up a log queue,
    creates a console handler with a specific formatter, starts a queue listener,
    and registers a listener stop function to be called at exit.
    """

    console_handler = RichHandler(show_path=True)
    console_handler.setFormatter(
        logging.Formatter(
            "%(label)s: %(message)s",
            datefmt="%H:%M:%S",
            defaults={"label": "unknown"},
        )
    )
    for filter_ in filters or ():
        console_handler.addFilter(filter_)
    logger.setLevel(logging.DEBUG)
    logger.handlers = [console_handler]
    return console_handler


def setup_file_handler(
    path: Path,
    logger: logging.Logger = logging.getLogger(),
    filters: tuple[logging.Filter, ...] = (),
) -> logging.FileHandler:
    """
    Creates a file handler for the specified logger and adds it to the logger's
    handlers. The file handler writes log messages to the specified file path.
    The log messages are formatted with a timestamp, label, and message.

    Args:
        logger (logging.LoggerAdapter): The logger to add the file handler to.
        path (Path): The path to the log file.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s : %(levelname)s : %(label)s : %(message)s",
            defaults={"label": "external"},
        )
    )
    for filter_ in filters:
        file_handler.addFilter(filter_)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    return file_handler
