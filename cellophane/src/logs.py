"""Logging utilities"""

import atexit
import logging
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue
from pathlib import Path

from rich.logging import RichHandler


def setup_queue_logging(
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


def start_queue_listener() -> Queue:
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
        queue, *logging.getLogger().handlers, respect_handler_level=True
    )
    listener.start()
    atexit.register(listener.stop)
    return queue


def setup_logging(logger: logging.Logger = logging.getLogger()) -> RichHandler:
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
            defaults={"label": "external"},
        )
    )

    logger.setLevel(logging.DEBUG)
    logger.handlers = [console_handler]
    return console_handler


def add_file_handler(path: Path, logger: logging.Logger = logging.getLogger()) -> None:
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
            "%(asctime)s : %(label)s : %(message)s",
            defaults={"label": "external"},
        )
    )
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
