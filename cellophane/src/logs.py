"""Logging utilities"""

import atexit
import logging
import multiprocessing as mp
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path

from rich.logging import RichHandler

def setup_logging():  # pragma: no cover
    """
    Sets up logging for the cellophane module.

    Removes any existing handlers, creates a logger, sets up a log queue,
    creates a console handler with a specific formatter, starts a queue listener,
    and registers a listener stop function to be called at exit.
    """

    # Remove any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers = []

    logger = logging.getLogger("cellophane")
    logger.handlers = []

    log_queue = mp.Manager().Queue(-1)
    console_handler = RichHandler(show_path=True)
    console_handler.setFormatter(
        logging.Formatter("%(label)s: %(message)s", datefmt="%H:%M:%S")
    )
    listener = QueueListener(log_queue, console_handler)
    listener.start()
    atexit.register(listener.stop)

    queue_handler = QueueHandler(log_queue)
    logger.addHandler(queue_handler)


def add_file_handler(logger: logging.LoggerAdapter, path: Path) -> None:
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
        logging.Formatter("%(asctime)s : %(label)s : %(message)s")
    )
    logger.logger.addHandler(file_handler)


def get_labeled_adapter(
    label: str,
    logger: logging.Logger | None = None,
) -> logging.LoggerAdapter:
    if logger is None:
        logger = logging.getLogger("cellophane")
    return logging.LoggerAdapter(logger, {"label": label})
