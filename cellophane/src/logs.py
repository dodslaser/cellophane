"""Logging utilities"""

import atexit
import logging
from logging.handlers import QueueHandler, QueueListener
from pathlib import Path
import multiprocessing as mp

from rich.logging import RichHandler


def setup_logging():  # pragma: no cover
    """Setup logging for multiprocessing."""
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
    """Add a file handler to a logger."""
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
