"""Utility functions for the executors."""

import logging
import multiprocessing as mp
import os
import shlex
import sys
from multiprocessing.synchronize import Lock
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

from mpire.exception import InterruptWorker

from cellophane.src import cfg, logs


def target_wrapper(
    shared: tuple[mp.Queue, cfg.Config, Callable, Callable],
    *,
    args: tuple[str | Path, ...],
    uuid: UUID,
    name: str,
    workdir: Path | None,
    env: dict[str, str],
    os_env: bool,
    cpus: int,
    memory: int,
) -> None:
    """Target function for the executor."""
    sys.stdout = sys.stderr = open(os.devnull, "w", encoding="utf-8")
    log_queue, config, target_, terminate_hook = shared
    logs.setup_queue_logging(log_queue)
    logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})

    _workdir = workdir or config.workdir / uuid.hex
    _workdir.mkdir(parents=True, exist_ok=True)

    def _terminate_hook(*args: Any, **kwargs: Any) -> None:
        del args, kwargs  # Unused
        code = terminate_hook(uuid, logger)
        raise SystemExit(code or 143)

    try:
        target_(
            *(word for arg in args for word in shlex.split(str(arg))),
            name=name,
            uuid=uuid,
            workdir=_workdir,
            env={k: str(v) for k, v in env.items()} if env else {},
            os_env=os_env,
            logger=logger,
            cpus=cpus or config.executor.cpus,
            memory=memory or config.executor.memory,
        )
    except InterruptWorker:
        logger.warning(f"Terminating job with uuid {uuid}")
        _terminate_hook()
    except SystemExit as exc:
        if exc.code != 0:
            logger.warning(f"Command failed with exit code: {exc.code}")
            raise exc
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(f"Command failed with exception: {exc!r}")
        raise SystemExit(1) from exc


def callback_wrapper(
    result: Any,
    fn: Callable | None,
    msg: str,
    logger: logging.LoggerAdapter,
    lock: Lock,
) -> None:
    """Callback function for the executor."""
    logger.debug(msg)
    if fn:
        try:
            fn(result)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"Callback failed: {exc!r}")
    lock.release()
