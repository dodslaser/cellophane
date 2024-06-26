from typing import Any, Callable
from unittest.mock import MagicMock
from uuid import uuid4, UUID

from attrs import define
from mpire.async_result import AsyncResult
from pathlib import Path

from cellophane.src.executors.executor import Executor
from cellophane.src.cfg import Config
from logging import LoggerAdapter
from time import sleep


@define(slots=False, init=False)
class MockExecutor(Executor, name="mock"):
    def target(
        self,
        *args: str,
        name: str,
        uuid: UUID,
        workdir: Path,
        env: dict[str, str],
        os_env: bool,
        cpus: int,
        memory: int,
        logger: LoggerAdapter,
        **kwargs: Any,
    ) -> None:
        del kwargs
        logger.debug(f"MockExecutor called with {name=}")
        logger.debug(f"cmdline={' '.join(args)}")
        logger.debug(f"{uuid=!s}")
        logger.debug(f"{workdir=!s}")
        for k, v in env.items():
            logger.debug(f"env.{k}={v}")
        logger.debug(f"{os_env=}")
        logger.debug(f"{cpus=}")
        logger.debug(f"{memory=}")
        sleep(.1)  # Ensure logs are printed before the return