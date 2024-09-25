from logging import LoggerAdapter
from pathlib import Path
from time import sleep
from typing import Any
from uuid import UUID

from attrs import define

from cellophane.executors.executor import Executor


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
        sleep(0.1)  # Ensure logs are printed before the return
