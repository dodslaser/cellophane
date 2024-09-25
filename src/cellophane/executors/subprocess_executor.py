"""Executor using subprocess."""

import os
import shlex
import subprocess as sp  # nosec
from logging import LoggerAdapter
from pathlib import Path
from typing import Any
from uuid import UUID

import psutil
from attrs import define, field

from .executor import Executor


@define(slots=False, init=False)
class SubprocessExecutor(Executor, name="subprocess"):
    """Executor using multiprocessing."""

    pids: dict[UUID, int] = field(factory=dict, init=False)

    def target(
        self,
        *args: str,
        uuid: UUID,
        workdir: Path,
        env: dict,
        os_env: bool = True,
        logger: LoggerAdapter,
        **kwargs: Any,
    ) -> None:
        """Execute a command."""
        del kwargs  # Unused
        logdir = self.config.logdir / "subprocess"
        logdir.mkdir(parents=True, exist_ok=True)

        with (
            open(logdir / f"{uuid.hex}.out", "w", encoding="utf-8") as stdout,
            open(logdir / f"{uuid.hex}.err", "w", encoding="utf-8") as stderr,
        ):
            proc = sp.Popen(  # nosec
                shlex.split(shlex.join(args)),
                cwd=workdir,
                env=env | ({**os.environ} if os_env else {}),
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            self.pids[uuid] = proc.pid
            logger.debug(f"Started process (pid={proc.pid})")
            returncode = proc.wait()
            logger.debug(f"Process (pid={proc.pid}) exited with code {returncode}")
            exit(returncode)

    def terminate_hook(self, uuid: UUID, logger: LoggerAdapter) -> int | None:
        if uuid in self.pids:
            proc = psutil.Process(self.pids[uuid])
            children = proc.children(recursive=True)
            logger.warning(f"Terminating process (pid={proc.pid})")
            proc.terminate()
            code = int(proc.wait())
            logger.debug(f"Process (pid={proc.pid}) exited with code {code}")
            for child in children:
                logger.warning(f"Terminating orphan process (pid={child.pid})")
                child.terminate()
            psutil.wait_procs(children)
            return code
        else:
            return None
