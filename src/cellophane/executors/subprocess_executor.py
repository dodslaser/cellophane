"""Executor using subprocess."""
from __future__ import annotations

import os
import shlex
import subprocess as sp  # nosec
from typing import TYPE_CHECKING

import psutil
from attrs import define, field

from .executor import Executor

if TYPE_CHECKING:
    from logging import LoggerAdapter
    from pathlib import Path
    from typing import Any
    from uuid import UUID



@define(slots=False, init=False)
class SubprocessExecutor(Executor, name="subprocess"):
    """Executor using multiprocessing."""

    pids: dict[UUID, int] = field(factory=dict, init=False)

    def target(
        self,
        *args: str,
        name: str,
        uuid: UUID,
        workdir: Path,
        env: dict,
        os_env: bool = True,
        logger: LoggerAdapter,
        stdout: Path,
        stderr: Path,
        **kwargs: Any,
    ) -> None:
        """Execute a command."""
        del kwargs  # Unused
        logdir = self.config.logdir / "subprocess"  # ty: ignore[unsupported-operator]
        logdir.mkdir(parents=True, exist_ok=True)

        with (
            open(stdout, "w", encoding="utf-8") as out,
            open(stderr, "w", encoding="utf-8") as err
        ):
            proc = sp.Popen(  # nosec
                shlex.split(shlex.join(args)),
                cwd=workdir,
                env=({**os.environ} if os_env else {}) | env,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
            self.pids[uuid] = proc.pid
            logger.debug(f"Started process (pid={proc.pid}) for job '{name}' (UUID={uuid.hex[:8]})")
            returncode = proc.wait()
            exit(returncode)

    def terminate_hook(self, uuid: UUID, logger: LoggerAdapter) -> int | None:
        if uuid not in self.pids:
            return None

        proc = psutil.Process(self.pids[uuid])
        children = proc.children(recursive=True)
        if proc.is_running():
            logger.warning(f"Terminating process (pid={self.pids[uuid]})")
            proc.terminate()
        code = int(proc.wait())
        logger.debug(f"Process (pid={self.pids[uuid]}) exited with code {code}")
        # Master skywalker, there are too many of them, what are we going to do?
        for child in children:
            logger.warning(f"Terminating orphan process (pid={child.pid})")
            child.terminate()
        psutil.wait_procs(children)
        return code
