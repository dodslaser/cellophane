"""Executors for running external scripts as jobs."""

import logging
import multiprocessing as mp
import os
import shlex
import subprocess as sp
import sys
from pathlib import Path
from signal import SIGTERM, signal
from typing import Any, Callable, ClassVar
from uuid import UUID, uuid4

from attrs import define, field

from . import cfg, logs


@define(slots=False)
class Executor:
    """Executor base class."""

    name: ClassVar[str]
    config: cfg.Config
    log_queue: mp.Queue
    jobs: dict[UUID, mp.Process] = field(factory=dict, init=False)

    def __init_subclass__(cls, *args: Any, name: str, **kwargs: Any) -> None:
        """Register the class in the registry."""
        super().__init_subclass__(*args, **kwargs)
        cls.name = name or cls.__name__.lower()

    def target(self, *args: Any, **kwargs: Any) -> int | None:  # pragma: no cover
        del args, kwargs  # Unused
        raise NotImplementedError

    def submit(
        self,
        *args: str | Path,
        name: str = __name__,
        wait: bool = False,
        uuid: UUID | None = None,
        workdir: Path | None = None,
        env: dict | None = None,
        os_env: bool = True,
        callback: Callable | None = None,
        error_callback: Callable | None = None,
        cpus: int | None = None,
        memory: int | None = None,
    ) -> tuple[mp.Process, UUID]:
        """Submit a job."""

        logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})
        _uuid = uuid or uuid4()
        
        def _terminate_hook(*args: Any, **kwargs: Any) -> None:
            del args, kwargs  # Unused
            nonlocal logger, _uuid
            code = self.terminate_hook(_uuid, logger)
            raise SystemExit(code or 143)

        def _target() -> None:
            sys.stdout = sys.stderr = open(os.devnull, "w", encoding="utf-8")
            logs.setup_queue_logging(self.log_queue)
            logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})
            _workdir = workdir or self.config.workdir / _uuid.hex
            _workdir.mkdir(parents=True, exist_ok=True)
            _args = [word for arg in args for word in shlex.split(str(arg))]
            _env = {k: str(v) for k, v in env.items()} if env else {}
            _cpus = cpus or self.config.executor.cpus
            _memory = memory or self.config.executor.memory
            signal(SIGTERM, _terminate_hook)

            try:
                self.target(
                    *_args,
                    name=name,
                    uuid=_uuid,
                    workdir=_workdir,
                    env=_env,
                    os_env=os_env,
                    logger=logger,
                    cpus=_cpus,
                    memory=_memory,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(f"{args} failed with exception {exc}")
                (_workdir / "error").touch()
                _callback = error_callback or (lambda: None)
            else:
                logger.debug(f"{args} completed successfully")
                _callback = callback or (lambda: None)

            try:
                _callback()
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(f"Callback failed with exception {exc}")

        proc = mp.Process(target=_target)
        self.jobs[_uuid] = proc
        proc.start()

        if wait:
            proc.join()
        return proc, _uuid

    def terminate_hook(self, uuid: UUID, logger: logging.LoggerAdapter) -> int | None:
        """Hook to be called prior to job termination.

        This hook will only run if the job is terminated prior to completion.
        After this hook has been called the job will exit with code 143.

        Args:
            uuid (UUID): The UUID of the job pending termination.
            logger (logging.LoggerAdapter): A logger adapter for the job.
        """
        del uuid, logger  # Unused
        return 143  # SIGTERM

    def terminate(self, uuid: UUID | None = None) -> None:
        """Terminating a specific job or all jobs."""
        if uuid in self.jobs:
            self.jobs[uuid].terminate()
        elif uuid is None:
            for job in self.jobs.values():
                job.terminate()

    def join(self, uuid: UUID | None = None) -> None:
        """Wait for a specific job or all jobs to complete."""
        if uuid in self.jobs:
            self.jobs[uuid].join()
        elif uuid is None:
            for job in self.jobs.values():
                job.join()

EXECUTOR: type[Executor] = Executor

@define(slots=False)
class SubprocesExecutor(Executor, name="subprocess"):
    """Executor using multiprocessing."""

    procs: dict[UUID, sp.Popen] = field(factory=dict, init=False)

    def target(
        self,
        *args: str,
        uuid: UUID,
        workdir: Path,
        env: dict,
        os_env: bool = True,
        logger: logging.LoggerAdapter,
        **kwargs: Any,
    ) -> None:
        del kwargs  # Unused
        logdir = self.config.logdir / "subprocess"
        logdir.mkdir(parents=True, exist_ok=True)

        with (
            open(logdir / f"{uuid.hex}.out", "w", encoding="utf-8") as stdout,
            open(logdir / f"{uuid.hex}.err", "w", encoding="utf-8") as stderr
        ):
            proc = sp.Popen(
                shlex.join(args),
                cwd=workdir,
                env=env or {} | ({**os.environ} if os_env else {}),
                shell=True,
                stdout=stdout,
                stderr=stderr,
            )
            self.procs[uuid] = proc
            logger.debug(f"Started child process (pid={proc.pid})")

            self.procs[uuid].wait()
            logger.debug(
                f"Child process (pid={proc.pid}) exited with code {proc.returncode}"
            )
            raise SystemExit(self.procs[uuid].returncode)

    def terminate_hook(self, uuid: UUID, logger: logging.LoggerAdapter) -> int | None:
        if uuid in self.procs:
            proc = self.procs[uuid]
            logger.warning(f"Terminating child process (pid={proc.pid})")
            self.procs[uuid].send_signal(SIGTERM)
            pid, code = os.waitpid(self.procs[uuid].pid, 0)
            logger.debug(f"Child process (pid={pid}) exited with code {code}")
            return code
        else:
            return None
