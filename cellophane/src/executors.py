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
from mpire import WorkerPool
from mpire.async_result import AsyncResult

from . import cfg, logs


@define(slots=False)
class Executor:
    """Executor base class."""

    name: ClassVar[str]
    config: cfg.Config
    pool: WorkerPool
    log_queue: mp.Queue
    jobs: dict[UUID, AsyncResult] = field(factory=dict, init=False)

    def __init_subclass__(cls, *args: Any, name: str, **kwargs: Any) -> None:
        """Register the class in the registry."""
        super().__init_subclass__(*args, **kwargs)
        cls.name = name or cls.__name__.lower()

    def target(
        self,
        *,
        name: str,
        uuid: UUID,
        workdir: Path,
        env: dict,
        os_env: bool,
        logger: logging.LoggerAdapter,
        cpus: int,
        memory: int,
    ) -> int | None:  # pragma: no cover
        del name, uuid, workdir, env, os_env, logger, cpus, memory  # Unused
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
    ) -> tuple[AsyncResult, UUID]:
        """Submit a job."""

        logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})
        _uuid = uuid or uuid4()

        def _terminate_hook(*args: Any, **kwargs: Any) -> None:
            del args, kwargs  # Unused
            nonlocal logger, _uuid
            code = self.terminate_hook(_uuid, logger)
            raise SystemExit(code or 143)

        def _target(shared) -> None:
            sys.stdout = sys.stderr = open(os.devnull, "w", encoding="utf-8")
            log_queue, config, target, terminate_hook = shared
            logs.setup_queue_logging(log_queue)
            logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})
            _workdir = workdir or config.workdir / _uuid.hex
            _workdir.mkdir(parents=True, exist_ok=True)
            signal(SIGTERM, terminate_hook)

            try:
                target(
                    *(word for arg in args for word in shlex.split(str(arg))),
                    name=name,
                    uuid=_uuid,
                    workdir=_workdir,
                    env={k: str(v) for k, v in env.items()} if env else {},
                    os_env=os_env,
                    logger=logger,
                    cpus=cpus or config.executor.cpus,
                    memory=memory or config.executor.memory,
                )
            except SystemExit as exc:
                if exc.code != 0:
                    logger.error(f"{args} failed with exit code {exc.code}")
                    raise SystemExit(exc.code) from exc
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(f"{args} failed with exception {exc}")
                raise SystemExit(1) from exc

            logger.debug(f"{args} completed successfully")

        self.pool.set_shared_objects(
            (
                self.log_queue,
                self.config,
                self.target,
                _terminate_hook
            ))

        result = self.pool.apply_async(
            func=_target,
            callback=callback,
            error_callback=error_callback,
        )
        self.jobs[_uuid] = result

        if wait:
            result.wait()

        return result, _uuid

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

    def terminate(self) -> None:
        """Terminate all jobs."""
        self.pool.terminate()

    def wait(self, uuid: UUID | None = None) -> None:
        """Wait for a specific job or all jobs to complete."""
        if uuid in self.jobs:
            self.jobs[uuid].wait()
        elif uuid is None:
            self.pool.stop_and_join(keep_alive=True)


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
            open(logdir / f"{uuid.hex}.err", "w", encoding="utf-8") as stderr,
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
