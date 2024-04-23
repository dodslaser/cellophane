"""Executors for running external scripts as jobs."""

import logging
import multiprocessing as mp
from functools import partial
from multiprocessing.synchronize import Lock
from pathlib import Path
from typing import Any, Callable, ClassVar
from uuid import UUID, uuid4

from attrs import define, field
from mpire import WorkerPool
from mpire.async_result import AsyncResult

from cellophane.src import cfg

from .util import callback_wrapper, target_wrapper


@define(slots=False)
class Executor:
    """Executor base class."""

    name: ClassVar[str]
    config: cfg.Config
    pool: WorkerPool
    log_queue: mp.Queue
    jobs: dict[UUID, AsyncResult] = field(factory=dict, init=False)
    locks: dict[UUID, Lock] = field(factory=dict, init=False)

    def __init_subclass__(cls, *args: Any, name: str, **kwargs: Any) -> None:
        """Register the class in the registry."""
        super().__init_subclass__(*args, **kwargs)
        cls.name = name or cls.__name__.lower()

    def __attrs_post_init__(self) -> None:
        self.pool.set_shared_objects(
            (
                self.log_queue,
                self.config,
                self.target,
                self.terminate_hook,
            )
        )

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
        """
        Will be called by the executor to execute a command.

        Subclasses should override this method to implement the target execution.

        Args:
            name (str): The name of the job.
            uuid (UUID): The UUID of the job.
                This should generally not be overridden, but can be used to
                identify the job in the target execution.
            workdir (Path): The working directory for the target.
            env (dict): The environment variables for the target.
            os_env (bool): Flag indicating whether to use the OS environment variables.
            logger (logging.LoggerAdapter): The logger for the target.
            cpus (int): The number of CPUs to allocate for the target.
            memory (int): The amount of memory to allocate for the target.

        Returns:
            int | None: The return code of the target execution,
                or None if not applicable.

        Raises:
            NotImplementedError: If the target execution is not implemented.
        """

        del name, uuid, workdir, env, os_env, logger, cpus, memory  # Unused
        raise NotImplementedError

    def submit(
        self,
        *args: str | Path,
        name: str | None = None,
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
        """Submit a job for execution.

        Args:
            *args: Variable length argument list of strings or paths.
            name: The name of the job.
                Defaults to __name__.
            wait: Whether to wait for the job to complete.
                Defaults to False.
            uuid: The UUID of the job.
                Defaults to a random UUID.
            workdir: The working directory for the job.
                Defaults to the `config.workdir / uuid.hex`.
            env: The environment variables for the job.
                Defaults to None.
            os_env: Whether to include the OS environment variables.
                Defaults to True.
            callback: The callback function to be executed on job completion.
                Defaults to None.
            error_callback: The callback function to be executed on job failure.
                Defaults to None.
            cpus: The number of CPUs to allocate for the job.
                Defaults to all available CPUs.
                May not be supported by all executors.
            memory: The amount of memory to allocate for the job.
                Defaults to all available memory.
                May not be supported by all executors.

        Returns:
            A tuple containing the AsyncResult object and the UUID of the job.
        """
        _uuid = uuid or uuid4()
        logger = logging.LoggerAdapter(
            logging.getLogger(),
            {"label": name or self.__class__.name},
        )
        self.locks[_uuid] = mp.Lock()
        self.locks[_uuid].acquire()

        result = self.pool.apply_async(
            func=target_wrapper,
            kwargs={
                "uuid": _uuid,
                "name": name,
                "args": args,
                "workdir": workdir,
                "env": env,
                "os_env": os_env,
                "cpus": cpus,
                "memory": memory,
            },
            callback=partial(
                callback_wrapper,
                fn=callback,
                msg=f"Job completed: {_uuid}",
                logger=logger,
                lock=self.locks[_uuid],
            ),
            error_callback=partial(
                callback_wrapper,
                fn=error_callback,
                msg=f"Job failed: {_uuid}",
                logger=logger,
                lock=self.locks[_uuid],
            ),
        )
        self.jobs[_uuid] = result

        if wait:
            result.wait()
            self.locks[_uuid].acquire()

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
        for lock in self.locks.values():
            lock.acquire()
            lock.release()

    def wait(self, uuid: UUID | None = None) -> None:
        """Wait for a specific job or all jobs to complete.

        Args:
            uuid: The UUID of the job to wait for. Defaults to None.

        Returns:
            None
        """
        if uuid in self.jobs:
            self.jobs[uuid].wait()
            self.locks[uuid].acquire()
            self.locks[uuid].release()
        elif uuid is None:
            self.pool.stop_and_join(keep_alive=True)
            for uuid, lock in self.locks.items():
                lock.acquire()
                lock.release()
