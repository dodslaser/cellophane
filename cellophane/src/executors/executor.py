"""Executors for running external scripts as jobs."""

import logging
import multiprocessing as mp
import os
import shlex
import sys
from contextlib import suppress
from functools import partial
from multiprocessing.synchronize import Lock
from pathlib import Path
from time import sleep
from typing import Any, Callable, ClassVar, TypeVar
from uuid import UUID, uuid4

from attrs import define, field
from mpire import WorkerPool
from mpire.async_result import AsyncResult
from mpire.exception import InterruptWorker
from ruamel.yaml import YAML

from cellophane.src import cfg, logs

_LOCKS: dict[UUID, dict[UUID, Lock]] = {}
_POOLS: dict[UUID, WorkerPool] = {}
_ROOT = Path(__file__).parent


class ExecutorTerminatedError(Exception):
    """Exception raised when trying to access a terminated executor."""


T = TypeVar("T", bound="Executor")


@define(slots=False, init=False)
class Executor:
    """Executor base class."""

    name: ClassVar[str]
    config: cfg.Config
    uuid: UUID = field(init=False)

    def __init_subclass__(cls, *args: Any, name: str, **kwargs: Any) -> None:
        """Register the class in the registry."""
        super().__init_subclass__(*args, **kwargs)
        cls.name = name or cls.__name__.lower()

    def __init__(self, *args: Any, log_queue: mp.Queue, **kwargs: Any) -> None:
        """Initialize the executor."""
        self.__attrs_init__(*args, **kwargs)
        self.uuid = uuid4()
        _POOLS[self.uuid] = WorkerPool(
            start_method="fork",
            daemon=False,
            use_dill=True,
            shared_objects=log_queue,
        )

    def __enter__(self: T) -> T:
        """Enter the context manager."""
        return self

    def __exit__(self, *args: object) -> None:
        """Exit the context manager."""
        self.terminate()

    @property
    def pool(self) -> WorkerPool:
        """Return the worker pool."""
        try:
            return _POOLS[self.uuid]
        except KeyError as exc:
            raise ExecutorTerminatedError from exc

    @property
    def locks(self) -> dict[UUID, Lock]:
        if self.uuid not in _LOCKS:
            _LOCKS[self.uuid] = {}
        return _LOCKS[self.uuid]

    def _callback(
        self,
        result: Any,
        fn: Callable | None,
        msg: str,
        logger: logging.LoggerAdapter,
        lock: Lock,
    ) -> None:
        """Callback function for the executor."""
        logger.debug(msg)
        try:
            (fn or (lambda _: ...))(result)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"Callback failed: {exc!r}")
        lock.release()

    def _target(
        self,
        log_queue: mp.Queue,
        *args: str | Path,
        name: str,
        uuid: UUID,
        workdir: Path | None,
        env: dict[str, str],
        os_env: bool,
        cpus: int,
        memory: int,
        config: cfg.Config,
        conda_spec: dict | None,
    ) -> None:
        """Target function for the executor."""
        sys.stdout = sys.stderr = open(os.devnull, "w", encoding="utf-8")
        logs.redirect_logging_to_queue(log_queue)
        logs.handle_warnings()
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})

        workdir_ = workdir or config.workdir / uuid.hex
        workdir_.mkdir(parents=True, exist_ok=True)

        env_ = env or {}
        args_ = tuple(word for arg in args for word in shlex.split(str(arg)))
        if conda_spec:
            yaml = YAML(typ="safe")
            (workdir_ / "conda").mkdir(parents=True, exist_ok=True)
            conda_env_spec = workdir_ / "conda" / f"{uuid.hex}.environment.yaml"
            micromamba_bootstrap = _ROOT / "scripts" / "bootstrap_micromamba.sh"
            with open(conda_env_spec, "w") as f:
                yaml.dump(conda_spec, f)
            env_["_CONDA_ENV_SPEC"] = str(conda_env_spec.relative_to(workdir_))
            env_["_CONDA_ENV_NAME"] = f"{uuid.hex}"
            args_ = (str(micromamba_bootstrap), *args_)

        try:
            self.target(
                *args_,
                name=name,
                uuid=uuid,
                workdir=workdir_,
                env={k: str(v) for k, v in env_.items()},
                os_env=os_env,
                cpus=cpus or config.executor.cpus,
                memory=memory or config.executor.memory,
                config=config,
                logger=logger,
            )
        except InterruptWorker as exc:
            logger.debug(f"Terminating job with uuid {uuid}")
            code = self.terminate_hook(uuid, logger)
            raise SystemExit(code or 143) from exc
        except SystemExit as exc:
            if exc.code != 0:
                logger.warning(f"Command failed with exit code: {exc.code}")
                self.terminate_hook(uuid, logger)
                raise exc
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Command failed with exception: {exc!r}")
            self.terminate_hook(uuid, logger)
            raise SystemExit(1) from exc

    def target(
        self,
        *,
        name: str,
        uuid: UUID,
        workdir: Path,
        env: dict,
        os_env: bool,
        cpus: int,
        memory: int,
        config: cfg.Config,
        logger: logging.LoggerAdapter,
    ) -> int | None:  # pragma: no cover
        """Will be called by the executor to execute a command.

        Subclasses should override this method to implement the target execution.

        Args:
        ----
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
        -------
            int | None: The return code of the target execution,
                or None if not applicable.

        Raises:
        ------
            NotImplementedError: If the target execution is not implemented.

        """
        # Exluded from coverage as this is a stub method.
        del name, uuid, workdir, env, os_env, cpus, memory, config, logger  # Unused
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
        conda_spec: dict | None = None,
    ) -> tuple[AsyncResult, UUID]:
        """Submit a job for execution.

        Args:
        ----
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
        -------
            A tuple containing the AsyncResult object and the UUID of the job.

        """
        _uuid = uuid or uuid4()
        _name = name or self.__class__.name
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": _name})
        self.locks[_uuid] = mp.Lock()
        self.locks[_uuid].acquire()

        result = self.pool.apply_async(
            func=self._target,
            args=args,
            kwargs={
                "uuid": _uuid,
                "name": _name,
                "config": self.config,
                "workdir": workdir,
                "env": env,
                "os_env": os_env,
                "cpus": cpus,
                "memory": memory,
                "conda_spec": conda_spec,
            },
            callback=partial(
                self._callback,
                fn=callback,
                msg=f"Job completed: {_uuid}",
                logger=logger,
                lock=self.locks[_uuid],
            ),
            error_callback=partial(
                self._callback,
                fn=error_callback,
                msg=f"Job failed: {_uuid}",
                logger=logger,
                lock=self.locks[_uuid],
            ),
        )
        if wait:
            self.wait(_uuid)
        else:
            # Sleep to ensure jobs are submitted before returning
            sleep(0.1)

        return result, _uuid

    def terminate_hook(self, uuid: UUID, logger: logging.LoggerAdapter) -> int | None:
        """Hook to be called prior to job termination.

        This hook will only run if the job is terminated prior to completion.
        After this hook has been called the job will exit with code 143.

        Args:
        ----
            uuid (UUID): The UUID of the job pending termination.
            logger (logging.LoggerAdapter): A logger adapter for the job.

        """
        del uuid, logger  # Unused
        return 143  # SIGTERM

    def terminate(self) -> None:
        """Terminate all jobs."""
        with suppress(ExecutorTerminatedError):
            self.pool.terminate()
            self.pool.stop_and_join()
            del _POOLS[self.uuid]
        self.wait()

    def wait(self, uuid: UUID | None = None) -> None:
        """Wait for a specific job or all jobs to complete.

        Args:
        ----
            uuid: The UUID of the job to wait for. Defaults to None.

        Returns:
        -------
            None

        """
        if uuid is None:
            for uuid_ in [*self.locks]:
                self.wait(uuid_)
        elif uuid in self.locks:
            self.locks[uuid].acquire()
            self.locks[uuid].release()
            del self.locks[uuid]
