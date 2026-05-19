"""Executors for running external scripts as jobs."""
from __future__ import annotations

import logging
import os
import platform
import shlex
import sys
from inspect import signature
from multiprocessing import Lock
from pathlib import Path
from shutil import copytree
from time import sleep
from typing import TYPE_CHECKING, ClassVar
from uuid import uuid4
from warnings import warn

from attrs import define, field
from mpire import WorkerPool
from mpire.exception import InterruptWorker

from cellophane.executors.environment import build_environment
from cellophane.logs import handle_warnings, redirect_logging_to_queue

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from multiprocessing.synchronize import Lock as LockType
    from typing import Any, Callable, TypeVar
    from uuid import UUID

    from mpire.async_result import AsyncResult

    from cellophane.cfg import Config
    T = TypeVar("T", bound="Executor")


_LOCKS: dict[UUID, dict[UUID, LockType]] = {}
_POOLS: dict[UUID, WorkerPool] = {}
_ROOT = Path(__file__).parent

class ExecutorSubmitException(Exception):
    """Exception raised when trying to access a terminated executor."""


@define(slots=False, init=False)
class Executor:
    """Executor base class."""
    environment_lock: ClassVar[LockType] = Lock()
    name: ClassVar[str]
    config: Config
    workdir_base: Path
    _locks: dict[UUID, LockType] = field(init=False)
    _log_queue: Queue = field(repr=False, init=False)
    _pool: WorkerPool | None = field(default=None, init=False)
    _pools: dict[UUID, WorkerPool] = field(factory=dict, init=False, repr=False)
    _dispatcher: Any = field(default=None, init=False, repr=False)

    def __init_subclass__(
        cls,
        *args: Any,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Register the class in the registry."""
        super().__init_subclass__(*args, **kwargs)
        if name is not None:
            cls.name = name
        elif not hasattr(cls, "name"):
            cls.name = cls.__name__.lower()

    def __init__(self, *args: Any, log_queue: Queue, dispatcher: Any = None, **kwargs: Any) -> None:
        """Initialize the executor."""
        self.__attrs_init__(*args, **kwargs)  # ty: ignore[unresolved-attribute]
        self._locks = {}
        self._log_queue = log_queue
        self._pool = None
        self._dispatcher = dispatcher

    def __getstate__(self) -> dict[str, Any]:
        return self.__dict__ | {
            "_pool": None,
            "_log_queue": None,
            "_locks": {},
            "_pools": {},
        }

    def __enter__(self: T) -> T:
        """Enter the context manager."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit the context manager."""
        self.terminate()

    @staticmethod
    def _callback(
        fn: Callable | None,
        *,
        executor_name: str,
        job_name: str,
        uuid: UUID,
        logger: logging.LoggerAdapter,
        lock: LockType,
        dispatcher: Any,
        pool: WorkerPool,
    ) -> Callable[[Any | BaseException], None]:
        """Callback function for the executor."""

        def inner(result_or_exception: Any | BaseException) -> None:
            try:
                match result_or_exception:
                    case BaseException() as exc:
                        logger.debug(f"Error in {executor_name} job '{job_name}' (UUID={uuid.hex[:8]}): {exc!r}")
                        dispatcher.run_exception_hooks(exception=exc, pool=pool)
                    case _:
                        logger.debug(f"Completed {executor_name} job '{job_name}' (UUID={uuid.hex[:8]})")
                if fn is not None:
                    fn(result_or_exception)
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    f"Unhandled exception in callback of {executor_name} job '{job_name}' (UUID={uuid.hex[:8]}): {exc!r}",
                    exc_info=exc,
                )
                dispatcher.run_exception_hooks(exception=exc, pool=pool)
            finally:
                lock.release()

        return inner

    def _target(
        self,
        log_queue: Queue,
        *args: str | Path,
        name: str,
        uuid: UUID,
        workdir: Path | None,
        env: dict[str, str],
        os_env: bool,
        cpus: int,
        memory: int,
        config: Config,
        conda_spec: dict | None,
    ) -> None:
        """Target function for the executor."""
        self._log_queue = log_queue
        sys.stdout = sys.stderr = open(os.devnull, "w", encoding="utf-8")
        redirect_logging_to_queue(log_queue)
        handle_warnings()
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": name})

        _workdir = workdir or self.workdir_base / f"{name}.{uuid.hex}.{self.name}"
        _workdir.mkdir(parents=True, exist_ok=True)

        stdout_path = _workdir / f"{name}.{uuid.hex}.{self.name}.stdout"
        stderr_path = _workdir / f"{name}.{uuid.hex}.{self.name}.stderr"

        _env = env or {}
        _args = tuple(word for arg in args for word in shlex.split(str(arg)))
        if conda_spec:
            env_path = build_environment(
                conda_spec=conda_spec,
                platform=config.executor.environment.get("platform", None),
                environment_lock=self.environment_lock,
                path=config.executor.environment.cache,
                logger=logger,
            )
            if config.executor.environment.copy_to_workdir:
                local_env_path = (_workdir / env_path.name).absolute()
                copytree(env_path, local_env_path, dirs_exist_ok=True)
            else:
                local_env_path = env_path.absolute()

            if "PATH" in _env:
                _env["PATH"] = f"{local_env_path}/bin:{_env['PATH']}"
            elif os_env and (os_path := os.environ.get("PATH")) is not None:
                _env["PATH"] = f"{local_env_path}/bin:{os_path}"
            else:
                _env["PATH"] = f"{local_env_path}/bin"

            if "CONDA_SHLVL" in _env:
                logger.warning("Environment variable 'CONDA_SHLVL' passed to executor will be overridden to ensure proper conda environment activation.")
            _env["CONDA_SHLVL"] = "1"

            if "CONDA_PREFIX" in _env:
                logger.warning("Environment variable 'CONDA_PREFIX' passed to executor will be overridden to ensure proper conda environment activation.")
            _env["CONDA_PREFIX"] = f"{env_path.name}"

        try:
            logger.debug(f"Starting {self.name} job '{name}' (UUID={uuid.hex[:8]})")
            target_signature = signature(self.target).parameters
            kwargs = {
                "name": name,
                "uuid": uuid,
                "workdir": _workdir,
                "env": {k: str(v) for k, v in _env.items()},
                "os_env": os_env,
                "cpus": cpus or config.executor.cpus,
                "memory": memory or config.executor.memory,
                "config": config,
                "logger": logger,
                "stderr": stderr_path,
                "stdout": stdout_path,
            }

            if any(arg not in target_signature for arg in ["stdout", "stderr"]):
                kwargs.pop("stderr")
                kwargs.pop("stdout")
                warn(
                    f"The target method of executor '{self.name}' does not accept 'stdout' and 'stderr' arguments. "
                    "These arguments define 'pathlib.Path' objects pointing where cellophane expects an executor to "
                    "write the standard output and error streams of the job. In the next major release of cellophane, "
                    "this will raise an exception.",
                    category=PendingDeprecationWarning,
                )

            self.target(*_args, **kwargs)  # type: ignore[arg-type]
        except InterruptWorker as exc:
            logger.debug(f"Terminating {self.name} job '{name}' (UUID={uuid.hex[:8]})")
            code = self.terminate_hook(uuid, logger)
            raise SystemExit(code or 143) from exc
        except SystemExit as exc:
            if exc.code != 0:
                logger.warning(f"Non-zero exit code ({exc.code}) for {self.name} job '{name}' (UUID={uuid.hex[:8]})")
                self.terminate_hook(uuid, logger)
                raise exc
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Unhandled exception in {self.name} job '{name}' (UUID={uuid.hex[:8]}): {exc!r}", exc_info=exc)
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
        config: Config,
        logger: logging.LoggerAdapter,
        stderr: Path,
        stdout: Path,
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
        del name, uuid, workdir, env, os_env, cpus, memory, config, logger, stdout, stderr  # Unused
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
        if _uuid in self._locks or _uuid in self._pools:
            raise ExecutorSubmitException(f"Job with UUID {_uuid} is already submitted.")

        _name = name or f"{self.__class__.name}_job"
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": _name})
        self._locks[_uuid] = Lock()
        self._locks[_uuid].acquire()
        self._pools[_uuid] = WorkerPool(
            n_jobs=1,
            start_method="fork",
            daemon=False,
            use_dill=True,
            shared_objects=self._log_queue,
        )

        result = self._pools[_uuid].apply_async(
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
            callback=self._callback(
                callback,
                executor_name=self.name,
                job_name=_name,
                uuid=_uuid,
                logger=logger,
                lock=self._locks[_uuid],
                dispatcher=self._dispatcher,
                pool=self._pools[_uuid],
            ),
            error_callback=self._callback(
                error_callback,
                executor_name=self.name,
                job_name=_name,
                uuid=_uuid,
                logger=logger,
                lock=self._locks[_uuid],
                dispatcher=self._dispatcher,
                pool=self._pools[_uuid],
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

    def terminate(self, uuid: UUID | None = None, wait: bool = True) -> None:
        """Terminate a specific job or all jobs."""
        if uuid is None:
            for _uuid in [*self._pools]:
                self.terminate(_uuid, wait=False)
            self.wait()
        elif uuid in self._pools and uuid in self._locks:
            self._pools[uuid].terminate()
            if wait:
                self.wait(uuid)

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
            for uuid_ in [*self._pools]:
                self.wait(uuid_)
        elif uuid in self._pools and uuid in self._locks:
            self._locks[uuid].acquire()
            self._locks[uuid].release()
            self._pools[uuid].stop_and_join()
            del self._locks[uuid]
            del self._pools[uuid]
