"""Utils for running jobs on SGE."""

import multiprocessing as mp
import os
import shlex
import sys
import time
from pathlib import Path
from signal import SIGTERM, signal
from typing import Any, Callable
from uuid import UUID, uuid4

import drmaa2

from . import cfg


def _cleanup(job: drmaa2.Job, session: drmaa2.JobSession) -> Callable:
    """Clean up after a job."""

    # FIXME: Nextflow doesn't kill chlid processes when a job is killed.
    def inner(*args: Any) -> None:
        del args  # Unused.
        job.terminate()
        job.wait_terminated()
        session.close()
        session.destroy()
        raise SystemExit(1)

    return inner


def _run(
    script: str,
    *args: Any,
    logdir: Path,
    uuid: UUID,
    queue: str | None,
    pe: str | None,
    slots: int | None,
    name: str,
    env: dict,
    cwd: Path,
    os_env: bool = True,
    callback: Callable,
    error_callback: Callable,
) -> None:
    (logdir / "sge").mkdir(exist_ok=True)

    _args = [word for arg in args for word in shlex.split(arg)]
    _impl_args = f"-l excl=1 -S /bin/bash -notify -q {queue} {'-V' if os_env else ''}"
    _env = {k: str(v) for k, v in env.items()}

    sys.stdout = open(os.devnull, "w", encoding="utf-8")
    sys.stderr = open(os.devnull, "w", encoding="utf-8")
    try:
        session = drmaa2.JobSession(f"{name}_{uuid.hex}")
        # FIXME: Improve error logging when DRMAA2 fails to submit a job.
        job = session.run_job(
            {
                "remote_command": script,
                "args": _args,
                "min_slots": slots,
                "implementation_specific": {
                    "uge_jt_pe": pe,
                    "uge_jt_native": _impl_args,
                },
                "job_name": f"{name}_{uuid.hex[:8]}",
                "job_environment": _env,
                "output_path": str(logdir / "sge" / f"{name}.{uuid.hex}.out"),
                "error_path": str(logdir / "sge" / f"{name}.{uuid.hex}.err"),
                "working_directory": str(cwd),
            }
        )
    except Exception as e:
        with open(
            logdir / "sge" / f"{name}.{uuid.hex}.sge_err",
            mode="w",
            encoding="utf-8",
        ) as f:
            f.write(str(e))
        if error_callback is not None:
            error_callback(e)
        raise SystemExit(1) from e

    try:
        signal(SIGTERM, _cleanup(job, session))
        state = None
        while state not in (
            drmaa2.JobState.DONE,
            drmaa2.JobState.FAILED,
        ):
            state, _ = job.get_state()
            time.sleep(1)
    except KeyboardInterrupt:
        error_callback(RuntimeError("Job killed by user"))
        _cleanup(job, session)()
    else:
        job_info = job.get_info()
        session.close()
        session.destroy()
        if job_info.exit_status != 0 and error_callback is not None:
            error_callback(RuntimeError(job_info.exit_status))
        elif callback is not None:
            callback()
        raise SystemExit(job.get_info().exit_status)


def submit(
    script: str,
    *args: Any,
    name: str = __name__,
    config: cfg.Config,
    uuid: UUID | None = None,
    queue: str | None = None,
    pe: str | None = None,
    slots: int | None = None,
    env: dict | None = None,
    cwd: Path = Path.cwd(),
    os_env: bool = True,
    check: bool = True,
    callback: Callable | None = None,
    error_callback: Callable | None = None,
) -> mp.Process:
    """
    Submits a job for execution on SGE.

    Args:
        script (str): The path to the script to be executed.
        *args: Additional positional arguments for the script.
        name (str): The name of the job. Defaults to the current module name.
        config (cfg.Config): The configuration object.
        uuid (UUID | None): The UUID of the job. Defaults to a new UUID.
        queue (str | None): The queue for the job.
            Defaults to the queue specified in the configuration.
        pe (str | None): The parallel environment for the job.
            Defaults to the parallel environment specified in the configuration.
        slots (int | None): The number of slots for the job.
            Defaults to the number of slots specified in the configuration.
        env (dict | None): Additional environment variables for the job.
            Defaults to an empty dictionary.
        cwd (Path): The current working directory for the job.
            Defaults to the current working directory.
        os_env (bool): Whether to include the current operating system environment
            variables for the job. Defaults to True.
        check (bool): Whether to wait and check the exit code of the job.
            Defaults to True.
        callback (Callable | None): A callback function to be called after the job
            completes successfully.
        error_callback (Callable | None): A callback function to be called if the job
            fails.

    Returns:
        multiprocessing.Process: The process object representing the submitted job.

    Raises:
        RuntimeError: Raised when the job fails with a non-zero exit code
            if `check` is True.

    Example:
        ```python
        script = "/path/to/script.py"
        args = ("arg1", "arg2")
        config = cfg.Config(...)
        job = submit(script, *args, config=config)
        job.join()
        ```
    """
    if uuid is None:
        uuid = uuid4()

    proc = mp.Process(
        target=_run,
        args=(script, *args),
        kwargs={
            "logdir": config.logdir,
            "queue": queue or config.sge.queue,
            "slots": slots or config.sge.slots,
            "pe": pe or config.sge.pe,
            "name": name,
            "uuid": uuid,
            "env": (env or {}),
            "cwd": cwd,
            "os_env": os_env,
            "callback": callback,
            "error_callback": error_callback,
        },
    )

    proc.start()

    if check:
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(f"Job {name} failed with exit code {proc.exitcode}")

    return proc
