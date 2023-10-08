"""Utils for running jobs on SGE."""

import multiprocessing as mp
import os
import shlex
import sys
import time
from pathlib import Path
from signal import SIGTERM, signal
from typing import Callable, Optional
from uuid import UUID, uuid4

import drmaa2

from . import cfg


def _cleanup(job, session):
    """Clean up after a job."""

    # FIXME: Nextflow doesn't kill chlid processes when a job is killed.
    def inner(*_):
        job.terminate()
        job.wait_terminated()
        session.close()
        session.destroy()
        raise SystemExit(1)

    return inner


def _run(
    script: str,
    *args,
    logdir: Path,
    uuid: UUID,
    queue: Optional[str],
    pe: Optional[str],
    slots: Optional[int],
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
    *args,
    name: str = __name__,
    config: cfg.Config,
    uuid: Optional[UUID] = None,
    queue: Optional[str] = None,
    pe: Optional[str] = None,
    slots: Optional[int] = None,
    env: Optional[dict] = None,
    cwd: Path = Path.cwd(),
    os_env: bool = True,
    check: bool = True,
    callback: Optional[Callable] = None,
    error_callback: Optional[Callable] = None,
):
    """Submit a job to SGE using DRMAA."""
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
