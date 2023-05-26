"""Utils for running jobs on SGE."""

import multiprocessing as mp
import os
import sys
import shlex
import time
from pathlib import Path
from signal import SIGTERM, signal
from typing import Optional, Callable

from . import util, cfg

drmaa2 = util.lazy_import("drmaa2")


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
    queue: Optional[str],
    pe: Optional[str],
    slots: Optional[int],
    name: str,
    env: dict,
    cwd: Path,
    os_env: bool = True,
    stdout: Path,
    stderr: Path,
    callback: Callable,
    error_callback: Callable
) -> None:
    _args = [word for arg in args for word in shlex.split(arg)]
    _impl_args = f"-l excl=1 -S /bin/bash -notify -q {queue} {'-V' if os_env else ''}"
    _env = {k: str(v) for k, v in env.items()}

    null = open(os.devnull, 'w')
    sys.stdout = null
    sys.stderr = null

    session = drmaa2.JobSession()
    job = session.run_job(
        {
            "remote_command": str(script),
            "args": _args,
            "min_slots": slots,
            "implementation_specific": {
                "uge_jt_pe": pe,
                "uge_jt_native": _impl_args
            },
            "job_name": name,
            "job_environment": _env,
            "output_path": str(stdout),
            "error_path": str(stderr),
            "working_directory": str(cwd),
        }
    )
    signal(SIGTERM, _cleanup(job, session))
    try:
        state = None
        while state not in (
            drmaa2.JobState.DONE,
            drmaa2.JobState.FAILED,
        ):
            time.sleep(1)
            state, _ = job.get_state()
    except KeyboardInterrupt:
        _cleanup(job, session)()
        raise SystemExit(1)
    finally:
        job_info = job.get_info()
        session.close()
        session.destroy()
        if job_info.exit_status != 0 and error_callback is not None:
            error_callback(job_info.exit_status)
        elif callback is not None:
            callback()
        raise SystemExit(job_info.exit_status)


def submit(
    script: str,
    *args,
    name: str = __name__,
    config: cfg.Config,
    queue: Optional[str] = None,
    pe: Optional[str] = None,
    slots: Optional[int] = None,
    env: Optional[dict] = None,
    cwd: Path = Path.cwd(),
    os_env: bool = True,
    stdout: Path = Path("/dev/null"),
    stderr: Path = Path("/dev/null"),
    check: bool = True,
    callback: Optional[Callable] = None,
    error_callback: Optional[Callable] = None,
):
    """Submit a job to SGE using DRMAA."""
    proc = mp.Process(
        target=_run,
        args=(script, *args),
        kwargs={
            "queue": queue or config.sge.queue,
            "slots": slots or config.sge.slots,
            "pe": pe or config.sge.pe,
            "name": name,
            "env": (env or {}),
            "cwd": cwd,
            "os_env": os_env,
            "stdout": stdout,
            "stderr": stderr,
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
