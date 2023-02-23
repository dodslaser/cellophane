"""Utils for running jobs on SGE."""

import multiprocessing as mp
import os
import shlex
import time
from pathlib import Path
from signal import SIGTERM, signal
from typing import Optional

from . import util

drmaa = util.lazy_import("drmaa")


def _cleanup(jid, session):
    """Clean up after a job."""

    # FIXME: Nextflow doesn't kill chlid processes when a job is killed.
    def inner(*_):
        session.control(jid, drmaa.JobControlAction.TERMINATE)
        while session.jobStatus(jid) not in (
            drmaa.JobState.DONE,
            drmaa.JobState.FAILED,
        ):
            time.sleep(1)
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
) -> None:
    _queue = f"-q {queue}" if queue is not None else ""
    _pe = f"-pe {pe} {slots}" if pe is not None else ""
    _args = [word for arg in args for word in shlex.split(arg)]
    _env = {**os.environ, **env} if os_env else env

    with drmaa.Session() as session:
        template = session.createJobTemplate()
        template.remoteCommand = str(script)
        template.args = _args
        template.nativeSpecification = f"-S /bin/bash -notify {_queue} {_pe}"
        template.jobName = name
        template.jobEnvironment = _env
        template.outputPath = f":{stdout}"
        template.errorPath = f":{stderr}"
        template.workingDirectory = str(cwd)
        jid = session.runJob(template)
        session.deleteJobTemplate(template)
        signal(SIGTERM, _cleanup(jid, session))
        try:
            while session.jobStatus(jid) not in (
                drmaa.JobState.DONE,
                drmaa.JobState.FAILED,
            ):
                time.sleep(1)
        except KeyboardInterrupt:
            _cleanup(jid, session)()
        finally:
            job_info = session.wait(jid, drmaa.Session.TIMEOUT_WAIT_FOREVER)
            raise SystemExit(job_info.hasExited and job_info.exitStatus)


def submit(
    script: str,
    *args,
    queue: str,
    pe: str,
    slots: int = 1,
    name: str = __name__,
    env: Optional[dict] = None,
    cwd: Path = Path.cwd(),
    os_env: bool = True,
    stdout: Path = Path("/dev/null"),
    stderr: Path = Path("/dev/null"),
    check: bool = True,
):
    """Submit a job to SGE using DRMAA."""
    proc = mp.Process(
        target=_run,
        args=(script, *args),
        kwargs={
            "queue": queue,
            "pe": pe,
            "slots": slots,
            "name": name,
            "env": env or {},
            "cwd": cwd,
            "os_env": os_env,
            "stdout": stdout,
            "stderr": stderr,
        },
    )

    proc.start()

    if check:
        proc.join()
        if proc.exitcode != 0:
            raise RuntimeError(f"Job {name} failed with exit code {proc.exitcode}")
    return proc
