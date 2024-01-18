"""Test cellphane.src.executors."""

import multiprocessing as mp
import time
from pathlib import Path
from unittest.mock import MagicMock

from mpire import WorkerPool

from cellophane import data, executors


class Test_SubprocessExecutor:
    """Test SubprocessExecutor."""
    def test_executor(self, tmp_path: Path) -> None:
        """Test SubprocessExecutor."""
        config = data.Container(
            workdir=tmp_path,
            logdir=tmp_path,
            executor={"cpus": 1, "memory": 1},
        )
        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            spe = executors.SubprocesExecutor(
                config=config,
                pool=pool,
                log_queue=mp.Queue(),
            )

            result, _ = spe.submit("sleep .2", name="sleep", wait=True)

            assert result.ready()
            assert result.successful()

    def test_callback(self, tmp_path: Path) -> None:
        """Test callback."""
        config = data.Container(
            workdir=tmp_path,
            logdir=tmp_path,
            executor={"cpus": 1, "memory": 1},
        )
        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            spe = executors.SubprocesExecutor(
                config=config,  # type: ignore[arg-type]
                pool=pool,
                log_queue=mp.Queue(),
            )
            _callback = MagicMock()
            _error_callback = MagicMock()
            assert not _callback.called
            assert not _error_callback.called
            spe.submit(
                "sleep 0",
                name="sleep",
                wait=True,
                callback=_callback,
                error_callback=_error_callback,
            )
            assert _callback.called
            assert not _error_callback.called
            spe.submit(
                "exit 42",
                name="exception",
                wait=True,
                callback=_callback,
                error_callback=_error_callback,
            )
            assert _error_callback.called

    def test_executor_terminate_all(self, tmp_path: Path) -> None:
        """Test that all processes are terminated when the executor is terminated."""
        config = data.Container(
            workdir=tmp_path,
            logdir=tmp_path,
            executor={"cpus": 1, "memory": 1},
        )
        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            spe = executors.SubprocesExecutor(
                config=config,  # type: ignore[arg-type]
                pool=pool,
                log_queue=mp.Queue(),
            )
            results = [
                spe.submit("sleep 1", name="sleep")[0],
                spe.submit("sleep 1", name="sleep")[0],
                spe.submit("sleep 1", name="sleep")[0],
            ]
            time.sleep(0.1)
            assert not any(r.ready() for r in results), [r.get() for r in results]
            spe.terminate()
            spe.wait()
            assert all(r.ready() for r in results)
