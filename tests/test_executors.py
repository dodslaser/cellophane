"""Test cellphane.src.executors."""

import logging
import multiprocessing as mp
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from mpire import WorkerPool
from pytest import LogCaptureFixture, raises
from pytest_mock import MockerFixture

from cellophane import data, executors, logs


class Test_SubprocessExecutor:
    """Test SubprocessExecutor."""

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

    @staticmethod
    def test_command_exception(
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
        tmp_path: Path,
    ) -> None:
        """Test command exception."""

        mocker.patch(
            "cellophane.executors.sp.Popen",
            side_effect=Exception("DUMMY"),
        )

        config = data.Container(
            workdir=tmp_path,
            logdir=tmp_path,
            executor={"cpus": 1, "memory": 1},
        )

        _log_queue = logs.start_queue_listener()

        with (
            WorkerPool(
                daemon=False,
                use_dill=True,
            ) as pool,
            caplog.at_level("DEBUG"),
            raises(SystemExit) as exception,
        ):
            spe = executors.SubprocesExecutor(
                config=config,  # type: ignore[arg-type]
                pool=pool,
                log_queue=_log_queue,
            )

            result = spe.submit("exit", name="exception")[0]

            spe.wait()
            result.get()

        assert repr(exception.value) == "SystemExit(1)"
        assert "Command failed with exception: Exception('DUMMY')" in caplog.messages

    @staticmethod
    def test_wait_for_uuid(tmp_path: Path) -> None:
        """Test wait_for_uuid."""
        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            spe = executors.SubprocesExecutor(
                config=data.Container(  # type: ignore[arg-type]
                    workdir=tmp_path,
                    logdir=tmp_path,
                    executor={"cpus": 1, "memory": 1},
                ),
                pool=pool,
                log_queue=mp.Queue(),
            )
            result1, uuid1 = spe.submit("sleep .1", name="sleep")
            result2, _ = spe.submit("sleep 2", name="sleep")

            spe.wait(uuid1)
            spe.terminate()

            assert result1.ready()
            assert not result2.successful()
