import time
from unittest.mock import MagicMock

from cellophane import executors


class Test_SubprocessExecutor:
    def test_executor(self, tmp_path):
        config = MagicMock(workdir=tmp_path, logdir=tmp_path)
        spe = executors.SubprocesExecutor(config=config)
        proc, _ = spe.submit("sleep .2", name="sleep", wait=True)
        assert not proc.is_alive()
        assert proc.exitcode == 0

    def test_executor_terminate(self, tmp_path):
        config = MagicMock(workdir=tmp_path, logdir=tmp_path)
        spe = executors.SubprocesExecutor(config=config)
        proc, uuid = spe.submit("sleep 1", name="sleep")
        time.sleep(.1)
        assert proc.is_alive()
        spe.terminate(uuid)
        spe.join(uuid)
        assert not proc.is_alive()
        assert proc.exitcode == 15  # SIGTERM

    def test_executor_terminate_all(self, tmp_path):
        config = MagicMock(workdir=tmp_path, logdir=tmp_path)
        spe = executors.SubprocesExecutor(config=config)
        procs = [
            spe.submit("sleep 1", name="sleep")[0],
            spe.submit("sleep 1", name="sleep")[0],
            spe.submit("sleep 1", name="sleep")[0],
        ]
        time.sleep(.1)
        assert all(p.is_alive() for p in procs)
        spe.terminate()
        spe.join()
        assert not any(p.is_alive() for p in procs)
