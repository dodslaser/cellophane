import logging
from cellophane.src import logs

from pathlib import Path


class Test_add_file_handler:
    @staticmethod
    def test_add_file_handler(tmp_path: Path):
        _logger = logging.getLogger("TEST")
        _adapter = logging.LoggerAdapter(_logger)
        _path = tmp_path / "test.log"
        logs.add_file_handler(_adapter, _path)
        _logger.info("TEST")
        assert _path.exists()
        assert any(
            isinstance(_handler, logging.FileHandler)
            and _handler.baseFilename == str(_path)
            for _handler in _logger.handlers
        )


class Test_get_logger:
    @staticmethod
    def test_get_labeled_adapter():
        _logger = logs.get_labeled_adapter("TEST")

        assert isinstance(_logger, logging.LoggerAdapter)
        assert _logger.logger.name == "cellophane"
        assert _logger.extra["label"] == "TEST"
