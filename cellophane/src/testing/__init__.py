"""Testing utilities for Cellophane."""

import logging
from pathlib import Path
from shutil import copytree
from unittest.mock import MagicMock

from click.testing import CliRunner
from pytest import LogCaptureFixture, MonkeyPatch, fixture
from ruamel.yaml import YAML

import cellophane

_YAML = YAML(typ="safe", pure=True)

def _create_structure(structure: dict, root: Path):
    for path, content in structure.items():
        (root / "modules").mkdir(parents=True, exist_ok=True)
        (root / "schema.yaml").touch(exist_ok=True)
        copytree(
            Path(__file__).parent / "instrumentation",
            root / "modules" / "instrumentation",
            dirs_exist_ok=True,
        )

        if isinstance(content, dict):
            (root / path).mkdir(parents=True, exist_ok=True)
            _create_structure(content, root / path)
        else:
            (root / path).write_text(content)


@fixture
def run_definition(tmp_path: Path, caplog: LogCaptureFixture):
    """Run a cellophane wrapper from a definition YAML file."""
    # FIXME: Check output
    _runner = CliRunner()
    _handlers = logging.getLogger().handlers
    logging.getLogger().handlers = []

    def inner(definition: Path):
        _definition = _YAML.load(definition)
        _args = [i for p in _definition["args"].items() for i in p if i is not None]

        try:
            with _runner.isolated_filesystem(tmp_path) as td:
                _create_structure(_definition["structure"], Path(td))
                _main = cellophane.cellophane("DUMMY", root=Path(td))
                _runner.invoke(_main, _args)
        except SystemExit as e:
            assert repr(e) == _definition.get("exception", None)
        finally:
            for log_line in _definition.get("logs", []):
                assert log_line in "\n".join(caplog.messages)

    yield inner
    logging.getLogger().handlers = _handlers
