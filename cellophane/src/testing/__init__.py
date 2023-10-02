"""Testing utilities for Cellophane."""

import logging
from pathlib import Path
from shutil import copy, copytree

from click.testing import CliRunner
from pytest import LogCaptureFixture, fixture
from ruamel.yaml import YAML

import cellophane

_YAML = YAML(typ="safe", pure=True)


def _create_structure(
    root: Path,
    structure: dict[str, str | dict[str, str]],
    external: dict[Path] | None = None,
):
    for path, content in structure.items():
        (root / "modules").mkdir(parents=True, exist_ok=True)
        (root / "schema.yaml").touch(exist_ok=True)
        copy(
            Path(__file__).parent / "instrumentation.py",
            root / "modules" / "instrumentation.py",
        )

        if isinstance(content, dict):
            (root / path).mkdir(parents=True, exist_ok=True)
            _create_structure(root / path, content)
        else:
            (root / path).write_text(content)

    for src, dst in (external or {}).items():
        if src.is_dir():
            copytree(src, root / dst)
        else:
            copy(src, root / dst)


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
                _create_structure(
                    Path(td),
                    _definition["structure"],
                    _definition.get("external", None),
                )
                _main = cellophane.cellophane("DUMMY", root=Path(td))
                _runner.invoke(_main, _args)
        except SystemExit as e:
            assert repr(e) == _definition.get("exception", None)
        finally:
            for log_line in _definition.get("logs", []):
                assert log_line in "\n".join(caplog.messages)

    yield inner
    logging.getLogger().handlers = _handlers
