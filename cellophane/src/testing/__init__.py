"""Testing utilities for Cellophane."""

import logging
from pathlib import Path
from shutil import copy, copytree

from click.testing import CliRunner
from pytest import FixtureRequest, LogCaptureFixture, fixture
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

import cellophane

_YAML = YAML(typ="safe", pure=True)


def _create_structure(
    root: Path,
    structure: dict[str, str | dict[str, str]],
    external_root: Path | None = None,
    external: dict[str, str] | None = None,
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
        _src = Path(src)
        if not _src.is_absolute():
            _src = (external_root / src).resolve()
        if _src.is_dir():
            copytree(_src, root / dst)
        else:
            copy(_src, root / dst)


@fixture
def run_definition(
    tmp_path: Path,
    caplog: LogCaptureFixture,
    request: FixtureRequest,
    mocker: MockerFixture,
):
    """Run a cellophane wrapper from a definition YAML file."""
    # FIXME: Check output
    _runner = CliRunner()
    _handlers = logging.getLogger().handlers
    _extenal_root = Path(request.fspath).parent
    logging.getLogger().handlers = []

    def inner(definition: Path):
        _definition = _YAML.load(definition)
        _args = [i for p in _definition["args"].items() for i in p if i is not None]
        for target, mock in _definition.get("mocks", {}).items():
            mocker.patch(
                target=target,
                side_effect=Exception if mock.get("raise_exception", False) else None,
                **mock.get("kwargs", {}),
            )
        try:
            with _runner.isolated_filesystem(tmp_path) as td:
                _create_structure(
                    root=Path(td),
                    structure=_definition["structure"],
                    external_root=_extenal_root,
                    external=_definition.get("external", None),
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
