"""Testing utilities for Cellophane."""

import logging
from pathlib import Path
from shutil import copy, copytree
from typing import Any

from click.testing import CliRunner
from pytest import FixtureRequest, LogCaptureFixture, fixture, mark, param
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
    (root / "modules").mkdir(parents=True, exist_ok=True)
    (root / "schema.yaml").touch(exist_ok=True)
    copy(
        Path(__file__).parent / "instrumentation.py",
        root / "modules" / "instrumentation.py",
    )

    for path, content in structure.items():
        if isinstance(content, dict):
            (root / path).mkdir(parents=True, exist_ok=True)
            _create_structure(root / path, content)
        else:
            (root / path).write_text(content)

    for src, dst in (external or {}).items():
        _src = Path(src)
        if not _src.is_absolute():
            _src = (external_root / src).resolve()
        (root / dst).symlink_to(_src)


def _execute_definition(
    root: Path,
    mocks: dict[str, dict[str, Any] | None],
    args: list[str] | None,
    caplog: LogCaptureFixture,
    mocker: MockerFixture,
    runner: CliRunner,
    exception: Exception | None,
    logs: list[str] | None,
):
    _args = [i for p in (args or []).items() for i in p if i is not None]

    try:
        _main = cellophane.cellophane("DUMMY", root=root)
        for target, mock in (mocks or {}).items():
            mocker.patch(
                target=target,
                side_effect=Exception(e)
                if (e := (mock or {}).get("exception", False))
                else None,
                **(mock or {}).get("kwargs", {}),
            )
        _result = runner.invoke(_main, _args)
    except (SystemExit, Exception) as e:  # pylint: disable=broad-except
        assert repr(e) == exception or repr(None)
    else:
        assert repr(_result.exception) == exception or repr(None)
    finally:
        for log_line in logs or []:
            assert log_line in "\n".join(caplog.messages)


def parametrize_from_yaml(paths: list[Path]) -> callable:
    """Parametrize a test from a YAML file."""

    def wrapper(func: callable) -> callable:
        @mark.parametrize(
            "definition",
            [
                param(definition, id=definition.get("id", path.stem))
                for path in paths
                for definition in _YAML.load_all(path)
            ],
        )
        def inner(
            definition: dict[str, str | dict[str, str]],
            run_definition: callable,
        ):
            func(definition, run_definition)

        return inner

    return wrapper


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
        with (
            _runner.isolated_filesystem(tmp_path) as td,
            caplog.at_level(logging.DEBUG),
        ):
            _create_structure(
                root=Path(td),
                structure=definition.get("structure", {}),
                external_root=_extenal_root,
                external=definition.get("external", None),
            )
            _execute_definition(
                root=Path(td),
                mocks=definition.get("mocks", {}),
                args=definition.get("args", None),
                caplog=caplog,
                mocker=mocker,
                runner=_runner,
                exception=definition.get("exception", None),
                logs=definition.get("logs", None),
            )

    yield inner
    logging.getLogger().handlers = _handlers
