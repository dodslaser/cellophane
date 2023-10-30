# pragma: no cover

"""Testing utilities for Cellophane."""

import logging
import traceback
from pathlib import Path
from shutil import copy
from typing import Any

from click.testing import CliRunner, Result
from pytest import FixtureRequest, LogCaptureFixture, fail, fixture, mark, param
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

import cellophane

_YAML = YAML(typ="unsafe", pure=True)


def _create_structure(
    root: Path,
    structure: dict[str, str | dict[str, str]],
    external_root: Path | None = None,
    external: dict[str, str] | None = None,
) -> None:
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


def _fail_from_click_result(result: Result, msg: str) -> None:
    if result:
        fail(
            pytrace=False,
            msg=(
                f"{msg}\n"
                f"Exit code: {result.exit_code}\n"
                f"Output:\n{result.output}"
            ),
        )
    else:
        fail(pytrace=False, msg=msg)


def _execute_from_structure(
    root: Path,
    mocks: dict[str, dict[str, Any] | None],
    args: list[str] | None,
    caplog: LogCaptureFixture,
    mocker: MockerFixture,
    runner: CliRunner,
    exception: Exception | None,
    logs: list[str] | None,
    output: list[str] | None,
):
    _args = [i for p in (args or []).items() for i in p if i is not None]

    try:
        mocker.patch("cellophane.logs.setup_logging")
        _main = cellophane.cellophane("DUMMY", root=root)
        for target, mock in (mocks or {}).items():
            _side_effect = (
                exc()
                if isinstance(exc := (mock or {}).get("exception"), type)
                and issubclass(exc, BaseException)
                else Exception(exc) if exc
                else None
            )
            mocker.patch(
                target=target,
                side_effect=_side_effect,
                **(mock or {}).get("kwargs", {}),
            )
        _result = runner.invoke(_main, _args)
    except (SystemExit, Exception) as e:  # pylint: disable=broad-except
        _exception = e
        _result = None
    else:
        _exception = _result.exception

    if repr(_exception) != (exception or repr(None)):
        _fail_from_click_result(
            result=_result,
            msg=(
                "Unexpected exception\n"
                f"Expected: {exception}\n"
                f"Received: {repr(_exception)}\n"
                f"Traceback: {''.join(traceback.format_exception(_exception))}"
            ),
        )

    for log_line in logs or []:
        if log_line not in "\n".join(caplog.messages):
            _fail_from_click_result(
                result=_result,
                msg=("Log message not found\n" f"Missing line:\n{log_line}"),
            )

    for output_line in output or []:
        if output_line not in _result.output:
            _fail_from_click_result(
                result=_result,
                msg=("Command output not found\n" f"Missing output:\n{output_line}"),
            )

    return _result


def parametrize_from_yaml(paths: list[Path]) -> callable:
    """Parametrize a test from a YAML file."""

    def wrapper(func: callable) -> callable:
        return mark.parametrize(
            "definition",
            [
                param(definition, id=definition.get("id", path.stem))
                for path, documents in [(p, _YAML.load_all(p)) for p in paths]
                for definitions in documents
                for definition in (
                    definitions if isinstance(definitions, list) else [definitions]
                )
            ],
        )(func)

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
            _execute_from_structure(
                root=Path(td),
                mocks=definition.get("mocks", {}),
                args=definition.get("args", None),
                caplog=caplog,
                mocker=mocker,
                runner=_runner,
                exception=definition.get("exception", None),
                logs=definition.get("logs", None),
                output=definition.get("output", None),
            )

    yield inner
    logging.getLogger().handlers = _handlers
