# pragma: no cover

"""Testing utilities for Cellophane."""

import logging
import traceback
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from click.testing import CliRunner, Result
from coverage import Coverage
from pytest import LogCaptureFixture, fail, mark, param
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

import cellophane

_YAML = YAML(typ="unsafe", pure=True)


def create_structure(
    root: Path,
    structure: dict,
    external_root: Path,
    external: dict[str, str] | None = None,
) -> None:
    """Create a directory structure from definition YAML."""
    (root / "modules").mkdir(parents=True, exist_ok=True)
    (root / "schema.yaml").touch(exist_ok=True)
    for path, content in structure.items():
        if isinstance(content, dict):
            (root / path).mkdir(parents=True, exist_ok=True)
            create_structure(root / path, content, external_root)
        else:
            (root / path).write_text(content)
            (root / path).chmod(0o755)

    for src, dst in (external or {}).items():
        _src = Path(src)
        if not _src.is_absolute():
            _src = (external_root / src).resolve()
        (root / dst).symlink_to(_src)


def fail_from_click_result(result: Result | None, reason: str) -> None:
    """Fail a test with a message and a click result."""
    if result:
        fail(
            pytrace=False,
            reason=(
                f"{reason}\n"
                f"Exit code: {result.exit_code}\n"
                f"Output:\n{result.output}"
            ),
        )
    else:
        fail(pytrace=False, reason=reason)


def execute_from_structure(
    root: Path,
    mocks: dict[str, dict[str, Any] | None],
    args: dict[str, str] | None,
    caplog: LogCaptureFixture,
    mocker: MockerFixture,
    runner: CliRunner,
    exception: Exception | None,
    logs: list[str] | None,
    output: list[str] | None,
    pwd: Path,
) -> Result | None:
    """Execute a cellophane wrapper from a directory structure."""
    # Extract --flag value pairs from args. If a value is None, the flag is
    # considered to be a flag without a value.
    _args = [p for f in (args or {}).items() for p in f if p is not None]
    _handlers = logging.getLogger().handlers.copy()
    logging.getLogger().handlers = [
        h for h in _handlers if h.__class__ != logging.StreamHandler
    ]
    try:
        mocker.patch("cellophane.cellophane.setup_console_handler")
        mocker.patch("cellophane.cellophane.setup_file_handler")
        _main = cellophane.cellophane("DUMMY", root=root)
        for target, mock in (mocks or {}).items():
            mocker.patch(target=target, **(mock or {}))
        _result = runner.invoke(_main, _args)
        _exception = _result.exception
    except (SystemExit, Exception) as exc:  # pylint: disable=broad-except
        _exception = exc
        _result = None

    cov = Coverage(data_file=pwd / f".coverage.{uuid4()}")
    cov.combine(data_paths=[str(d) for d in root.glob(".coverage.*")])

    if repr(_exception) != (exception or repr(None)):
        fail_from_click_result(
            result=_result,
            reason=(
                "Unexpected exception\n"
                f"Expected: {exception}\n"
                f"Received: {repr(_exception)}\n"
                f"Traceback: {''.join(traceback.format_exception(_exception))}"
            ),
        )

    for log_line in logs or []:
        if log_line not in "\n".join(caplog.messages):
            fail_from_click_result(
                result=_result,
                reason=("Log message not found\n" f"Missing line:\n{log_line}"),
            )

    for output_line in output or []:
        if _result and output_line not in _result.output:
            fail_from_click_result(
                result=_result,
                reason=("Command output not found\n" f"Missing output:\n{output_line}"),
            )

    return _result


def parametrize_from_yaml(paths: list[Path]) -> Callable:
    """Parametrize a test from a YAML file."""

    def wrapper(func: Callable) -> Callable:
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
