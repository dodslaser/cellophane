# pragma: no cover

"""Testing utilities for Cellophane."""

import traceback
from logging import NullHandler, getLogger
from pathlib import Path
from typing import Any, Callable

from click.testing import CliRunner, Result
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


def fail_from_click_result(result: Result | None, msg: str) -> None:
    """Fail a test with a message and a click result."""
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
) -> Result | None:
    """Execute a cellophane wrapper from a directory structure."""
    # Extract --flag value pairs from args. If a value is None, the flag is
    # considered to be a flag without a value.
    _args = [p for f in (args or {}).items() for p in f if p is not None]

    def _setup_logging(*args: Any, **kwargs: Any) -> NullHandler:
        del args, kwargs  # unused
        getLogger().handlers = getLogger().handlers[1:]
        return NullHandler()

    try:
        mocker.patch(
            "cellophane.cellophane.setup_logging",
            new=_setup_logging,
        )
        mocker.patch("cellophane.cellophane.add_file_handler")
        _main = cellophane.cellophane("DUMMY", root=root)
        for target, mock in (mocks or {}).items():
            _side_effect = (
                exc()
                if isinstance(exc := (mock or {}).get("exception"), type)
                and issubclass(exc, BaseException)
                else Exception(exc) if exc else None
            )
            mocker.patch(
                target=target,
                side_effect=_side_effect,
                **(mock or {}).get("kwargs", {}),
            )
        _result = runner.invoke(_main, _args)
        _exception = _result.exception
    except (SystemExit, Exception) as e:  # pylint: disable=broad-except
        _exception = e
        _result = None

    if repr(_exception) != (exception or repr(None)):
        fail_from_click_result(
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
            fail_from_click_result(
                result=_result,
                msg=("Log message not found\n" f"Missing line:\n{log_line}"),
            )

    for output_line in output or []:
        if _result and output_line not in _result.output:
            fail_from_click_result(
                result=_result,
                msg=("Command output not found\n" f"Missing output:\n{output_line}"),
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
