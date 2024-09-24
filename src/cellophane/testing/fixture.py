import logging
from pathlib import Path
from typing import Callable, Iterator

from click.testing import CliRunner
from pytest import FixtureRequest, LogCaptureFixture, fixture
from pytest_mock import MockerFixture

from .util import create_structure, execute_from_structure


@fixture()
def run_definition(
    tmp_path: Path,
    caplog: LogCaptureFixture,
    request: FixtureRequest,
    mocker: MockerFixture,
) -> Iterator[Callable]:
    """Run a cellophane wrapper from a definition YAML file."""
    _runner = CliRunner()
    _handlers = logging.getLogger().handlers.copy()
    _extenal_root = Path(request.fspath).parent  # type: ignore[attr-defined]
    _pytest_pwd = Path.cwd()

    def inner(definition: dict) -> None:
        with (
            _runner.isolated_filesystem(tmp_path) as td,
            caplog.at_level(logging.DEBUG),
        ):
            create_structure(
                root=Path(td),
                structure=definition.get("structure", {}),
                external_root=_extenal_root,
                external=definition.get("external"),
            )
            execute_from_structure(
                root=Path(td),
                mocks=definition.get("mocks", {}),
                args=definition.get("args"),
                caplog=caplog,
                mocker=mocker,
                runner=_runner,
                exception=definition.get("exception"),
                logs=definition.get("logs"),
                output=definition.get("output"),
                pwd=_pytest_pwd,
            )

    yield inner
    logging.getLogger().handlers = _handlers
