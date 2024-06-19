"""Tests for the cellophane.__main__ module."""

# pylint: disable=protected-access,redefined-outer-name

import logging
from os import chdir
from pathlib import Path
from shutil import copytree, rmtree
from typing import Any, Iterator
from unittest.mock import MagicMock

from click.testing import CliRunner
from git import Repo
from pytest import (
    LogCaptureFixture,
    MonkeyPatch,
    TempPathFactory,
    fixture,
    mark,
    param,
    raises,
)
from pytest_mock import MockerFixture

from cellophane.src import dev

LIB = Path(__file__).parent / "lib"


@fixture(scope="class")
def classy_monkey() -> Iterator[MonkeyPatch]:
    """Class scoped monkeypatch."""
    with MonkeyPatch.context() as mp:
        yield mp


def _mock_recursive(endpoints: list[str], **kwargs: Any) -> MagicMock:
    return MagicMock(
        **{
            (k := e.split(".", 1))[0]: (
                _mock_recursive([k[1]], **kwargs) if len(k) > 1 else MagicMock(**kwargs)
            )
            for e in endpoints
        }
    )


@fixture(scope="module")
def modules_repo(
    tmp_path_factory: TempPathFactory,
) -> Iterator[tuple[dev.ModulesRepo, Path]]:
    """Create a dummy modules repository."""
    path = tmp_path_factory.mktemp("modules_repo")
    Repo.init(path)
    repo = dev.ModulesRepo(path)
    repo.create_remote("origin", url=str(path))
    copytree(LIB / "repo", path, dirs_exist_ok=True)
    repo.index.add("**")
    repo.index.commit("Initial commit")
    repo.create_tag("a/1.0.0")
    repo.create_tag("b/1.0.0")
    (path / "modules" / "a" / "A").write_text("2.0.0")
    repo.index.add("**")
    repo.index.commit("Dummy commit")
    repo.create_tag("a/2.0.0")
    repo.create_head("dev")

    repo.remote("origin").push("master")
    repo.remote("origin").push("dev")

    yield repo, path
    rmtree(path)


@fixture(scope="class")
def cellophane_repo(
    tmp_path_factory: TempPathFactory,
    modules_repo: tuple[dev.ModulesRepo, Path],
    classy_monkey: MonkeyPatch,
) -> Iterator[tuple[dev.ProjectRepo, Path]]:
    """Create a dummy cellophane repository."""
    m_repo, m_path = modules_repo

    def _modules_repo(*args: Any, **kwargs: Any) -> dev.ModulesRepo:
        del args, kwargs  # Unused
        return m_repo

    classy_monkey.setattr(dev.ModulesRepo, "from_url", _modules_repo)
    path = tmp_path_factory.mktemp("repo")
    repo = dev.initialize_project("DUMMY", path, str(m_path), "main")
    yield repo, path
    rmtree(path)


class Test_ProjectRepo:
    """Test cellophane repository."""

    @staticmethod
    def test_initialize(cellophane_repo: tuple[dev.ProjectRepo, Path]) -> None:
        """Test cellophane repository initialization."""
        _repo, _path = cellophane_repo
        assert _path.exists()
        assert (_path / "modules").exists()
        assert (_path / "schema.yaml").exists()
        assert (_path / "config.example.yaml").exists()
        assert (_path / "DUMMY.py").exists()
        assert (_path / "__main__.py").exists()
        assert {*_repo.absent_modules} == {*_repo.external.modules}
        assert _repo.modules == set()

    @staticmethod
    def test_initialize_exception_file_exists(
        cellophane_repo: tuple[dev.ProjectRepo, Path],
    ) -> None:
        """Test cellophane repository initialization with existing file."""
        _, _path = cellophane_repo
        with raises(FileExistsError):
            dev.initialize_project("DUMMY", _path, "DUMMY", "main")

    @staticmethod
    def test_invalid_repository(tmp_path: Path) -> None:
        """Test invalid cellophane repository."""
        with raises(dev.InvalidProjectRepoError):
            dev.ProjectRepo(
                tmp_path,
                modules_repo_url="__INVALID__",
                modules_repo_branch="main",
            )


class Test_ModulesRepo:
    """Test modules repository."""

    @staticmethod
    def test_from_url(modules_repo: tuple[dev.ModulesRepo, Path]) -> None:
        """Test modules repository initialization from URL."""
        repo, _ = modules_repo
        assert repo

    @staticmethod
    def test_invalid_remote_url() -> None:
        """Test invalid remote URL."""
        with raises(dev.InvalidModulesRepoError):
            dev.ModulesRepo.from_url("__INVALID__", branch="main")

    @staticmethod
    def test_tags(modules_repo: tuple[dev.ModulesRepo, Path]) -> None:
        """Test tags."""
        repo, _ = modules_repo
        assert repo.tags

    @staticmethod
    def test_url(modules_repo: tuple[dev.ModulesRepo, Path]) -> None:
        """Test URL."""
        repo, path = modules_repo
        assert repo.url == str(path)


class Test_update_example_config:
    """Test updating example config."""

    def test_update_example_config(self, tmp_path: Path) -> None:
        """Test updating example config."""
        chdir(tmp_path)
        (tmp_path / "modules").mkdir()
        (tmp_path / "schema.yaml").touch()

        dev.update_example_config(tmp_path)

        assert (tmp_path / "config.example.yaml").exists()


class Test_ask_modules_branch:
    """Test asking for modules and branches."""

    @mark.parametrize(
        "valid_modules,exception",
        [
            param(["DUMMY_a", "DUMMY_b"], None, id="valid"),
            param([], dev.NoModulesError, id="invalid"),
        ],
    )
    def test_ask_modules(
        self,
        mocker: MockerFixture,
        valid_modules: list[str],
        exception: type[Exception],
    ) -> None:
        """Test asking for modules."""
        _checkbox_mock = MagicMock()
        mocker.patch("cellophane.src.dev.util.checkbox", return_value=_checkbox_mock)
        assert (
            raises(exception, dev.ask_modules, valid_modules)
            if exception
            else dev.ask_modules(valid_modules) and _checkbox_mock.ask.call_count == 1
        )

    def test_ask_version(
        self,
        mocker: MockerFixture,
        modules_repo: tuple[dev.ModulesRepo, Path],
    ) -> None:
        """Test asking for branch."""
        repo, _ = modules_repo
        _select_mock = MagicMock(ask=MagicMock(return_value="latest"))
        mocker.patch("cellophane.src.dev.util.select", return_value=_select_mock)
        assert dev.ask_version(
            [*repo.modules.keys()][0], valid=[("foo/1.33.7", "1.33.7")]
        )
        assert _select_mock.ask.call_count == 1


class Test_module_cli:
    """Test module CLI."""

    runner = CliRunner()

    @mark.parametrize(
        "command,mocks,exit_code,logs",
        [
            param(
                "add a@1.0.0",
                {"add": {"side_effect": Exception("DUMMY")}},
                1,
                ["Unhandled Exception: Exception('DUMMY')"],
                id="module_unhandled_exception",
            ),
            param(
                "add a@1.0.0",
                {"update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to add 'a@1.0.0': Exception('DUMMY')"],
                id="add_unhandled_exception",
            ),
            param(
                "add a@INVALID",
                {},
                1,
                ["Version 'INVALID' is invalid for 'a'"],
                id="invalid_branch",
            ),
            param(
                "add INVALID@latest",
                {},
                1,
                ["Module 'INVALID' is not valid"],
                id="invalid_module",
            ),
            param(
                "add a@1.0.0",
                {},
                0,
                ["Added 'a@1.0.0"],
                id="add_a",
            ),
            param(
                "add a@1.0.0",
                {},
                1,
                ["Module 'a' is not valid"],
                id="add_a_exists",
            ),
            param(
                "add b@1.0.0",
                {},
                0,
                ["Added 'b@1.0.0"],
                id="add_b",
            ),
            param(
                "update a@dev",
                {"update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to update 'a->dev': Exception('DUMMY')"],
                id="update_unhandled_exception",
            ),
            param(
                "update a@dev",
                {},
                0,
                ["Updated 'a->dev'"],
                id="update",
            ),
            param(
                "update a@latest",
                {},
                0,
                ["Updated 'a->2.0.0'"],
                id="update_latest",
            ),
            param(
                "rm a",
                {"update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to remove 'a': Exception('DUMMY')"],
                id="rm_unhandled_exception",
            ),
            param(
                "rm a b",
                {},
                0,
                ["Removed 'a'", "Removed 'b'"],
                id="rm",
            ),
            param(
                "rm",
                {},
                1,
                ["No modules to select from"],
                id="rm_no_module_present",
            ),
        ],
    )
    def test_module_cli(
        self,
        cellophane_repo: tuple[dev.ProjectRepo, Path],
        command: str,
        mocks: dict[str, dict[str, Any]],
        exit_code: int,
        logs: list[str],
        caplog: LogCaptureFixture,
        mocker: MockerFixture,
    ) -> None:
        """Test module CLI."""
        repo, path = cellophane_repo
        mocker.patch("cellophane.logs.setup_console_handler")
        for target, kwargs in mocks.items():
            mocker.patch(f"cellophane.src.dev.cli.{target}", **kwargs)
        chdir(path)
        with caplog.at_level(logging.DEBUG):
            result = self.runner.invoke(dev.main, f"module {command}")
        for log_line in logs:
            assert log_line in "\n".join(caplog.messages)
        assert not repo.is_dirty(), repo.git.status()
        assert result.exit_code == exit_code, result

    def test_module_cli_invalid_repo(
        self,
        tmp_path: Path,
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test module CLI with invalid cellophane repository."""
        mocker.patch("cellophane.logs.setup_console_handler")
        chdir(tmp_path)
        with caplog.at_level(logging.DEBUG):
            result = self.runner.invoke(dev.main, "module add")
        assert "Invalid cellophane repository" in "\n".join(caplog.messages)
        assert result.exit_code == 1

    def test_module_cli_dirty_repo(
        self,
        cellophane_repo: tuple[dev.ProjectRepo, Path],
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test module CLI with dirty cellophane repository."""
        repo, path = cellophane_repo
        mocker.patch("cellophane.logs.setup_console_handler")
        chdir(path)
        (path / "DIRTY").touch()
        repo.index.add("DIRTY")
        with caplog.at_level(logging.DEBUG):
            result = self.runner.invoke(dev.main, "module add")
            repo.index.remove("DIRTY")
        assert "Repository has uncommited changes" in "\n".join(caplog.messages)
        assert result.exit_code == 1


class Test_cli_init:
    """Test cellophane CLI for initializing a new project."""

    runner = CliRunner()

    @fixture(scope="class")
    def project_path(self, tmp_path_factory: TempPathFactory) -> Path:
        """Create a temporary project path."""
        return tmp_path_factory.mktemp("DUMMY")

    @mark.parametrize(
        "command,exit_code",
        [
            param("init DUMMY", 0, id="init"),
            param("init DUMMY", 1, id="init_exists"),
            param("init DUMMY --force", 0, id="init_force"),
        ],
    )
    def test_init_cli(
        self,
        project_path: Path,
        command: str,
        exit_code: int,
    ) -> None:
        """Test cellophane CLI for initializing a new project."""
        chdir(project_path)
        result = self.runner.invoke(dev.main, command)
        assert result.exit_code == exit_code

    def test_init_cli_unhandled_exception(
        self,
        tmp_path: Path,
        mocker: MockerFixture,
    ) -> None:
        """Test exception handling in cellophane CLI for initializing a new project."""
        mocker.patch(
            "cellophane.src.dev.cli.initialize_project",
            side_effect=Exception("DUMMY"),
        )
        chdir(tmp_path)
        result = self.runner.invoke(dev.main, "init DUMMY")
        assert result.exit_code == 1
