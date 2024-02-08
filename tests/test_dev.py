"""Tests for the cellophane.__main__ module."""

# pylint: disable=protected-access,redefined-outer-name

import logging
from os import chdir
from pathlib import Path
from shutil import rmtree
from typing import Any, Iterator
from unittest.mock import MagicMock

from click.testing import CliRunner
from pytest import LogCaptureFixture, TempPathFactory, fixture, mark, param, raises
from pytest_mock import MockerFixture

from cellophane import __main__ as dev

# FIXME: Create a dummy repo to test against
MODULES_REPO_URL = "https://github.com/ClinicalGenomicsGBG/cellophane_modules"


def _mock_recursive(endpoints: list[str], **kwargs: Any) -> MagicMock:
    return MagicMock(
        **{
            (k := e.split(".", 1))[0]: (
                _mock_recursive([k[1]], **kwargs) if len(k) > 1 else MagicMock(**kwargs)
            )
            for e in endpoints
        }
    )


@fixture(scope="function")
def modules_repo() -> dev.ModulesRepo:
    """Create a dummy modules repository."""
    return dev.ModulesRepo.from_url(MODULES_REPO_URL, branch="main")


@fixture(scope="class")
def cellophane_repo(
    tmp_path_factory: TempPathFactory,
) -> Iterator[tuple[dev.CellophaneRepo, Path]]:
    """Create a dummy cellophane repository."""
    _path = tmp_path_factory.mktemp("repo")
    _repo = dev.CellophaneRepo.initialize("DUMMY", _path, MODULES_REPO_URL, "main")
    yield _repo, _path
    rmtree(_path)


class Test_CellophaneRepo:
    """Test cellophane repository."""

    @staticmethod
    def test_initialize(cellophane_repo: tuple[dev.CellophaneRepo, Path]) -> None:
        """Test cellophane repository initialization."""
        _repo, _path = cellophane_repo
        assert _path.exists()
        assert (_path / "modules").exists()
        assert (_path / "schema.yaml").exists()
        assert (_path / "config.example.yaml").exists()
        assert (_path / "DUMMY.py").exists()
        assert (_path / "__main__.py").exists()
        assert {*_repo.absent_modules} == {*_repo.external.modules}
        assert _repo.present_modules == []

    @staticmethod
    def test_initialize_exception_file_exists(
        cellophane_repo: tuple[dev.CellophaneRepo, Path]
    ) -> None:
        """Test cellophane repository initialization with existing file."""
        _, _path = cellophane_repo
        with raises(FileExistsError):
            dev.CellophaneRepo.initialize("DUMMY", _path, MODULES_REPO_URL, "main")

    @staticmethod
    def test_invalid_repository(tmp_path: Path) -> None:
        """Test invalid cellophane repository."""
        with raises(dev.InvalidCellophaneRepoError):
            dev.CellophaneRepo(
                tmp_path,
                modules_repo_url="__INVALID__",
                modules_repo_branch="main",
            )


class Test_ModulesRepo:
    """Test modules repository."""

    @staticmethod
    def test_from_url(modules_repo: dev.ModulesRepo) -> None:
        """Test modules repository initialization from URL."""
        assert modules_repo

    @staticmethod
    def test_invalid_remote_url() -> None:
        """Test invalid remote URL."""
        with raises(dev.InvalidModulesRepoError):
            dev.ModulesRepo.from_url("__INVALID__", branch="main")

    @staticmethod
    def test_branches(modules_repo: dev.ModulesRepo) -> None:
        """Test branches."""
        assert modules_repo._branches

    @staticmethod
    def test_modules(modules_repo: dev.ModulesRepo) -> None:
        """Test modules."""
        assert all(
            m in [b.split("_")[0] for b in modules_repo._branches]
            for m in modules_repo.modules
        )

    @staticmethod
    def test_tags(modules_repo: dev.ModulesRepo) -> None:
        """Test tags."""
        assert modules_repo.tags

    @staticmethod
    def test_module_branches(modules_repo: dev.ModulesRepo) -> None:
        """Test module branches."""
        assert modules_repo.module_branches(modules_repo.modules[0])
        assert not modules_repo.module_branches("__DOES_NOT_EXIST__")

    @staticmethod
    def test_latest_module_tag(modules_repo: dev.ModulesRepo) -> None:
        """Test latest module tag."""
        assert modules_repo.latest_module_tag(modules_repo.modules[0])
        with raises(AttributeError):
            modules_repo.latest_module_tag("__DOES_NOT_EXIST__")

    @staticmethod
    def test_url(modules_repo: dev.ModulesRepo) -> None:
        """Test URL."""
        assert modules_repo.url == MODULES_REPO_URL


class Test__update_example_config:
    """Test updating example config."""

    def test__update_example_config(self, tmp_path: Path) -> None:
        """Test updating example config."""
        # FIXME: Should the contents be verified? It is tested in test_cfg
        chdir(tmp_path)
        (tmp_path / "modules").mkdir()
        (tmp_path / "schema.yaml").touch()

        dev._update_example_config(tmp_path)

        assert (tmp_path / "config.example.yaml").exists()


class Test__ask_modules_branch:
    """Test asking for modules and branches."""

    @mark.parametrize(
        "valid_modules,exception",
        [
            param(["DUMMY_a", "DUMMY_b"], None, id="valid"),
            param([], dev.NoModulesError, id="_ask_modules_invalid"),
        ],
    )
    def test__ask_modules(
        self,
        mocker: MockerFixture,
        valid_modules: list[str],
        exception: type[Exception],
    ) -> None:
        """Test asking for modules."""
        _checkbox_mock = MagicMock()
        mocker.patch("cellophane.__main__.checkbox", return_value=_checkbox_mock)
        assert (
            raises(exception, dev._ask_modules, valid_modules)
            if exception
            else dev._ask_modules(valid_modules) and _checkbox_mock.ask.call_count == 1
        )

    def test__ask_branch(
        self,
        mocker: MockerFixture,
        modules_repo: dev.ModulesRepo,
    ) -> None:
        """Test asking for branch."""
        _select_mock = MagicMock(ask=MagicMock(return_value="latest"))
        mocker.patch("cellophane.__main__.select", return_value=_select_mock)
        assert dev._ask_branch(modules_repo.modules[0], modules_repo)
        assert _select_mock.ask.call_count == 1


# class Test__validate_modules:
#     @mark.parametrize(
#         "modules,exception",
#         [
#             param([("rsync", "dev")], None, id="valid"),
#             param([("rsync", "latest")], None, id="latest"),
#             param([("rsync", None)], None, id="no_branch"),
#             param(
#                 [("rsync", "__INVALID__")],
#                 dev.InvalidBranchError,
#                 id="invalid_branch",
#             ),
#             param(
#                 [("__INVALID__", "dev")],
#                 dev.InvalidModuleError,
#                 id="invalid_module",
#             ),
#         ],
#     )
#     def test__validate_modules(
#         self,
#         mocker,
#         modules_repo,
#         cellophane_repo,
#         modules,
#         exception,
#     ):
#         _project_repo, _ = cellophane_repo
#         _ask_branch_mock = MagicMock(return_value="dev")
#         mocker.patch("cellophane.__main__._ask_branch", _ask_branch_mock)
#         assert (
#             raises(
#                 exception,
#                 dev._validate_modules,
#                 modules,
#                 _project_repo,
#                 modules_repo.modules,
#                 False,
#             )
#             if exception
#             else dev._validate_modules(modules, _project_repo, modules_repo.modules)
#             # and _ask_branch_mock.call_count == sum(m[1] is None for m in modules)
#         )


class Test_module_cli:
    """Test module CLI."""

    runner = CliRunner()

    @mark.parametrize(
        "command,mocks,exit_code,logs",
        [
            param(
                "add rsync@dev",
                {"add": {"side_effect": Exception("DUMMY")}},
                1,
                ["Unhandled Exception: Exception('DUMMY')"],
                id="module_unhandled_exception",
            ),
            param(
                "add rsync@dev",
                {"_update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to add 'rsync@dev': Exception('DUMMY')"],
                id="add_unhandled_exception",
            ),
            param(
                "add rsync@INVALID",
                {},
                1,
                ["Branch 'INVALID' is invalid for 'rsync'"],
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
                "add rsync@latest",
                {},
                0,
                ["Added 'rsync@"],
                id="add",
            ),
            param(
                "update rsync@dev",
                {"_update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to update 'rsync->dev': Exception('DUMMY')"],
                id="update_unhandled_exception",
            ),
            param(
                "update rsync@dev",
                {},
                0,
                ["Updated 'rsync->dev'"],
                id="update",
            ),
            param(
                "rm rsync",
                {"_update_example_config": {"side_effect": Exception("DUMMY")}},
                0,
                ["Unable to remove 'rsync': Exception('DUMMY')"],
                id="rm_unhandled_exception",
            ),
            param(
                "rm rsync",
                {},
                0,
                ["Removed 'rsync'"],
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
        cellophane_repo: tuple[dev.CellophaneRepo, Path],
        command: str,
        mocks: dict[str, dict[str, Any]],
        exit_code: int,
        logs: list[str],
        caplog: LogCaptureFixture,
        mocker: MockerFixture,
    ) -> None:
        """Test module CLI."""
        repo, path = cellophane_repo
        mocker.patch("cellophane.logs.setup_logging")
        for target, kwargs in mocks.items():
            mocker.patch(f"cellophane.__main__.{target}", **kwargs)
        chdir(path)
        with caplog.at_level(logging.DEBUG):
            result = self.runner.invoke(dev.main, f"module {command}")
        assert result.exit_code == exit_code, result
        for log_line in logs:
            assert log_line in "\n".join(caplog.messages)
        assert not repo.is_dirty(), repo.git.status()

    def test_module_cli_invalid_repo(
        self,
        tmp_path: Path,
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test module CLI with invalid cellophane repository."""
        mocker.patch("cellophane.logs.setup_logging")
        chdir(tmp_path)
        with caplog.at_level(logging.DEBUG):
            result = self.runner.invoke(dev.main, "module add")
        assert "Invalid cellophane repository" in "\n".join(caplog.messages)
        assert result.exit_code == 1

    def test_module_cli_dirty_repo(
        self,
        cellophane_repo: tuple[dev.CellophaneRepo, Path],
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test module CLI with dirty cellophane repository."""
        repo, path = cellophane_repo
        mocker.patch("cellophane.logs.setup_logging")
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
            "cellophane.__main__.CellophaneRepo.initialize",
            side_effect=Exception("DUMMY"),
        )
        chdir(tmp_path)
        result = self.runner.invoke(dev.main, "init DUMMY")
        assert result.exit_code == 1
