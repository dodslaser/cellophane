from cellophane import __main__ as dev
from pytest import raises, mark, param, fixture
from unittest.mock import MagicMock
from click.testing import CliRunner
from os import chdir
from shutil import rmtree
import logging


# FIXME: Create a dummy repo to test against
MODULES_REPO_URL = "https://github.com/ClinicalGenomicsGBG/cellophane_modules"


def _mock_recursive(endpoints, **kwargs):
    return MagicMock(
        **{
            (k := e.split(".", 1))[0]: _mock_recursive([k[1]], **kwargs)
            if len(k) > 1
            else MagicMock(**kwargs)
            for e in endpoints
        }
    )


@fixture(scope="module")
def modules_repo():
    return dev.ModulesRepo.from_url(MODULES_REPO_URL)


@fixture(scope="class")
def cellophane_repo(tmp_path_factory):
    _path = tmp_path_factory.mktemp("repo")
    _repo = dev.CellophaneRepo.initialize("DUMMY", _path, MODULES_REPO_URL)
    yield _repo, _path
    rmtree(_path)


class Test_CellophaneRepo:
    @staticmethod
    def test_initialize(cellophane_repo):
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
    def test_initialize_exception_file_exists(cellophane_repo):
        _, _path = cellophane_repo
        with raises(FileExistsError):
            dev.CellophaneRepo.initialize("DUMMY", _path, MODULES_REPO_URL)

    @staticmethod
    def test_invalid_repository(tmp_path):
        with raises(dev.InvalidCellophaneRepoError):
            dev.CellophaneRepo(tmp_path)


class Test_ModulesRepo:
    @staticmethod
    def test_from_url(modules_repo):
        assert modules_repo

    @staticmethod
    def test_invalid_remote_url():
        with raises(dev.InvalidModulesRepoError):
            dev.ModulesRepo.from_url("__INVALID__")

    @staticmethod
    def test_branches(modules_repo):
        assert modules_repo._branches

    @staticmethod
    def test_modules(modules_repo):
        assert all(
            m in [b.split("_")[0] for b in modules_repo._branches]
            for m in modules_repo.modules
        )

    @staticmethod
    def test_tags(modules_repo):
        assert modules_repo.tags

    @staticmethod
    def test_module_branches(modules_repo):
        assert modules_repo.module_branches(modules_repo.modules[0])
        assert not modules_repo.module_branches("__DOES_NOT_EXIST__")

    @staticmethod
    def test_latest_module_tag(modules_repo):
        assert modules_repo.latest_module_tag(modules_repo.modules[0])
        with raises(AttributeError):
            modules_repo.latest_module_tag("__DOES_NOT_EXIST__")

    @staticmethod
    def test_url(modules_repo):
        assert modules_repo.url == MODULES_REPO_URL


class Test__update_example_config:
    def test__update_example_config(self, tmp_path):
        # FIXME: Should the contents be veriied? It is tested in test_cfg
        chdir(tmp_path)
        (tmp_path / "modules").mkdir()
        (tmp_path / "schema.yaml").touch()

        dev._update_example_config(tmp_path)

        assert (tmp_path / "config.example.yaml").exists()


class Test__ask_modules_branch:
    @mark.parametrize(
        "valid_modules,exception",
        [
            param(["DUMMY_a", "DUMMY_b"], None, id="valid"),
            param([], dev.NoModulesError, id="_ask_modules_invalid"),
        ],
    )
    def test__ask_modules(self, mocker, valid_modules, exception):
        _checkbox_mock = MagicMock()
        mocker.patch("cellophane.__main__.checkbox", return_value=_checkbox_mock)
        assert (
            raises(exception, dev._ask_modules, valid_modules)
            if exception
            else dev._ask_modules(valid_modules) and _checkbox_mock.ask.call_count == 1
        )

    def test__ask_branch(self, mocker, modules_repo):
        _select_mock = MagicMock(ask=MagicMock(return_value="latest"))
        mocker.patch("cellophane.__main__.select", return_value=_select_mock)
        assert dev._ask_branch(modules_repo.modules[0], modules_repo)
        assert _select_mock.ask.call_count == 1


class Test__validate_modules:
    @mark.parametrize(
        "modules,exception",
        [
            param([("rsync", "dev")], None, id="valid"),
            param([("rsync", "latest")], None, id="latest"),
            param([("rsync", None)], None, id="no_branch"),
            param(
                [("rsync", "__INVALID__")],
                dev.InvalidBranchError,
                id="invalid_branch",
            ),
            param(
                [("__INVALID__", "dev")],
                dev.InvalidModuleError,
                id="invalid_module",
            ),
        ],
    )
    def test__validate_modules(
        self,
        mocker,
        modules_repo,
        cellophane_repo,
        modules,
        exception,
    ):
        _project_repo, _ = cellophane_repo
        _ask_branch_mock = MagicMock(return_value="dev")
        mocker.patch("cellophane.__main__._ask_branch", _ask_branch_mock)
        assert (
            raises(
                exception,
                dev._validate_modules,
                modules,
                _project_repo,
                modules_repo.modules,
                False,
            )
            if exception
            else dev._validate_modules(modules, _project_repo, modules_repo.modules)
            # and _ask_branch_mock.call_count == sum(m[1] is None for m in modules)
        )


class Test_cli_module:
    runner = CliRunner()

    @fixture(scope="class")
    def project_path(self, tmp_path_factory):
        return tmp_path_factory.mktemp("project")

    @mark.parametrize(
        "command,exit_code",
        [
            param("add rsync@__INVALID__", 1, id="add_invalid_branch"),
            param("add __INVALID__@latest", 1, id="add_invalid_module"),
            param("add rsync@latest", 0, id="add"),
            param("update rsync@dev", 0, id="update"),
            param("rm rsync", 0, id="rm"),
            param("rm", 1, id="rm_no_module_present"),
        ],
    )
    def test_module_cli(self, cellophane_repo, command, exit_code):
        _, _path = cellophane_repo
        chdir(_path)
        result = self.runner.invoke(dev.main, f"module {command}")
        assert result.exit_code == exit_code

    def test_module_cli_unhandled_exception(self, cellophane_repo, mocker):
        mocker.patch(
            "cellophane.__main__._validate_modules", side_effect=Exception("DUMMY")
        )
        result = self.runner.invoke(dev.main, "module add")
        assert result.exit_code == 1

    def test_module_cli_invalid_repo(self, tmp_path):
        chdir(tmp_path)
        result = self.runner.invoke(dev.main, "module add")
        assert result.exit_code == 1

    def test_module_cli_dirty_repo(self, cellophane_repo):
        _repo, _path = cellophane_repo
        chdir(_path)
        (_path / "DIRTY").touch()
        _repo.index.add("DIRTY")
        result = self.runner.invoke(dev.main, "module add")
        assert result.exit_code == 1
        _repo.index.remove("DIRTY")

    @mark.parametrize(
        "functions,exception",
        [
            param(["create_submodule", "submodule"], None, id="git_submodule"),
            param(["index.commit"], SystemExit, id="git_add"),
        ],
    )
    @mark.parametrize("command", [dev.add, dev.update, dev.rm])
    def test_module_cli_unhandeled_exceptions(
        self, caplog, tmp_path, mocker, command, functions, exception
    ):
        mocker.patch("cellophane.__main__._update_example_config")

        _repo_mock = _mock_recursive(functions, side_effect=Exception("DUMMY"))

        try:
            command(
                path=tmp_path,
                repo=_repo_mock,
                logger=logging.getLogger("DUMMY"),
                log_level="DEBUG",
                modules=[("DUMMY", "DUMMY")],
            )
        except (exception, Exception) as e:
            assert exception and isinstance(e, exception)
        else:
            assert exception is None
        finally:
            assert "DUMMY" in caplog.text


class Test_cli_init:
    runner = CliRunner()

    @fixture(scope="class")
    def project_path(self, tmp_path_factory):
        return tmp_path_factory.mktemp("DUMMY")

    @mark.parametrize(
        "command,exit_code",
        [
            param("init DUMMY", 0, id="init"),
            param("init DUMMY", 1, id="init_exists"),
            param("init DUMMY --force", 0, id="init_force"),
        ],
    )
    def test_init_cli(self, project_path, command, exit_code):
        chdir(project_path)
        result = self.runner.invoke(dev.main, command)
        assert result.exit_code == exit_code

    def test_init_cli_unhandled_exception(self, tmp_path, mocker):
        mocker.patch(
            "cellophane.__main__.CellophaneRepo.initialize",
            side_effect=Exception("DUMMY"),
        )
        chdir(tmp_path)
        result = self.runner.invoke(dev.main, "init DUMMY")
        assert result.exit_code == 1
