"""CLI for managing cellophane projects"""

import logging
import re
from functools import cached_property, lru_cache, wraps
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Callable, Literal, Sequence

import rich_click as click
from git.exc import InvalidGitRepositoryError
from git.repo import Repo
from questionary import Choice, checkbox, select

from cellophane import CELLOPHANE_ROOT, cfg, logs


class InvalidModuleError(Exception):
    """
    Exception raised when a module is not valid.

    Args:
        _module (str): The name of the module.
        msg (str | None): The error message (default: None).
    """

    def __init__(self, _module: str, msg: str | None = None):
        self.module = _module
        super().__init__(msg or f"Module '{_module}' is not valid")


class InvalidBranchError(Exception):
    """
    Exception raised when a module is not valid.

    Args:
        _module (str): The name of the module.
        branch (str): The name of the branch.
        msg (str | None): The error message (default: None).
    """

    def __init__(
        self, _module: str, branch: str | None, msg: str | None = None
    ) -> None:
        self.module = _module
        self.branch = branch
        super().__init__(msg or f"Branch '{branch}' is invalid for '{_module}'")


class NoModulesError(Exception):
    """
    Exception raised when there are no modules to select from.
    """

    def __init__(self, msg: str | None = None) -> None:
        super().__init__(msg)


class InvalidModulesRepoError(InvalidGitRepositoryError):
    """
    Exception raised when the modules repository is invalid.

    Args:
        url (str): The URL of the invalid modules repository.
        *args: Additional positional arguments passed to InvalidGitRepositoryError.
        msg (str | None): The error message (default: None).
        **kwargs: Additional keyword arguments passed to InvalidGitRepositoryError.
    """

    def __init__(
        self,
        url: str,
        *args: Any,
        msg: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            msg or f"Invalid modules repository ({url})",
            *args,
            **kwargs,
        )


class InvalidCellophaneRepoError(InvalidGitRepositoryError):
    """
    Exception raised when the project repository is invalid.

    Args:
        path (Path | str): The project root path.
        *args: Additional positional arguments passed to InvalidGitRepositoryError.
        msg (str | None): The error message (default: None).
        **kwargs: Additional keyword arguments passed to InvalidGitRepositoryError.
    """

    def __init__(
        self,
        path: Path | str,
        *args: Any,
        msg: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            msg or f"Invalid cellophane repository ({path})",
            *args,
            **kwargs,
        )


class ModulesRepo(Repo):
    """
    Represents a modules repository.

    This class extends the `Repo` class and provides additional functionality
    specific to modules repositories.

    Methods:
        from_url(cls, url: str) -> ModulesRepo:
            Creates a `ModulesRepo` instance by cloning the repository from the
            specified URL.

        _branches(self) -> List[str]:
            Retrieves the list of branches in the repository.

        modules(self) -> List[str]:
            Retrieves the list of modules in the repository.

        module_branches(self, _module: str) -> List[str]:
            Retrieves the branches associated with the specified module.

        latest_module_tag(self, _module: str) -> str:
            Retrieves the latest tag for the specified module.

    Attributes:
        url: The URL of the repository.

        Example:
            ```python
            url = "https://github.com/ClinicalGenomicsGBG/cellophane_modules"
            repo = ModulesRepo.from_url(url)
            branches = repo.module_branches("my_module")
            latest_tag = repo.latest_module_tag("my_module")
            ```
    """

    @classmethod
    def from_url(cls, url: str) -> "ModulesRepo":
        """
        Creates a `ModulesRepo` instance by cloning the repository from the specified
        URL.

        Args:
            cls: The class itself.
            url (str): The URL of the repository.

        Returns:
            ModulesRepo: An instance of the `ModulesRepo` class.

        Raises:
            InvalidModulesRepoError: Raised when the repository cloning fails.

        Example:
            ```python
            url = "https://github.com/ClinicalGenomicsGBG/cellophane_modules"
            repo = ModulesRepo.from_url(url)
            ```
        """
        _path = mkdtemp(prefix="cellophane_modules_")
        try:
            return cls.clone_from(
                url=url,
                to_path=_path,
                checkout=False,
            )  # type: ignore[return-value]
        except Exception as e:
            raise InvalidModulesRepoError(url) from e

    @cached_property
    def _branches(self) -> list[str]:
        return [
            r.name.split("/")[-1]
            for r in self.remote("origin").refs
            if r.name != "origin/HEAD"
        ]

    @cached_property
    def modules(self) -> list[str]:
        """
        Retrieves the list of modules in the repository.


        Uses `git ls_tree` to retrieve the list of subdirectories in the repository.
        All non-hidden directories at the base level are considered modules.

        Returns:
            List[str]: The list of module names.
        """

        return [
            m
            for m in self.git.ls_tree(
                "HEAD",
                r=True,
                d=True,
                name_only=True,
            ).split("\n")
            if all(
                (
                    not m.startswith("."),
                    "/" not in m,
                )
            )
        ]

    @lru_cache
    def module_branches(self, _module: str) -> list[str]:
        """
        Retrieves the branches associated with the specified module.

        Args:
            _module (str): The name of the module.

        Returns:
            list[str]: The list of branch names associated with the module.

        Example:
            ```python
            repo = ModulesRepo()
            module = "my_module"
            repo.module_branches(module)  # ["latest", "v1.0.0", "v1.0.1"]
            ```
        """

        return [
            b.removeprefix(_module).lstrip("_")
            for b in self._branches
            if all(
                (
                    b.startswith(_module),
                    b != _module,
                )
            )
        ]

    @lru_cache
    def latest_module_tag(self, _module: str) -> str:
        """
        Retrieves the latest tag for the specified module.

        Assumes most recent tag is the latest release.

        Args:
            _module (str): The name of the module.

        Returns:
            str: The name of the latest tag for the module.

        Raises:
            AttributeError: Raised when no releases are found for the module.

        Example:
            ```python
            repo = ModulesRepo()
            module = "my_module"
            repo.latest_module_tag(module) # "v1.0.1"
            ```
        """
        if tags := [t for t in self.tags if t.name in self.module_branches(_module)]:
            # FIXME: This assumes that the most recent release is the latest version
            return sorted(tags, key=lambda t: t.object.committed_date)[-1].name
        else:
            raise AttributeError(f"Could not find any releases for {_module}")

    @property
    def url(self) -> str:
        """Returns the URL of the repository."""
        return self.remote("origin").url


class CellophaneRepo(Repo):
    """
    Represents a Cellophane project repository.

    Extends the `Repo` class and provides additional functionality specific to
    Cellophane project repositories.

    Attributes:
        external (ModulesRepo): The external modules repository.

    Methods:
        initialize(cls, name, path: Path, modules_repo_url: str, force=False):
            Initializes a new Cellophane project repository with the specified name,
            path, and modules repository URL.

    Properties:
        modules (List[str]): The list of modules in the repository.
        absent_modules (List[str]): List modules that are not added to the project.
        present_modules (List[str]): List modules that are present in the project.

    Example:
        ```python
        path = Path("path/to/repo")
        modules_repo_url = "https://github.com/ClinicalGenomicsGBG/cellophane_modules"
        repo = CellophaneRepo(path, modules_repo_url)
        modules = repo.modules
        absent_modules = repo.absent_modules  # ["module_a", "module_b"]
        present_modules = repo.present_modules  # ["module_c", "module_d"]
        ```
    """

    external: ModulesRepo

    def __init__(
        self,
        path: Path,
        modules_repo_url: str,
        **kwargs: Any,
    ) -> None:
        try:
            super().__init__(str(path), **kwargs)
        except InvalidGitRepositoryError as e:
            raise InvalidCellophaneRepoError(path) from e

        self.external = ModulesRepo.from_url(modules_repo_url)

    @classmethod
    def initialize(
        cls,
        name: str,
        path: Path,
        modules_repo_url: str,
        force: bool = False,
    ) -> "CellophaneRepo":
        """
        Initializes a new Cellophane repository with the specified name, path,
        and modules repository URL.

        Creates the necessary directories and files for the repository structure.
        The repository is then initialized,, and an initial commit is made.

        Args:
            name (str): The name of the repository.
            path (Path): The path where the repository will be initialized.
            modules_repo_url (str): The URL of the modules repository.
            force (bool | None): Whether to force initialization even if the path
                is not empty. Defaults to False.

        Returns:
            CellophaneRepo: An instance of the `CellophaneRepo` class representing the
                initialized repository.

        Raises:
            FileExistsError: Raised when the path is not empty and force is False.

        Example:
            ```python
            name = "my_awesome_repo"
            path = Path("/path/to/repo")
            modules_repo_url = "https://example.com/modules"
            repo = CellophaneRepo.initialize(name, path, modules_repo_url)

            # ./my_awesome_wrapper/
            # │
            # │   # Directory containing cellophane modules
            # ├── modules
            # │   ├── __init__.py
            # │   │
            # │   │   # Requirements file for the modules
            # │   └── requirements.txt
            # │
            # │   # Directory containing scripts to be submitted by Popen, SGE, etc.
            # ├── scripts
            # │   └── my_script.sh
            # │
            # │   # Directory containing misc. files used by the wrapper.
            # ├── scripts
            # │   └── some_more_data.txt
            # │
            # │   # Requirements file for the wrapper
            # ├── requirements.txt
            # │
            # │   # JSON Schema defining configuration options
            # ├── schema.yaml
            # │
            # │   # Main entrypoint for the wrapper
            # └── __main__.py
            # │
            # │   # Alternative entrypoint for the wrapper
            # └── my_awesome_wrapper.py
            ```
        """
        _prog_name = re.sub("\\W", "_", name)

        if [*path.glob("*")] and not force:
            raise FileExistsError(path)

        for subdir in (
            path / "modules",
            path / "scripts",
        ):
            subdir.mkdir(parents=True, exist_ok=force)

        for file in (
            path / "modules" / "__init__.py",
            path / "schema.yaml",
        ):
            file.touch(exist_ok=force)

        (path / "__main__.py").write_text(
            (CELLOPHANE_ROOT / "template" / "__main__.py")
            .read_text(encoding="utf-8")
            .format(label=name, prog_name=_prog_name)
        )

        (path / f"{_prog_name}.py").write_text(
            (CELLOPHANE_ROOT / "template" / "entrypoint.py")
            .read_text(encoding="utf-8")
            .format(label=name, prog_name=_prog_name)
        )

        (path / "requirements.txt").write_text(
            (CELLOPHANE_ROOT / "template" / "requirements.txt")
            .read_text(encoding="utf-8")
            .format(label=name, prog_name=_prog_name)
        )
        (path / "modules" / "requirements.txt").write_text(
            (CELLOPHANE_ROOT / "template" / "modules" / "requirements.txt")
            .read_text(encoding="utf-8")
            .format(label=name, prog_name=_prog_name)
        )

        _update_example_config(path)

        repo = Repo.init(str(path))

        repo.index.add(
            [
                path / "modules" / "__init__.py",
                path / "modules" / "requirements.txt",
                path / "requirements.txt",
                path / "schema.yaml",
                path / "config.example.yaml",
                path / "__main__.py",
                path / f"{_prog_name}.py",
            ]
        )
        repo.index.write()
        repo.index.commit("feat(cellophane): Initial commit from cellophane 🎉")

        return cls(path, modules_repo_url)

    @property
    def modules(self) -> list[str]:
        """
        Retrieves the list of modules in the repository.

        Returns:
            List[str]: The list of module names.
        """
        return [sm.name for sm in self.submodules]

    @property
    def absent_modules(self) -> list[str]:
        """
        Retrieves the list of modules not added to the project.

        Returns:
            List[str]: List modules not added to the project.
        """

        return [*{*self.external.modules} - {*self.modules}]

    @property
    def present_modules(self) -> list[str]:
        """
        Retrieves the list of modules currently in the project.

        Returns:
            List[str]: List modules not added to the project.
        """
        return [*{*self.modules} & {*self.external.modules}]


def _add_requirements(path: Path, _module: str) -> None:
    requirements_path = path / "modules" / "requirements.txt"
    module_path = path / "modules" / _module

    if (
        module_path.is_dir()
        and (module_path / "requirements.txt").exists()
        and (spec := f"-r {_module}/requirements.txt\n") not in requirements_path.read_text()
    ):
        with open(requirements_path, "a", encoding="utf-8") as handle:
            handle.write(spec)


def _remove_requirements(path: Path, _module: str) -> None:
    requirements_path = path / "modules" / "requirements.txt"

    if (spec := f"-r {_module}/requirements.txt\n") in (
        requirements := requirements_path.read_text()
    ):
        with open(requirements_path, "w", encoding="utf-8") as handle:
            handle.write(requirements.replace(spec, ""))


def _update_example_config(path: Path) -> None:
    # FIXME: Add support for manually defined examples
    schema = cfg.Schema.from_file(
        path=[
            CELLOPHANE_ROOT / "schema.base.yaml",
            path / "schema.yaml",
            *(path / "modules").glob("**/schema.yaml"),
        ],
    )

    with open(path / "config.example.yaml", "w", encoding="utf-8") as handle:
        handle.write(schema.example_config)


def _ask_modules(valid_modules: Sequence[str]) -> list[str]:
    if not valid_modules:
        raise NoModulesError("No modules to select from")

    if _modules := checkbox(
        "Select module(s)",
        choices=[Choice(title=m, value=(m, None)) for m in valid_modules],
        erase_when_done=True,
        validate=lambda x: len(x) > 0 or "Select at least one module",
    ).ask():
        return _modules
    else:
        raise NoModulesError("No modules selected")


def _ask_branch(_module: str, modules_repo: ModulesRepo) -> str:
    _branch = select(
        f"Select branch for {_module}",
        # FIXME: Should the number of branches be limited?
        choices=["latest", *modules_repo.module_branches(_module)],
        default="latest",
        erase_when_done=True,
    ).ask()
    if _branch == "latest":
        _branch = modules_repo.latest_module_tag(_module)

    return _branch



def _validate_modules(ignore_branch: bool = False) -> Callable:
    
    def wrapper(func: Callable) -> Callable:
        @wraps(func)
        def inner(
            modules: list[tuple[str, str]] | list[tuple[str, None]] | None,
            valid_modules: Sequence[str],
            repo: CellophaneRepo,
            **kwargs: Any,
        ) -> None:
            _modules = modules or _ask_modules(valid_modules)
            for _module, _ in _modules:
                if _module not in valid_modules:
                    raise InvalidModuleError(_module)
            
            if ignore_branch:
                _modules = [(m, None) for m, _ in _modules]
            else:
                _modules = [(m, b or _ask_branch(m, repo.external)) for m, b in _modules]
                for idx, (m, b) in enumerate(_modules):
                    if b == "latest":
                        b = repo.external.latest_module_tag(m)
                        _modules[idx] = (m, b)
                    if b not in repo.external.module_branches(m):
                        raise InvalidBranchError(m, b)    
            
            return func(repo, _modules, **kwargs)

        return inner
    return wrapper


@click.group(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        show_default=True,
    ),
)
@click.option(
    "--modules-repo",
    "modules_repo_url",
    type=str,
    help="URL to the module repository",
    default="https://github.com/ClinicalGenomicsGBG/cellophane_modules",
)
@click.option(
    "--path",
    type=click.Path(path_type=Path),
    help="Path to the cellophane project",
    default=Path("."),
)
@click.option(
    "--log_level",
    type=str,
    help="Log level",
    default="INFO",
    callback=lambda ctx, param, value: value.upper(),
)
@click.pass_context
def main(ctx: click.Context, path: Path, log_level: str, modules_repo_url: str) -> None:
    """Cellophane

    A library for writing modular wrappers
    """
    ctx.ensure_object(dict)
    logs.setup_logging().setLevel(log_level)

    ctx.obj["logger"] = logging.LoggerAdapter(
        logging.getLogger(), {"label": "cellophane"}
    )
    ctx.obj["logger"].setLevel(log_level)
    ctx.obj["path"] = path
    ctx.obj["log_level"] = log_level
    ctx.obj["modules_repo_url"] = modules_repo_url


@main.command()
@click.argument(
    "command",
    metavar="COMMAND",
    type=click.Choice(["add", "rm", "update"]),
    required=True,
)
@click.argument(
    "modules",
    metavar="MODULE[@BRANCH] ...",
    callback=lambda ctx, param, module_strings: [
        tuple(m.split("@")) if "@" in m else (m, None) for m in module_strings
    ],
    nargs=-1,
)
@click.pass_context
def module(
    ctx: click.Context,
    command: Literal["add", "update", "rm"],
    modules: list[tuple[str, str]] | list[tuple[str, None]] | None,
) -> None:
    """Manage modules

    COMMAND: add|update|rm
    """
    ctx.ensure_object(dict)
    _logger: logging.LoggerAdapter = ctx.obj["logger"]
    _path: Path = ctx.obj["path"]

    try:
        _repo = CellophaneRepo(_path, ctx.obj["modules_repo_url"])
    except InvalidGitRepositoryError as e:
        _logger.critical(e, exc_info=ctx.obj["log_level"] == "DEBUG")
        raise SystemExit(1) from e
    else:
        if _repo.is_dirty():
            _logger.critical("Repository has uncommited changes")
            raise SystemExit(1)

    try:
        common_kwargs = dict(
            modules=modules,
            repo=_repo,
            path=_path,
            logger=_logger,
            log_level=ctx.obj["log_level"],
        )
    
        match command:
            case "add":
                add(**common_kwargs, valid_modules = _repo.absent_modules)
            case "rm":
                rm(**common_kwargs, valid_modules = _repo.present_modules)
            case "update":
                update(**common_kwargs, valid_modules = _repo.present_modules)

    except NoModulesError as exc:
        _logger.warning(exc)
        raise SystemExit(1) from exc
    except (InvalidModuleError, InvalidBranchError) as exc:
        _logger.critical(exc)
        raise SystemExit(1) from exc
    except Exception as exc:
        _logger.critical(
            f"Unhandled Exception: {repr(exc)}",
            exc_info=ctx.obj["log_level"] == "DEBUG",
        )
        raise SystemExit(1) from exc



@_validate_modules()
def add(
    repo: CellophaneRepo,
    modules: list[tuple[str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
    log_level: str,
) -> None:
    """Add module(s)"""

    for _module, branch in modules:
        submodule = None
        try:
            submodule = repo.create_submodule(
                name=_module,
                path=path / "modules" / _module,
                url=repo.external.url,
                branch=f"{_module}_{branch}",
            )
            _update_example_config(path)
            _add_requirements(path, _module)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                f"Unable to add '{_module}@{branch}': {repr(exc)}",
                exc_info=log_level == "DEBUG",
            )
            if submodule is not None:
                submodule.remove(force=True)
                repo.index.reset(paths=[".gitmodules"])
            repo.git.restore("config.example.yaml")
            continue
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Added '{_module}@{branch}'")
            logger.info(f"Added '{_module}@{branch}'")

@_validate_modules()
def update(
    repo: Repo,
    modules: list[tuple[str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
    log_level: str,
    **kwargs: Any,
) -> None:
    """Update module(s)"""
    del kwargs  # Unused

    for _module, branch in modules:
        submodule = None
        _path = None
        try:
            submodule = repo.submodule(_module)
            _name = submodule.name
            _path = submodule.path
            _url = submodule.url
            submodule.remove(force=True)
            repo.create_submodule(
                name=_name,
                path=_path,
                url=_url,
                branch=f"{_module}_{branch}",
            )
            _update_example_config(path)
            _remove_requirements(path, _module)
            _add_requirements(path, _module)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                f"Unable to update '{_module}->{branch}': {repr(exc)}",
                exc_info=log_level == "DEBUG",
            )
            if submodule is not None:
                submodule.remove(force=True)
            if _path is not None:
                repo.git.checkout(
                    "HEAD", "--", _path, ".gitmodules", "config.example.yaml"
                )
            continue
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"chore(cellophane): Updated '{_module}->{branch}'")
            logger.info(f"Updated '{_module}->{branch}'")

@_validate_modules(ignore_branch=True)
def rm(
    repo: CellophaneRepo,
    modules: list[tuple[str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
    log_level: str,
    **kwargs: Any,
) -> None:
    """Remove module"""

    del kwargs  # Unused

    _path = None
    for _module, _ in modules:
        submodule = None
        try:
            submodule = repo.submodule(_module)
            _path = submodule.path
            submodule.remove()
            _update_example_config(path)
            _remove_requirements(path, _module)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                f"Unable to remove '{_module}': {repr(exc)}",
                exc_info=log_level == "DEBUG",
            )
            if _path is not None:
                repo.git.checkout(
                    "HEAD", "--", _path, ".gitmodules", "config.example.yaml"
                )
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Removed '{_module}'")
            logger.info(f"Removed '{_module}'")


@main.command()
@click.option(
    "--force",
    is_flag=True,
    help="Force initialization of non-empty directory",
    default=False,
)
@click.argument(
    "name",
    type=str,
)
@click.pass_context
def init(ctx: click.Context, name: str, force: bool) -> None:
    """Initialize a new cellophane project

    If no path is specified, the current directory will be used.
    If the path is not a git repository, it will be initialized as one.
    """
    path: Path = ctx.obj["path"]
    logger: logging.LoggerAdapter = ctx.obj["logger"]
    logger.info(f"Initializing new cellophane project at {path}")

    try:
        CellophaneRepo.initialize(
            name=name,
            path=path,
            force=force,
            modules_repo_url=ctx.obj["modules_repo_url"],
        )
    except FileExistsError as e:
        logger.critical("Project path is not empty (--force to ignore)")
        raise SystemExit(1) from e
    except Exception as e:
        logger.critical(e, exc_info=ctx.obj["log_level"] == "DEBUG")
        raise SystemExit(1) from e


if __name__ == "__main__":  # pragma: no cover
    click.rich_click.DEFAULT_STRING = "[{}]"
    main()  # pylint: disable=no-value-for-parameter
