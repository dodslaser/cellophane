"""Utility functions for cellophane dev command-line interface."""

import re
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

from git import Repo
from questionary import Choice, checkbox, select
from rich.console import Console

from cellophane import CELLOPHANE_ROOT, Schema

from .exceptions import (
    InvalidModuleError,
    InvalidVersionError,
    NoModulesError,
    NoVersionsError,
)
from .repo import ProjectRepo


def add_requirements(path: Path, _module: str) -> None:
    """
    Add module requirements to the global requirements file.

    Args:
      path (Path): The path to the root of the application.
      _module (str): The name of the module.
    """
    requirements_path = path / "modules" / "requirements.txt"
    module_path = path / "modules" / _module

    if (
        module_path.is_dir()
        and (module_path / "requirements.txt").exists()
        and (spec := f"-r {_module}/requirements.txt\n")
        not in requirements_path.read_text()
    ):
        with open(requirements_path, "a", encoding="utf-8") as handle:
            handle.write(spec)


def remove_requirements(path: Path, _module: str) -> None:
    """
    Remove a specific module's requirements from the project's requirements.txt file.

    Args:
      path (Path): The path to the project directory.
      _module (str): The name of the module to remove requirements for.
    """
    requirements_path = path / "modules" / "requirements.txt"

    if (spec := f"-r {_module}/requirements.txt\n") in (
        requirements := requirements_path.read_text()
    ):
        with open(requirements_path, "w", encoding="utf-8") as handle:
            handle.write(requirements.replace(spec, ""))


def update_example_config(path: Path) -> None:
    """
    Update the example configuration file.

    Args:
      path (Path): The path to the root of the application.
    """
    schema = Schema.from_file(
        path=[
            CELLOPHANE_ROOT / "schema.base.yaml",
            path / "schema.yaml",
            *(path / "modules").glob("**/schema.yaml"),
        ],
    )

    with open(path / "config.example.yaml", "w", encoding="utf-8") as handle:
        handle.write(schema.example_config)


def ask_modules(valid_modules: Iterable[str]) -> list[tuple[str, None, None]]:
    """
    Ask the user to select one or more modules.

    Args:
      valid_modules (Sequence[str]): The valid modules to select from.
    """
    if not valid_modules:
        raise NoModulesError("No modules to select from")

    _modules = checkbox(
        "Select module(s)",
        choices=[Choice(title=m, value=(m, None, None)) for m in valid_modules],
        erase_when_done=True,
        validate=lambda x: len(x) > 0 or "Select at least one module",
    ).ask()
    Console().show_cursor()
    if _modules:
        return _modules

    raise NoModulesError("No modules selected")


def ask_version(module_: str, valid: Iterable[tuple[str, str]]) -> tuple[str, str, str]:
    """
    Ask the user to select a version for a module.

    Args:
      _module (str): The name of the module.
      modules_repo (ModulesRepo): The modules repository.
    """
    if not valid:
        raise NoVersionsError("No compatible versions to select from")

    _versions = select(
        f"Select version for {module_}",
        choices=[
            Choice(title=version, value=(module_, tag, version))
            for version, tag in valid
        ],
        erase_when_done=True,
    ).ask()
    Console().show_cursor()

    if _versions:
        return _versions

    raise NoVersionsError("No version selected")


def with_modules(ignore_branch: bool = False) -> Callable:
    """Decorator for commands that require modules."""

    def wrapper(func: Callable) -> Callable:
        @wraps(func)
        def inner(
            modules: list[tuple[str, str]] | list[tuple[str, None]] | None,
            valid_modules: Sequence[str],
            repo: ProjectRepo,
            **kwargs: Any,
        ) -> None:
            if invalid_modules := {m for m, _ in modules or []} - set(valid_modules):
                raise InvalidModuleError(invalid_modules.pop())

            if invalid_versions := {
                (m, v)
                for m, v in modules or []
                if v is not None
                and v != "latest"
                and v not in repo.external.modules[m]["versions"]
            }:
                raise InvalidVersionError(*invalid_versions.pop())

            modules_: (
                list[tuple[str, None, str | None]]
                | list[tuple[str, None, None]]
                | list[tuple[str, str, str]]
            )
            if modules:
                modules_ = [(m, None, v) for m, v in modules]
            else:
                modules_ = ask_modules(valid_modules)

            for idx, module in enumerate(modules_):
                match module:
                    case _ if ignore_branch:
                        pass
                    case (m, None, None):
                        modules_[idx] = ask_version(
                            m, repo.compatible_versions(m)
                        )  # type: ignore[assignment]
                    case (m, None, "latest"):
                        version = repo.external.modules[m].get("latest")
                        if version is None:
                            raise InvalidVersionError(m, "latest")
                        tag = repo.external.modules[m]["versions"][version]["tag"]
                        modules_[idx] = (m, tag, version)
                    case (m, None, v):
                        tag = repo.external.modules[m]["versions"][v]["tag"]
                        modules_[idx] = (m, tag, v)

            return func(repo, modules_, **kwargs)

        return inner

    return wrapper


def initialize_project(
    name: str,
    path: Path,
    modules_repo_url: str,
    modules_repo_branch: str,
    force: bool = False,
) -> ProjectRepo:
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

    (path / ".gitignore").write_text(
        (CELLOPHANE_ROOT / "template" / ".gitignore")
        .read_text(encoding="utf-8")
        .format(label=name, prog_name=_prog_name)
    )

    (path / "modules" / "requirements.txt").write_text(
        (CELLOPHANE_ROOT / "template" / "modules" / "requirements.txt")
        .read_text(encoding="utf-8")
        .format(label=name, prog_name=_prog_name)
    )

    update_example_config(path)

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
            path / ".gitignore",
        ]
    )
    repo.index.write()
    repo.index.commit("feat(cellophane): Initial commit from cellophane 🎉")

    return ProjectRepo(path, modules_repo_url, modules_repo_branch)
