"""Repo classes for the cellophane dev command-line interface."""

import json
import re
from functools import cached_property
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

from git import GitCommandError, InvalidGitRepositoryError, Repo
from packaging.version import Version as PyPIVersion
from semver import Version

from cellophane import CELLOPHANE_VERSION

from .exceptions import InvalidModulesRepoError, InvalidProjectRepoError


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
    def from_url(cls, url: str, branch: str) -> "ModulesRepo":
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
                branch=branch,
                to_path=_path,
                checkout=False,
            )  # type: ignore[return-value]
        except Exception as exc:
            raise InvalidModulesRepoError(url) from exc

    @cached_property
    def modules(self) -> dict[str, Any]:
        """
        Retrieves the list of modules in the repository.


        Uses `git ls_tree` to retrieve the list of subdirectories in the repository.
        All non-hidden directories at the base level are considered modules.

        Returns:
            List[str]: The list of module names.
        """
        try:
            json_ = self.git.show(f"origin/{self.active_branch.name}:modules.json")
        except GitCommandError as exc:
            raise InvalidModulesRepoError(
                self.url, msg="Could not parse modules.json"
            ) from exc
        return json.loads(json_)

    @property
    def url(self) -> str:
        """Returns the URL of the repository."""
        return self.remote("origin").url


class ProjectRepo(Repo):
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
        modules_repo_branch: str,
        **kwargs: Any,
    ) -> None:
        try:
            super().__init__(str(path), **kwargs)
        except InvalidGitRepositoryError as exc:
            raise InvalidProjectRepoError(path) from exc

        self.external = ModulesRepo.from_url(
            url=modules_repo_url,
            branch=modules_repo_branch,
        )

    @property
    def modules(self) -> set[str]:
        """
        Retrieves the list of modules in the repository.

        Returns:
            List[str]: The list of module names.
        """
        return {
            name
            for name, item in self.external.modules.items()
            if (Path("modules") / name).exists()
        }

    @property
    def absent_modules(self) -> set[str]:
        """
        Retrieves the list of modules not added to the project.

        Returns:
            List[str]: List modules not added to the project.
        """

        return {*self.external.modules} - self.modules

    def compatible_versions(self, module: str) -> set[tuple[str, str]]:
        """
        Retrieves the set of compatible versions for the specified module.

        Args:
            module (str): The name of the module.

        Returns:
            Set[str]: The set of compatible versions.
        """
        pypi = PyPIVersion(CELLOPHANE_VERSION)
        semver = Version(
            major=pypi.major,
            minor=pypi.minor,
            patch=pypi.micro,
            prerelease="dev" if pypi.is_devrelease else None,
        )
        compatible = set()
        for version, meta in self.external.modules[module]["versions"].items():
            for c in meta["cellophane"]:
                try:
                    if semver.match(c):
                        compatible.add((version, meta["tag"]))
                        break
                except ValueError:
                    if re.fullmatch(c, CELLOPHANE_VERSION) or semver.prerelease == c:
                        compatible.add((version, meta["tag"]))
                        break

        return compatible
