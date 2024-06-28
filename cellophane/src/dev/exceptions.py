"""Exceptions for the cellophane dev command-line interface."""

from pathlib import Path
from typing import Any

from git import InvalidGitRepositoryError


class InvalidModuleError(Exception):
    """Exception raised when a module is not valid.

    Args:
    ----
        _module (str): The name of the module.
        msg (str | None): The error message (default: None).

    """

    def __init__(self, _module: str, msg: str | None = None):
        self.module = _module
        super().__init__(msg or f"Module '{_module}' is not valid")


class InvalidVersionError(Exception):
    """Exception raised when a module is not valid.

    Args:
    ----
        _module (str): The name of the module.
        branch (str): The name of the branch.
        msg (str | None): The error message (default: None).

    """

    def __init__(
        self, _module: str, branch: str | None, msg: str | None = None,
    ) -> None:
        self.module = _module
        self.branch = branch
        super().__init__(msg or f"Version '{branch}' is invalid for '{_module}'")


class NoModulesError(Exception):
    """Exception raised when there are no modules to select from.
    """

    def __init__(self, msg: str | None = None) -> None:
        super().__init__(msg)


class NoVersionsError(Exception):
    """Exception raised when there are no versions to select from.
    """

    def __init__(self, msg: str | None = None) -> None:
        super().__init__(msg)


class InvalidModulesRepoError(InvalidGitRepositoryError):
    """Exception raised when the modules repository is invalid.

    Args:
    ----
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


class InvalidProjectRepoError(InvalidGitRepositoryError):
    """Exception raised when the project repository is invalid.

    Args:
    ----
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
