"""Cleaner class for cleaning up files and directories."""

from copy import copy
from functools import cached_property
from logging import LoggerAdapter
from pathlib import Path
from shutil import rmtree
from typing import Literal
from warnings import warn

from attrs import define, field
from humanfriendly.text import pluralize


def _resolve_path(path: Path, root: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    elif path.is_relative_to(root):
        return (root / path.relative_to(root)).resolve()
    else:
        return (root / path).resolve()


@define(frozen=True, on_setattr=None)
class DeferredCall:
    action: Literal["register", "unregister"]
    path: Path
    ignore_outside_root: bool = False


@define(frozen=True, on_setattr=None)
class DeferredCleaner:
    root: Path
    calls: list[DeferredCall] = field(init=False, factory=list)

    def register(self, path: Path, ignore_outside_root: bool = False) -> None:
        self._add_call("register", path, ignore_outside_root)

    def unregister(self, path: Path, ignore_outside_root: bool = False) -> None:
        self._add_call("unregister", path, ignore_outside_root)

    def _add_call(
        self,
        action: Literal["register", "unregister"],
        path: str | Path,
        ignore_outside_root: bool,
    ) -> None:
        if ignore_outside_root:
            warn(
                f"Deferred cleaner does not support {action}ing "
                "paths outside the root directory",
            )
            return
        rpath = _resolve_path(Path(path), self.root)
        self.calls.append(DeferredCall(action, rpath, False))

    def clean(self) -> None:
        warn("Deferred cleaner does not support cleaning")


@define(frozen=True, on_setattr=None)
class Cleaner:
    root: Path
    trash: set[Path] = field(init=False, factory=set)

    @cached_property
    def _abs_root(self) -> Path:
        return self.root.resolve()

    def register(
        self,
        path: str | Path,
        ignore_outside_root: bool = False,
    ) -> None:
        rpath = _resolve_path(Path(path), self.root)
        if not ignore_outside_root and not rpath.is_relative_to(self._abs_root):
            warn(f"Refusing to register {rpath} outside {self.root}")
            return

        self.trash.add(rpath)

    def unregister(
        self,
        path: Path,
        ignore_outside_root: bool = False,
    ) -> None:
        rpath = _resolve_path(Path(path), self.root)
        if not ignore_outside_root and not rpath.is_relative_to(self._abs_root):
            warn(f"Refusing to unregister {rpath} outside {self.root}")
            return

        for parent in rpath.parents[::-1]:
            if parent in self.trash:
                self.trash.remove(parent)
                self.trash.update(parent.iterdir())

        if rpath.is_dir():
            for t in self.trash.copy():
                if t.is_relative_to(rpath):
                    self.trash.remove(t)

        self.trash.discard(rpath)

    def clean(self, logger: LoggerAdapter) -> None:
        n_dir = sum(t.is_dir() for t in self.trash)
        n_file = len(self.trash) - n_dir
        n_external = sum(not t.is_relative_to(self._abs_root) for t in self.trash)

        logger.info(
            f"Cleaning up {pluralize(n_file, 'file', 'files')} "
            f"and {pluralize(n_dir, 'directory', 'directories')}",
        )
        if n_external:
            logger.warning(
                f"Removing {pluralize(n_external, 'path', 'paths')} "
                f"outside {self.root}",
            )
        for path in self.trash:
            if not path.exists():
                logger.debug(f"Path {path} does not exist")
                continue
            if not path.is_relative_to(self._abs_root):
                warn(f"Removing path outside {self.root}: {path}")

            if path.is_relative_to(self._abs_root):
                relpath = self.root / path.relative_to(self._abs_root)
            else:
                relpath = path
            logger.debug(f"Removing {relpath}")
            try:
                if path.is_dir():
                    rmtree(path)
                else:
                    path.unlink()
            except BaseException as exc:
                warn(f"Failed to remove {path}: {exc!r}")

    def __and__(self, other: "DeferredCleaner") -> "Cleaner":
        cleaner = copy(self)
        for call in other.calls:
            match call:
                case DeferredCall("register", path, ignore_outside_root):
                    cleaner.register(path, ignore_outside_root)
                case DeferredCall("unregister", path, ignore_outside_root):
                    cleaner.unregister(path, ignore_outside_root)
                case _:
                    raise ValueError(f"Invalid action: {call.action}")
        return cleaner
