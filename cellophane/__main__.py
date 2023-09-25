import logging
import re
from functools import cached_property, lru_cache
from pathlib import Path
from tempfile import mkdtemp
from typing import Literal, Sequence

import rich_click as click
from git.exc import InvalidGitRepositoryError
from git.repo import Repo
from questionary import checkbox, select, Choice

from . import CELLOPHANE_ROOT, cfg, logs


class InvalidModuleError(Exception):
    def __init__(self, module: str, msg=None):
        self.module = module
        super().__init__(msg or f"Module '{module}' is not valid")


class InvalidBranchError(Exception):
    def __init__(self, module: str, branch: str, msg=None):
        self.module = module
        self.branch = branch
        super().__init__(msg or f"Branch '{branch}' is not valid for module '{module}'")


class NoModulesError(Exception):
    def __init__(self):
        super().__init__("No modules to select from")


class InvalidModulesRepoError(InvalidGitRepositoryError):
    def __init__(self, url, *args, msg=None, **kwargs):
        super().__init__(
            msg or f"Invalid modules repository ({url})",
            *args,
            **kwargs,
        )


class InvalidCellophaneRepoError(InvalidGitRepositoryError):
    def __init__(self, path, *args, msg=None, **kwargs):
        super().__init__(
            msg or f"Invalid cellophane repository ({path})",
            *args,
            **kwargs,
        )


class ModulesRepo(Repo):
    @classmethod
    def from_url(cls, url):
        _path = mkdtemp(prefix="cellophane_modules_")
        try:
            return cls.clone_from(url, _path, checkout=False)
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
    def module_branches(self, module: str):
        return [
            b.removeprefix(module).lstrip("_")
            for b in self._branches
            if all(
                (
                    b.startswith(module),
                    b != module,
                )
            )
        ]

    @lru_cache
    def latest_module_tag(self, module: str):
        if tags := [t for t in self.tags if t.name in self.module_branches(module)]:
            # FIXME: This assumes that the most recent release is the latest version
            return sorted(tags, key=lambda t: t.object.committed_date)[-1].name
        else:
            raise AttributeError(f"Could not find any releases for {module}")

    @property
    def url(self):
        return self.remote("origin").url


class CellophaneRepo(Repo):
    external: ModulesRepo

    def __init__(self, path: Path, modules_repo_url: str | None = None, **kwargs):
        try:
            super().__init__(str(path), **kwargs)
        except InvalidGitRepositoryError as e:
            raise InvalidCellophaneRepoError(path) from e

        self.external = ModulesRepo.from_url(modules_repo_url)

    @classmethod
    def initialize(cls, name, path: Path, modules_repo_url: str, force=False):
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

        with (
            open(CELLOPHANE_ROOT / "template" / "__main__.py", "r") as main_handle,
            open(CELLOPHANE_ROOT / "template" / "entrypoint.py", "r") as entry_handle,
            open(path / f"{_prog_name}.py", "w") as entry_dest_handle,
            open(path / "__main__.py", "w") as main_dest_handle,
        ):
            base = main_handle.read()
            main_dest_handle.write(base.format(label=name, prog_name=_prog_name))
            entry_dest_handle.write(entry_handle.read())

        _update_example_config(path)

        repo = Repo.init(str(path))

        repo.index.add(
            [
                path / "modules" / "__init__.py",
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
        return [sm.name for sm in self.submodules]

    @property
    def absent_modules(self) -> list[str]:
        return [*{*self.external.modules} - {*self.modules}]

    @property
    def present_modules(self) -> list[str]:
        return [*{*self.modules} & {*self.external.modules}]


def _update_example_config(path: Path):
    # FIXME: Add support for manually defined examples
    schema = cfg.Schema.from_file(
        path=[
            CELLOPHANE_ROOT / "schema.base.yaml",
            path / "schema.yaml",
            *(path / "modules").glob("**/schema.yaml"),
        ],
    )

    with open(path / "config.example.yaml", "w") as handle:
        handle.write(schema.example_config)


def _ask_modules(valid_modules: Sequence[str]):
    if not valid_modules:
        raise NoModulesError
    return checkbox(
        "Select module(s)",
        choices=[Choice(title=m, value=(m, None)) for m in valid_modules],
        erase_when_done=True,
        validate=lambda x: len(x) > 0 or "Select at least one module",
    ).ask()


def _ask_branch(module: str, modules_repo: ModulesRepo):
    _branch = select(
        f"Select branch for {module}",
        # FIXME: Should the number of branches be limited?
        choices=["latest", *modules_repo.module_branches(module)],
        default="latest",
        erase_when_done=True,
    ).ask()
    if _branch == "latest":
        _branch = modules_repo.latest_module_tag(module)

    return _branch


def _validate_modules(modules, repo, valid_modules, ignore_branch=False):
    for idx, (module, branch) in enumerate(modules):
        if module not in valid_modules:
            raise InvalidModuleError(module)

        if not ignore_branch and branch is None:
            branch = _ask_branch(module, repo.external)

        if branch == "latest":
            branch = repo.external.latest_module_tag(module)

        modules[idx] = (module, branch)

        if not ignore_branch and branch not in repo.external.module_branches(module):
            raise InvalidBranchError(module, branch)

    return modules


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
def main(ctx: click.Context, path: Path, log_level: str, modules_repo_url: str):
    """Cellophane

    A library for writing modular wrappers
    """
    ctx.ensure_object(dict)

    ctx.obj["logger"] = logs.get_labeled_adapter("cellophane")
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
    modules: list[tuple[str, str]] | None,
):
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
        match command:
            case "add":
                _command = add
                _modules = modules or _ask_modules(_repo.absent_modules)
            case "rm":
                _command = rm
                _modules = modules or _ask_modules(_repo.present_modules)
            case "update":
                _command = update
                _modules = modules or _ask_modules(_repo.present_modules)

        _modules = _validate_modules(
            modules=_modules,
            repo=_repo,
            ignore_branch=command == "rm",
            valid_modules=(
                _repo.absent_modules if command == "add" else _repo.present_modules
            ),
        )

    except NoModulesError as e:
        _logger.warning(e)
        raise SystemExit(1) from e
    except (InvalidModuleError, InvalidBranchError) as e:
        _logger.critical(e)
        raise SystemExit(1) from e
    except Exception as e:
        _logger.critical(e, exc_info=ctx.obj["log_level"] == "DEBUG")
        raise SystemExit(1) from e

    _command(
        path=_path,
        repo=_repo,
        modules=_modules,
        logger=_logger,
        log_level=ctx.obj["log_level"],
    )


def add(
    path: Path,
    repo: CellophaneRepo,
    logger: logging.LoggerAdapter,
    log_level: str,
    modules: list[tuple[str, str]],
):
    """Add module(s)"""

    added = []
    for module, branch in modules:
        logger.info(f"Adding module {module} ({branch})")
        try:
            sm = repo.create_submodule(
                name=module,
                path=path / "modules" / module,
                url=repo.external.url,
                branch=f"{module}_{branch}",
            )
            repo.index.add([sm])
        except Exception as e:
            logger.error(e, exc_info=log_level == "DEBUG")
            continue
        else:
            added.append(module)

    if added:
        try:
            _update_example_config(path)
            repo.index.add("config.example.yaml")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Add module(s) {', '.join(added)}")

        except Exception as e:
            logger.critical(e, exc_info=log_level == "DEBUG")
            raise SystemExit(1) from e


def update(
    path: Path,
    repo: Repo,
    logger: logging.LoggerAdapter,
    log_level: str,
    modules: list[tuple[str, str]],
    **_,
):
    """Update module(s)"""
    updated = []
    for module, branch in modules:
        logger.info(f"Updating module {module} ({branch})")
        try:
            sm_prev = repo.submodule(module)
            module_url, module_path = sm_prev.url, sm_prev.path
            sm_prev.remove(force=True)
            sm = repo.create_submodule(
                name=sm_prev.name,
                path=module_path,
                url=module_url,
                branch=f"{module}_{branch}",
            )
            repo.index.add([sm])
        except Exception as e:
            logger.error(e, exc_info=log_level == "DEBUG")
            continue
        else:
            updated.append(module)

    if updated:
        try:
            _update_example_config(path)
            repo.index.add(
                [
                    path / ".gitmodules",
                    path / "config.example.yaml",
                ]
            )
            repo.index.write()
            repo.index.commit(
                f"chore(cellophane): Updated module(s) {', '.join(updated)}"
            )
        except Exception as e:
            logger.critical(e, exc_info=log_level == "DEBUG")
            raise SystemExit(1) from e


def rm(
    path: Path,
    repo: CellophaneRepo,
    logger: logging.LoggerAdapter,
    log_level: str,
    modules: list[tuple[str, str]],
    **_,
):
    """Remove module"""

    removed = []
    for module, _ in modules:
        try:
            sm = repo.submodule(module)
            logger.info(f"Removing module {module}")
            sm.remove()
        except Exception as e:
            logger.error(e)
            continue
        else:
            removed.append(module)

    if removed:
        try:
            _update_example_config(path)
            repo.index.add(
                [
                    path / ".gitmodules",
                    path / "config.example.yaml",
                ]
            )
            repo.index.write()
            repo.index.commit(
                f"feat(cellophane): Removed module(s) {', '.join(removed)}"
            )
        except Exception as e:
            logger.critical(e, exc_info=log_level == "DEBUG")
            raise SystemExit(1) from e


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
def init(ctx: click.Context, name: str, force: str):
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
            modules_repo_url=ctx.obj["modules_repo_url"]
        )
    except FileExistsError as e:
        logger.critical("Project path is not empty (--force to ignore)")
        raise SystemExit(1) from e
    except Exception as e:
        logger.critical(e, exc_info=ctx.obj["log_level"] == "DEBUG")
        raise SystemExit(1) from e


if __name__ == "__main__":  # pragma: no cover
    click.rich_click.DEFAULT_STRING = "[{}]"
    logs.setup_logging()
    main()
