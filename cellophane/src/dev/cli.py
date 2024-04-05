"""CLI for managing cellophane projects"""

import logging
from pathlib import Path
from typing import Any, Literal

import rich_click as click
from git.exc import GitCommandError

from cellophane import logs

from .exceptions import (
    InvalidModuleError,
    InvalidModulesRepoError,
    InvalidProjectRepoError,
    InvalidVersionError,
    NoModulesError,
)
from .repo import ProjectRepo
from .util import (
    add_requirements,
    initialize_project,
    remove_requirements,
    update_example_config,
    with_modules,
)


@click.group(
    context_settings={
        "help_option_names": ["-h", "--help"],
        "show_default": True,
    },
)
@click.option(
    "--modules-repo",
    "modules_repo_url",
    type=str,
    help="URL to the module repository",
    default="https://github.com/ClinicalGenomicsGBG/cellophane_modules",
)
@click.option(
    "--modules-branch",
    "modules_repo_branch",
    type=str,
    help="Branch to use for the module repository",
    default="main",
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
def main(
    ctx: click.Context,
    path: Path,
    log_level: str,
    modules_repo_url: str,
    modules_repo_branch: str,
) -> None:
    """Cellophane

    A library for writing modular wrappers
    """
    ctx.ensure_object(dict)
    logs.setup_console_handler().setLevel(log_level)

    ctx.obj["logger"] = logging.LoggerAdapter(
        logging.getLogger(), {"label": "cellophane"}
    )
    ctx.obj["logger"].setLevel(log_level)
    ctx.obj["path"] = path
    ctx.obj["modules_repo_url"] = modules_repo_url
    ctx.obj["modules_repo_branch"] = modules_repo_branch


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
        _repo = ProjectRepo(
            _path,
            ctx.obj["modules_repo_url"],
            ctx.obj["modules_repo_branch"],
        )
    except InvalidProjectRepoError as exception:
        _logger.critical(exception, exc_info=True)
        raise SystemExit(1) from exception  # pylint: disable=bad-exception-cause

    if _repo.is_dirty():
        _logger.critical("Repository has uncommited changes")
        raise SystemExit(1)

    try:
        common_kwargs = {
            "modules": modules,
            "repo": _repo,
            "path": _path,
            "logger": _logger,
        }

        match command:
            case "add":
                add(**common_kwargs, valid_modules=_repo.absent_modules)
            case "rm":
                rm(**common_kwargs, valid_modules=_repo.modules)
            case "update":
                update(**common_kwargs, valid_modules=_repo.modules)

    except NoModulesError as exc:
        _logger.warning(exc)
        raise SystemExit(1) from exc
    except (InvalidModuleError, InvalidVersionError) as exc:
        _logger.critical(exc)
        raise SystemExit(1) from exc
    except InvalidModulesRepoError as exc:
        _logger.critical(exc)
        raise SystemExit(1) from exc
    except Exception as exc:
        _logger.critical(
            f"Unhandled Exception: {repr(exc)}",
            exc_info=True,
        )
        raise SystemExit(1) from exc


@with_modules()
def add(
    repo: ProjectRepo,
    modules: list[tuple[str, str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
) -> None:
    """Add module(s)"""

    for module_, ref, version in modules:

        try:
            remote = repo.create_remote("modules", repo.external.url)
        except GitCommandError as exc:
            # Remote already exists
            if exc.status == 3:
                remote = repo.remotes["modules"]
                remote.set_url(repo.external.url)
        try:
            remote.fetch()
            ref_ = ref if ref in [r.name for r in repo.tags] else f"modules/{ref}"
            repo.git.read_tree(
                f"--prefix=modules/{module_}/",
                "-u",
                f"{ref_}:{repo.external.modules[module_]['path']}",
            )
            update_example_config(path)
            add_requirements(path, module_)

        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                f"Unable to add '{module_}@{version}': {repr(exc)}",
                exc_info=True,
            )
            repo.head.reset("HEAD", index=True, working_tree=True)
            continue
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Added '{module_}@{version}'")
            logger.info(f"Added '{module_}@{version}'")


@with_modules()
def update(
    repo: ProjectRepo,
    modules: list[tuple[str, str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
    **kwargs: Any,
) -> None:
    """Update module(s)"""
    del kwargs  # Unused

    for module_, ref, version in modules:
        try:
            ref_ = ref if ref in [r.name for r in repo.tags] else f"modules/{ref}"
            repo.index.remove(path / f"modules/{module_}", working_tree=True, r=True)
            repo.git.read_tree(
                f"--prefix=modules/{module_}/",
                "-u",
                f"{ref_}:{repo.external.modules[module_]['path']}",
            )
            update_example_config(path)
            remove_requirements(path, module_)
            add_requirements(path, module_)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                f"Unable to update '{module_}->{version}': {repr(exc)}", exc_info=True
            )
            repo.head.reset("HEAD", index=True, working_tree=True)
            continue
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"chore(cellophane): Updated '{module_}->{version}'")
            logger.info(f"Updated '{module_}->{version}'")


@with_modules(ignore_branch=True)
def rm(
    repo: ProjectRepo,
    modules: list[tuple[str, str, str]],
    path: Path,
    logger: logging.LoggerAdapter,
    **kwargs: Any,
) -> None:
    """Remove module"""

    del kwargs  # Unused

    for module_, _, _ in modules:
        try:
            repo.index.remove(path / f"modules/{module_}", working_tree=True, r=True)
            update_example_config(path)
            remove_requirements(path, module_)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(f"Unable to remove '{module_}': {repr(exc)}", exc_info=True)
            repo.head.reset("HEAD", index=True, working_tree=True)
        else:
            repo.index.add("config.example.yaml")
            repo.index.add("modules/requirements.txt")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Removed '{module_}'")
            logger.info(f"Removed '{module_}'")


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
        initialize_project(
            name=name,
            path=path,
            force=force,
            modules_repo_url=ctx.obj["modules_repo_url"],
            modules_repo_branch=ctx.obj["modules_repo_branch"],
        )
    except FileExistsError as e:
        logger.critical("Project path is not empty (--force to ignore)")
        raise SystemExit(1) from e
    except Exception as e:
        logger.critical(f"Unhandeled exception: {e}", exc_info=True)
        raise SystemExit(1) from e
