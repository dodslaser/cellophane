import logging
import multiprocessing as mp
import re
from functools import cached_property, lru_cache
from pathlib import Path
from tempfile import mkdtemp
from typing import Iterator

import rich_click as click
from git.exc import InvalidGitRepositoryError
from git.refs import TagReference
from git.repo import Repo
from questionary import checkbox, select

from . import CELLOPHANE_ROOT, cfg, logs


class ModulesRepo:
    def __init__(self, url: str):
        self.url = url
        self.path = mkdtemp()
        self.repo = Repo.clone_from(url, self.path, no_checkout=True)

    @cached_property
    def branches(self) -> list[str]:
        return [
            r.name.split("/")[-1]
            for r in self.repo.remote("origin").refs
            if r.name != "origin/HEAD"
        ]

    @cached_property
    def modules(self) -> list[str]:
        return [
            m
            for m in self.repo.git.ls_tree(
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

    @cached_property
    def tags(self) -> list[TagReference]:
        return [*self.repo.tags]

    @lru_cache
    def module_branches(self, module: str):
        return [
            b.removeprefix(module).lstrip("_")
            for b in self.branches
            if all(
                (
                    b.startswith(module),
                    b != module,
                )
            )
        ]

    @lru_cache
    def latest_module_tag(self, module: str):
        tags = [t for t in self.tags if t.name in self.module_branches(module)]
        if tags:
            # FIXME: This assumes that the most recent release is the latest version
            return sorted(tags, key=lambda t: t.object.committed_date)[-1].name
        else:
            raise Exception(f"Could not find any releases for {module}")

    def get_module_branch(
        self,
        *,
        action: str,
        module: str | None,
        branch: str | None,
        valid_modules: list[str] | None = None,
        valid_branches: list[str] | None = None,
    ) -> Iterator[tuple[str, str, str] | None]:
        if module is None:
            modules: list[str] = checkbox(
                f"Select module(s) to {action}",
                choices=valid_modules or self.modules,
                erase_when_done=True,
            ).ask()
            if modules is None:
                logger.warning("No module(s) selected")
                raise SystemExit(0)
        elif module in self.modules:
            modules = [module]
        else:
            raise Exception(f"Could not find module {module}")

        for mod in modules:
            if branch is None:
                _branch: str = select(
                    f"Select branch for {mod}",
                    choices=[
                        "latest",
                        "main",
                        *(valid_branches or self.module_branches(mod)),
                    ],
                    erase_when_done=True,
                ).ask()
                if _branch is None:
                    logger.warning("No branch selected")
                    raise SystemExit(0)
            else:
                _branch = branch

            if _branch == "latest":
                _branch = self.latest_module_tag(mod)

            if _branch == "main":
                yield (mod, "main", mod)
            elif _branch in [*self.module_branches(mod), "main"]:
                yield (mod, _branch, f"{mod}_{_branch}")
            else:
                yield None


def _update_example_config(path: Path):
    examples = []
    schema_paths = []
    for module_path in Path("modules").glob("*"):
        if (module_example := module_path / "config.example.yaml").exists():
            examples.append(module_example.read_text())
        elif (module_schema := module_path / "schema.yaml").exists():
            schema_paths.append(module_schema)

    schema = cfg.Schema.from_file(
        path=[
            CELLOPHANE_ROOT / "schema.base.yaml",
            Path(".") / "schema.yaml",
            *schema_paths,
        ],
    )

    with open(path / "config.example.yaml", "w") as handle:
        handle.write(schema.example_config(extra="\n\n".join(examples)))


@click.group(
    context_settings=dict(
        help_option_names=["-h", "--help"],
        show_default=True,
    ),
)
@click.option(
    "--path",
    type=Path,
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
def main(ctx: click.Context, path: Path, log_level: str):
    """Cellophane

    A library for writing modular wrappers
    """
    ctx.obj["logger"].setLevel(log_level)
    ctx.obj["path"] = path
    ctx.obj["log_level"] = log_level
    ctx.obj["logger"] = logger


@main.group()
@click.option(
    "--repo",
    type=str,
    help="URL to the module repository",
    default="https://github.com/ClinicalGenomicsGBG/cellophane_modules",
)
@click.pass_context
def module(ctx: click.Context, repo: str):
    """Manage modules"""
    ctx.ensure_object(dict)
    ctx.obj["modules_repo"] = ModulesRepo(repo)
    ctx.obj["logger"].debug(f"Using module repository {repo}")
    if Repo(".").is_dirty():
        ctx.obj["logger"].critical(
            "Repository is dirty, please commit or stash changes before continuing"
        )
        raise SystemExit(1)


@module.command()
@click.argument(
    "module_name",
    type=str,
    metavar="MODULE",
    required=False,
)
@click.option(
    "-b",
    "--branch",
    type=str,
    required=False,
)
@click.pass_context
def add(
    ctx: click.Context,
    module_name: str | None,
    branch: str | None,
):
    """Add module

    If "latest" is specified as the branch, the latest tagged release
    will be used for all modules.
    """
    path: Path = ctx.obj["path"]
    logger: logging.LoggerAdapter = ctx.obj["logger"]
    modules_repo: ModulesRepo = ctx.obj["modules_repo"]
    log_level: str = ctx.obj["log_level"]

    try:
        repo = Repo(str(path))
    except InvalidGitRepositoryError:
        logger.critical(f"Path is not a git repository: {path}")
        raise SystemExit(1)

    added = []
    for result in modules_repo.get_module_branch(
        action="add",
        module=module_name,
        branch=branch,
        valid_modules=[*{sm.name for sm in repo.submodules} ^ {*modules_repo.modules}],
    ):
        if result is None:
            logger.error(f"Could not find branch {branch} for module {module_name}")
            continue
        else:
            mod, branch, module_branch = result

            logger.info(f"Adding module {mod} ({branch})")
            try:
                sm = repo.create_submodule(
                    name=mod,
                    path=path / "modules" / mod,
                    url=modules_repo.url,
                    branch=module_branch,
                )
                repo.index.add([sm])
            except Exception as e:
                logger.error(e, exc_info=log_level == "DEBUG")
                continue
            else:
                added.append(mod)

    if added:
        try:
            _update_example_config(path)
            repo.index.add("config.example.yaml")
            repo.index.write()
            repo.index.commit(f"feat(cellophane): Add module(s) {', '.join(added)}")

        except Exception as e:
            logger.error(e, exc_info=log_level == "DEBUG")
            raise SystemExit(1)


@module.command()
@click.argument(
    "module_name",
    type=str,
    metavar="MODULE",
    required=False,
)
@click.option(
    "-b",
    "--branch",
    help="Branch to update to (use 'latest' for latest release)",
    type=str,
    required=False,
)
@click.pass_context
def update(
    ctx: click.Context,
    module_name: str,
    branch: str,
):
    """Update module(s)

    If "all" is specified as the module name, all modules will be updated.
    """
    path: Path = ctx.obj["path"]
    logger: logging.LoggerAdapter = ctx.obj["logger"]
    modules_repo: ModulesRepo = ctx.obj["modules_repo"]
    log_level: str = ctx.obj["log_level"]

    try:
        repo = Repo(str(path))
    except InvalidGitRepositoryError:
        logger.critical(f"Path is not a git repository: {path}")
        raise SystemExit(1)

    local_modules = [sm.name for sm in repo.submodules]
    updated = []
    for result in modules_repo.get_module_branch(
        action="update",
        module=module_name,
        branch=branch,
        valid_modules=local_modules,
    ):
        if result is None:
            logger.error(f"Could not find branch {branch} for module {module_name}")
            continue
        else:
            mod, branch, module_branch = result

            logger.info(f"Updating module {mod} ({branch})")
            try:
                sm_prev = repo.submodule(mod)
                module_url, module_path = sm_prev.url, sm_prev.path
                sm_prev.remove(force=True, module=True)
                sm = repo.create_submodule(
                    name=sm_prev.name,
                    path=module_path,
                    url=module_url,
                    branch=module_branch,
                )
                repo.index.add([sm])
            except Exception as e:
                logger.error(e, exc_info=log_level == "DEBUG")
                continue
            else:
                updated.append(mod)

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
            raise SystemExit(1)


@module.command()
@click.argument(
    "module_name",
    metavar="MODULE",
    type=str,
    required=False,
)
@click.pass_context
def rm(
    ctx: click.Context,
    module_name: str | None,
):
    """Remove module"""

    path: Path = ctx.obj["path"]
    logger: logging.LoggerAdapter = ctx.obj["logger"]

    try:
        repo = Repo(str(path))
    except InvalidGitRepositoryError:
        logger.critical(f"Path is not a git repository: {path}")
        raise SystemExit(1)

    if module_name is None:
        modules = checkbox(
            "Select module(s) to remove",
            choices=[sm.name for sm in repo.submodules],
            erase_when_done=True,
        ).ask()
    else:
        modules = [module_name]

    removed = []
    for mod in modules:
        try:
            sm = repo.submodule(mod)
            logger.info(f"Removing module {mod}")
            sm.remove()
        except Exception as e:
            logger.error(e)
            continue
        else:
            removed.append(mod)

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
            logger.critical(e)
            raise SystemExit(1)


@main.command()
@click.argument(
    "name",
    type=str,
)
@click.pass_context
def init(ctx: click.Context, name: str):
    """Initialize a new cellophane project

    If no path is specified, the current directory will be used.
    If the path is not a git repository, it will be initialized as one.
    """
    path: Path = ctx.obj["path"]
    logger: logging.LoggerAdapter = ctx.obj["logger"]

    _prog_name = re.sub("\\W", "_", name)

    logger.info(f"Initializing new cellophane project at {path}")

    try:
        for subdir in (
            path / "modules",
            path / "scripts",
        ):
            subdir.mkdir(parents=True, exist_ok=False)

        for file in (
            path / "modules" / "__init__.py",
            path / "schema.yaml",
        ):
            file.touch(exist_ok=False)

        for file in [
            path / "__main__.py",
            path / f"{_prog_name}.py",
            path / "config.example.yaml",
        ]:
            if file.exists():
                raise FileExistsError(file)

        with (
            open(CELLOPHANE_ROOT / "template" / "__main__.py", "r") as main_handle,
            open(CELLOPHANE_ROOT / "template" / "entrypoint.py", "r") as entry_handle,
            open(path / f"{_prog_name}.py", "w") as entry_dest_handle,
            open(path / "__main__.py", "w") as main_dest_handle,
        ):
            base = main_handle.read()
            main_dest_handle.write(base.format(label=name, prog_name=_prog_name))
            entry_dest_handle.write(entry_handle.read())

        logger.info(f"Generating example config file at {path / 'config.example.yaml'}")
        _update_example_config(path)

    except FileExistsError as e:
        logger.critical(e)
        raise SystemExit(1)

    try:
        repo = Repo(str(path))
    except InvalidGitRepositoryError:
        logger.info("Initializing git repository")
        repo = Repo.init(str(path))
    finally:
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


if __name__ == "__main__":
    click.rich_click.DEFAULT_STRING = "[{}]"
    logger = logs.get_logger(
        label="cellophane",
        level=logging.INFO,
        queue=logs.get_log_queue(mp.Manager()),
    )
    try:
        obj = {"logger": logger}
        main(obj=obj)
    except Exception as e:
        logger.critical(e, exc_info=True)
        raise SystemExit(1)
