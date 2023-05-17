from pathlib import Path
import multiprocessing as mp
import rich_click as click
from typing import Optional
from logging import LoggerAdapter

import re

from . import logs

_ROOT = Path(__file__).parent


@click.group()
def main(**_):
    pass


@main.command()
@click.argument(
    "name",
    type=str,
)
@click.option(
    "--path",
    type=click.Path(exists=False),
    help="Path to the new module",
    required=False,
    default=None,
)
@logs.handle_logging(
    label="cellophane",
    queue=logs.get_log_queue(mp.Manager()),
    propagate_exceptions=False,
)
def init(name: str, path: Optional[click.Path], logger: LoggerAdapter):
    """Initialize a new cellophane project"""
    _path = Path(str(path)) if path else Path.cwd() / name
    _prog_name = re.sub('\\W', '_', name)

    logger.info(f"Initializing new cellophane project at {_path}")

    for subdir in (
        _path / "modules",
        _path / "scripts",
    ):
        subdir.mkdir(parents=True, exist_ok=False)

    for file in (
        _path / "modules" / "__init__.py",
        _path / "schema.yaml",
    ):
        file.touch(exist_ok=False)

    if not (dest_file := _path / "__main__.py").exists():
        with (
            open(_ROOT / "template" / "__main__.py", "r") as base_handle,
            open(_ROOT / "template" / "entrypoint.py", "r") as entrypoint_handle,
            open(_path / f"{_prog_name}.py", "w") as entrypoint_dest_handle,
            open(dest_file, "w") as base_dest_handle
        ):
            base = base_handle.read()
            base_dest_handle.write(base.format(label=name, prog_name=_prog_name))
            entrypoint_dest_handle.write(entrypoint_handle.read())
    else:
        logger.critical(f"File {dest_file} already exists")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
