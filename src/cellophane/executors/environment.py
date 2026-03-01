from __future__ import annotations

import asyncio as aio
from hashlib import sha256
from logging import LoggerAdapter
from multiprocessing.context import SpawnProcess
from multiprocessing.synchronize import Lock as LockType
from pathlib import Path
from typing import TYPE_CHECKING

from rattler import VirtualPackage, install, solve

if TYPE_CHECKING:
    from rattler.platform import PlatformLiteral


class _SolverProcess(SpawnProcess):
    def __init__(self, specs: list[str], sources: list[str], path: Path, platform: PlatformLiteral | None = None) -> None:
        super().__init__()
        self.specs = specs
        self.sources = sources
        self.path = path
        self.platform = platform

    def run(self) -> None:
        aio.run(self._solve_and_build())

    async def _solve_and_build(self) -> None:
        _platforms = [self.platform, "noarch"] if self.platform else None
        solved = await solve(
            specs=self.specs,
            sources=self.sources,
            virtual_packages=VirtualPackage.detect(),
            platforms=_platforms,
        )
        await install(
            records=solved,
            target_prefix=self.path,
            show_progress=False,
        )


def build_environment(
    conda_spec: dict,
    path: Path,
    environment_lock: LockType,
    logger: LoggerAdapter,
    platform: PlatformLiteral | None = None,
) -> Path:
    """Builds a conda environment according to the given specifications.

    Args:
    conda_spec (dict): A dictionary containing the conda specifications, with keys "dependencies"
        (list of conda specs) and "channels" (list of conda channels).

    """
    _spec = conda_spec or {}
    _deps = _spec.get("dependencies", [])
    _channels = _spec.get("channels", ["conda-forge", "bioconda"])

    for key in _spec:
        if key not in {"dependencies", "channels"}:
            logger.warning(f"Unsupported key in conda spec: {key!r}")

    for dependency in _deps:
        if isinstance(dependency, dict) and "pip" in dependency:
            logger.warning("Pip dependencies are not supported.")
            _deps.remove(dependency)
        elif not isinstance(dependency, str):
            logger.warning(f"Unsupported dependency format: {dependency!r}")
            _deps.remove(dependency)

    _spec_hash = sha256()
    _spec_hash.update(str(sorted(_deps)).encode())
    _spec_hash.update(str(_channels).encode())
    _path = path / f"conda_{_spec_hash.hexdigest()[:8]}"

    with environment_lock:
        if _path.exists():
            logger.debug(f"Environment already exists at {_path}, skipping build.")
        else:
            # Run the solver in a spawned process to avoid inheriting the parent asyncio event loop, as this can cause deadlocks.
            logger.debug(f"Building environment at {_path} with dependencies {_deps} and channels {_channels}.")
            solver = _SolverProcess(specs=_deps, sources=_channels, path=_path, platform=platform)
            solver.start()
            solver.join()
            logger.debug(f"Built environment at {_path}")

    return _path
