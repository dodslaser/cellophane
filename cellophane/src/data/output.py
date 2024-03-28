"""Outut classes for copying files to another directory."""

from glob import glob
from pathlib import Path
from typing import Iterable

from attrs import define, field
from attrs.setters import convert

from .container import Container


@define
class Output:
    """
    Output file to be copied to the another directory.
    """

    src: Path = field(
        kw_only=True,
        converter=Path,
        on_setattr=convert,
    )
    dst: Path = field(
        kw_only=True,
        converter=Path,
        on_setattr=convert,
    )
    checkpoint: str = field(
        default="main",
        kw_only=True,
        converter=str,
        on_setattr=convert,
    )

    optional: bool = field(
        default=False,
        kw_only=True,
        converter=bool,
        on_setattr=convert,
    )

    def __hash__(self) -> int:
        return hash((self.src, self.dst))


@define
class OutputGlob:  # type: ignore[no-untyped-def]
    """
    Output glob find files to be copied to the another directory.
    """

    src: str = field(
        converter=str,
        on_setattr=convert,
    )
    dst_dir: str | None = field(  # type: ignore[var-annotated]
        default=None,
        kw_only=True,
        converter=lambda v: v if v is None else str(v),
        on_setattr=convert,
    )
    dst_name: str | None = field(  # type: ignore[var-annotated]
        default=None,
        kw_only=True,
        converter=lambda v: v if v is None else str(v),
        on_setattr=convert,
    )

    checkpoint: str = field(
        default="main",
        kw_only=True,
        converter=str,
        on_setattr=convert,
    )

    optional: bool = field(
        default=False,
        kw_only=True,
        converter=bool,
        on_setattr=convert,
    )

    def __hash__(self) -> int:
        return hash((self.src, self.dst_dir, self.dst_name))

    def resolve(
        self,
        samples: Iterable,
        workdir: Path,
        config: Container,
    ) -> tuple[set[Output], set[str]]:
        """
        Resolve the glob pattern to a list of files to be copied.

        Args:
            samples (Samples): The samples being processed.
            workdir (Path): The working directory
                with tag and the value of the split_by attribute (if any) appended.
            config (Container): The configuration object.
            logger (LoggerAdapter): The logger.

        Returns:
            set[Output]: The list of files to be copied.

        """
        outputs = set()
        warnings = set()

        for sample in samples:
            meta = {
                "samples": samples,
                "config": config,
                "workdir": workdir,
                "sample": sample,
            }

            match self.src.format(**meta):
                case p if Path(p).is_absolute():
                    pattern = p
                case p if Path(p).is_relative_to(workdir):
                    pattern = p
                case p:
                    pattern = str(workdir / p)

            if not (matches := [Path(m) for m in glob(pattern)]) and not self.optional:
                warnings.add(f"No files matched pattern '{pattern}'")

            for m in matches:
                match self.dst_dir:
                    case str(d) if Path(d).is_absolute():
                        dst_dir = Path(d.format(**meta))
                    case str(d):
                        dst_dir = config.resultdir / d.format(**meta)
                    case _:
                        dst_dir = config.resultdir

                match self.dst_name:
                    case None:
                        dst_name = m.name
                    case _ if len(matches) > 1:
                        warnings.add(
                            f"Destination name {self.dst_name} will be ignored "
                            f"as '{self.src}' matches multiple files"
                        )
                        dst_name = m.name
                    case str() as n:
                        dst_name = n.format(**meta)

                dst = Path(dst_dir) / dst_name

                outputs.add(
                    Output(
                        src=m,
                        dst=dst,
                        optional=self.optional,
                        checkpoint=self.checkpoint.format(**meta),
                    )
                )

        return outputs, warnings
