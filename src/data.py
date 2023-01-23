"""Utilities for interacting with SLIMS"""

from collections import UserDict, UserList
from functools import reduce
from pathlib import Path
from typing import Any, Hashable, Mapping, Optional, Sequence, TypeVar, Callable

from yaml import safe_load


class Container(UserDict):
    """A dict that allows attribute access to its items"""

    def __setitem__(self, key: Hashable | Sequence[Hashable], item: Any) -> None:
        if isinstance(item, Mapping) and not isinstance(item, Container):
            item = Container(item)

        match key:
            case k if isinstance(k, Hashable):
                self.data[k] = item
            case *k,:
                reduce(lambda d, k: d.setdefault(k, Container()), k[:-1], self.data)[
                    k[-1]
                ] = item
            case _:
                raise TypeError("Key must be a string or a sequence of strings")

    def __getitem__(self, key: Hashable | Sequence[Hashable]) -> Any:
        match key:
            case k if isinstance(k, Hashable):
                return self.data[k]
            case *k,:
                return reduce(lambda d, k: d[k], k, self.data)
            case _:
                raise TypeError("Key must be hashble or a sequence of hashables")

    def __getattr__(self, key: str) -> Any:
        if "data" in self.__dict__ and key in self.data:
            return self.data[key]
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            )


class Sample(Container):
    """A basic sample container"""

    id: str
    fastq_paths: list[str]
    backup: Optional[Container]


S = TypeVar("S", bound=Sample)


class Samples(UserList[S]):
    """A list of sample containers"""

    @classmethod
    def from_file(cls, path: Path):
        """Get samples from a YAML file"""
        with open(path, "r", encoding="utf-8") as handle:
            samples = [
                Sample(
                    id=str(_id),
                    fastq_paths=[fastq1, fastq2],
                    backup=None,
                )
                for _id, (fastq1, fastq2) in safe_load(handle).items()
            ]
        return cls(samples)

    def hydra_units_samples(self, *_, location: str = "samples", **kwargs):
        """Write Hydra units and samples files"""
        # Path(location).mkdir(parents=True, exist_ok=True)
        # _units_path = Path(location) / "units.csv"
        # _samples_path = Path(location) / "samples.csv"
        # with (
        #     open(_units_path, "w", encoding="utf-8") as units,
        #     open(_samples_path, "w", encoding="utf-8") as samples,
        # ):
        #     pass

        # FIXME: Implement hydra units and samples files
        raise NotImplementedError

    def nfcore_samplesheet(self, *_, location: str | Path, **kwargs) -> Path:
        """Write a Nextflow samplesheet"""
        Path(location).mkdir(parents=True, exist_ok=True)
        _data = [
            {
                "sample": sample.id,
                "fastq_1": sample.fastq_paths[0],
                "fastq_2": sample.fastq_paths[1],
                **{
                    k: v[sample.id] if isinstance(v, Mapping) else v
                    for k, v in kwargs.items()
                },
            }
            for sample in self
        ]

        _header = ",".join(_data[0].keys())

        _samplesheet = "\n".join([_header, *(",".join(d.values()) for d in _data)])
        _path = Path(location) / "samples.nextflow.csv"
        with open(_path, "w", encoding="utf-8") as handle:
            handle.write(_samplesheet)

        return _path


    def __reduce__(self) -> Callable | tuple:
        return self.__class__, (self.data,)