"""Utilities for interacting with SLIMS"""

from collections import UserDict, UserList
from functools import reduce
from pathlib import Path
from typing import Any, Callable, Hashable, Mapping, Optional, Sequence, TypeVar, get_args, get_origin

from yaml import safe_load


class Container(UserDict):
    """A dict that allows attribute access to its items"""

    def __contains__(self, key: Hashable | Sequence[Hashable]) -> bool:
        try:
            self[key]
        except (KeyError, TypeError):
            return False
        else:
            return True

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
            case *k,:
                return reduce(lambda d, k: d[k], k, self.data)
            case k if isinstance(k, Hashable):
                return self.data[k]
            case k:
                raise TypeError("Key {k} is not hashble or a sequence of hashables")

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
    complete: Optional[bool] = None
    runner: Optional[str] = None

    def __init__(self, /, id, fastq_paths=[None, None], **kwargs):
        super().__init__(id=id, fastq_paths=fastq_paths, **kwargs)
    
    def add_mixin(self, mixin):
        self.__class__ = type(
            self.__class__.__name__,
            (self.__class__, mixin),
            {}
        )


S = TypeVar("S", bound=Sample)
class Samples(UserList[S]):
    """A list of sample containers"""

    @classmethod
    def from_file(cls, path: Path):
        """Get samples from a YAML file"""
        with open(path, "r", encoding="utf-8") as handle:
            samples = []
            for sample in safe_load(handle):
                id = sample.pop("id")
                samples.append(Sample(id=id, **sample))
        return cls(samples)

    def add_mixin(self, mixin: type):
        mixin_origin = get_origin(mixin) or mixin
        (mixin_arg, *_) = get_args(mixin) or (None,)
        self.__class__ = type(
            self.__class__.__name__,
            (self.__class__, mixin_origin),
            {}
        )
        if mixin_arg:
            for s in self:
                s.add_mixin(mixin_arg)

    def __reduce__(self) -> Callable | tuple:
        return self.__class__, (self.data,)
