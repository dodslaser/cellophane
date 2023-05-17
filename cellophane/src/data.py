"""Utilities for interacting with SLIMS"""

from attrs import define, field, fields_dict
from collections import UserDict, UserList
from functools import reduce, partial
from copy import deepcopy
from pathlib import Path
from typing import (
    Any,
    Hashable,
    Optional,
    Sequence,
    TypeVar,
    Iterable,
    Callable,
)

from yaml import safe_load


def _use_setitem(setattr_fn: Callable) -> Callable:
    """Use __setitem__ instead of __setattr__ if attribute is not defined on class"""
    def inner(instance, key: Hashable, value: Any) -> None:
        if key in (*fields_dict(instance.__class__), *dir(instance)):
            setattr_fn(key, value)
        elif key == "data":
            setattr_fn(key, value)
        else:
            instance[key] = value

    return inner


@define(slots=False)
class Container(UserDict):
    """Supercharged dict with attribute access

    Similar to dotwiz/Box, but with a few differences:
    - Supports nested attribute access/assignment
    - Uses attrs for initialization (so supports attrs features like validators)

    This is used internally for the cfg.Config and data.Sample classes
    """

    data: dict = field(factory=dict)

    def __init__(self, data: dict | None = None, *args, **kwargs):
        _data = data or {}
        for key in [k for k in kwargs if k not in fields_dict(self.__class__)]:
            _data[key] = kwargs.pop(key)
        self.__attrs_init__(*args, **kwargs)
        for k, v in _data.items():
            self[k] = v

    def __new__(cls, *args, **kwargs):
        instance = super().__new__(cls)
        object.__setattr__(instance, "data", {})
        # Monkey-patch __setattr__ to use __setitem__ if attribute is not defined
        # I wish there was a cleaner way to do this (eg. hooking class creation)
        # Current solution is a bit cursed, but it works
        object.__setattr__(instance, "__setattr__", _use_setitem(cls.__setattr__))
        return instance

    @property
    def _container_attributes(self):
        return [*dir(self), *fields_dict(self.__class__)]

    def __contains__(self, key: Hashable | Sequence[Hashable]) -> bool:
        try:
            self[key]
        except (KeyError, TypeError):
            return False
        else:
            return True

    def __setitem__(self, key: Hashable | Sequence[Hashable], item: Any) -> None:
        if isinstance(item, dict) and not isinstance(item, Container):
            item = Container(item)

        match key:
            case str(k) if k in self._container_attributes:
                self.__setattr__(k, item)
            case *k,:
                reduce(lambda d, k: d.setdefault(k, Container()), k[:-1], self.data)[
                    k[-1]
                ] = item
            case k if isinstance(k, Hashable):
                self.data[k] = item
            case _:
                raise TypeError("Key must be a string or a sequence of strings")

    def __getitem__(self, key: Hashable | Sequence[Hashable]) -> Any:
        match key:
            case str(k) if k in self._container_attributes:
                return super().__getattribute__(k)
            case *k,:
                return reduce(lambda d, k: d[k], k, self.data)
            case k if isinstance(k, Hashable):
                return self.data[k]
            case k:
                raise TypeError("Key {k} is not hashble or a sequence of hashables")

    def __getattr__(self, key: str) -> Any:
        if key in self._container_attributes:
            super().__getattribute__(key)
        elif key in self.data:
            return self.data[key]
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            )

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        _instance = self.__class__(
            **{deepcopy(k): deepcopy(self[k]) for k in fields_dict(self.__class__)}
        )
        _instance.data = deepcopy(self.data)
        return _instance


@define(frozen=True, slots=False)
class Output:
    """Output dataclass for samples."""

    src: set[Path]
    dest_dir: Path
    parent_id: str | None = None

    def __attrs_post_init__(self):
        object.__setattr__(self, "dest_dir", Path(self.dest_dir))
        if not isinstance(self.src, Iterable):
            object.__setattr__(self, "src", set([Path(self.src)]))
        else:
            object.__setattr__(self, "src", set([Path(s) for s in self.src]))

    def set_parent_id(self, value: str):
        object.__setattr__(self, "parent_id", value)

    def __hash__(self):
        return hash((*self.src, self.dest_dir, self.parent_id))

    def __len__(self):
        return len(self.src)


@define(slots=False, init=False)
class Sample(Container):
    """Base sample container

    Uses data.Container under the hood.

    Can be extended by subclassing in a module (subclass will be added to data.Sample
    as a base class). @attrs.define decorator is not necessary for subclasses but may
    help with type checking.
    """

    id: str = field(kw_only=True)
    files: list[str] = field(factory=list)
    done: bool | None = None
    output: list[Output] = field(factory=list)

    def __str__(self):
        return self.id

    def __reduce__(self):
        state = {k: self[k] for k in fields_dict(self.__class__)}
        builder = partial(self.__class__, state.pop("data"), **state)
        return (builder, ())


S = TypeVar("S", bound=Sample)


@define(slots=False, order=False, init=False, getstate_setstate=True)
class Samples(UserList[S]):
    """A list of sample containers

    Can be extended by subclassing in a module in the same way as data.Sample. Uses
    attrs under the hood, so the final object will support all attrs features.
    """

    data: list[S] = field(factory=list)
    sample_class: type[Sample] = Sample

    def __init__(self, data: list | None = None, /, **kwargs):
        self.__attrs_init__(**kwargs)
        super().__init__(data or [])

    @classmethod
    def from_file(cls, path: Path):
        """Get samples from a YAML file"""
        with open(path, "r", encoding="utf-8") as handle:
            samples = []
            for sample in safe_load(handle):
                id = sample.pop("id")
                samples.append(
                    cls.sample_class(id=str(id), **sample)  # type: ignore[call-arg]
                )
        return cls(samples)

    def split(self, link_by: Optional[str]):
        if link_by is not None:
            linked = {
                sample[link_by]: [li for li in self if li[link_by] == sample[link_by]]
                for sample in self
            }
            for li in linked.values():
                yield self.__class__(li)
        else:
            for sample in self:
                yield self.__class__([sample])

    def validate(self):
        for sample in self:
            if (
                sample.files is None
                or None in sample.files
                or not isinstance(sample.id, str)
            ):
                yield sample
        self.data = [
            sample
            for sample in self
            if sample.files is not None and None not in sample.files
        ]

    @property
    def unique_ids(self):
        return set(s.id for s in self)

    @property
    def complete(self):
        return self.__class__(
            [
                sample
                for samples_by_id in self.split(link_by="id")
                if all(sample.done for sample in samples_by_id)
                for sample in samples_by_id
            ]
        )

    @property
    def failed(self):
        return self.__class__(
            [
                sample
                for samples_by_id in self.split(link_by="id")
                if not all(sample.done for sample in samples_by_id)
                for sample in samples_by_id
            ]
        )

    def __str__(self):
        return "\n".join([str(s) for s in self])
