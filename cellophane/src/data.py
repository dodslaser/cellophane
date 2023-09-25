"""Utilities for interacting with SLIMS"""

from attrs import define, field, fields_dict, has, make_class
from collections import UserDict, UserList
from collections.abc import KeysView, ValuesView, ItemsView
from functools import reduce, partial
from copy import deepcopy
from pathlib import Path
from typing import (
    Any,
    Optional,
    Sequence,
    TypeVar,
    Iterable,
)

from ruamel.yaml import YAML

_YAML = YAML(typ="safe")


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
        return instance

    # FIXME: This is pretty slow, and should probably be implemented by each subclass
    @property
    def as_dict(self):
        ret = dict(self)
        for k, v in ret.items():
            if isinstance(v, Container):
                ret[k] = v.as_dict
        return ret

    @property
    def _container_attributes(self):
        return [*dir(self), *fields_dict(self.__class__)]

    def keys(self):
        _fields = {k: None for k in fields_dict(self.__class__) if k != "data"}
        return KeysView({**_fields, **self.data})

    def values(self):
        return ValuesView({k: self[k] for k in self.keys()})

    def items(self):
        return ItemsView({k: self[k] for k in self.keys()})

    def __contains__(self, key) -> bool:
        try:
            self[key]
        except (KeyError, TypeError):
            return False
        else:
            return True

    def __setattr__(self, name, value) -> None:
        if name not in self._container_attributes:
            self[name] = value
        else:
            super().__setattr__(name, value)

    def __setitem__(self, key: str | Sequence[str], item: Any) -> None:
        if isinstance(item, dict) and not isinstance(item, Container):
            item = Container(item)

        match key:
            case k if k in self._container_attributes:
                self.__setattr__(k, item)
            case str(k) if k.isidentifier():
                self.data[k] = item
            case *k, if all(isinstance(k_, str) for k_ in k):
                reduce(lambda d, k: d.setdefault(k, Container()), k[:-1], self.data)[
                    k[-1]
                ] = item
            case _:
                raise TypeError(f"Key {k} is not an string or a sequence of strings")

    def __getitem__(self, key: str | Sequence[str]) -> Any:
        match key:
            case str(k) if k in self._container_attributes:
                return super().__getattribute__(k)
            case str(k):
                return self.data[k]
            case *k, :
                return reduce(lambda d, k: d[k], k, self.data)
            case k:
                raise TypeError(f"Key {k} is not a string or a sequence of strings")

    def __getattr__(self, key: str) -> Any:
        if key in self.data:
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
            object.__setattr__(self, "src", {Path(self.src)})
        else:
            object.__setattr__(self, "src", {Path(s) for s in self.src})

    def set_parent_id(self, value: str):
        object.__setattr__(self, "parent_id", value)

    def __hash__(self):
        return hash((*self.src, self.dest_dir, self.parent_id))

    def __len__(self):
        return len(self.src)


def _apply_mixins(cls, base, mixins):
    for m in mixins:
        m.__bases__ = (base,)
        if not has(m):
            define(m, init=False, slots=False)

    return make_class(cls.__name__, (), (*mixins, cls), init=False, slots=False)


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

    @classmethod
    def with_mixins(cls, mixins):
        return _apply_mixins(cls, UserDict, mixins)


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
        samples = []
        for sample in _YAML.load(path):
            _id = sample.pop("id")
            samples.append(
                cls().sample_class(id=str(_id), **sample)  # type: ignore[call-arg]
            )
        return cls(samples)

    @classmethod
    def with_mixins(cls, mixins):
        return _apply_mixins(cls, UserList, mixins)

    def split(self, link_by: Optional[str] = None):
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

    def remove_invalid(self):
        self.data = [
            sample
            for sample in self
            if sample.files is not None
            and None not in sample.files
            and all(Path(f).exists() for f in sample.files)
            and isinstance(sample.id, str)
        ]

    @property
    def unique_ids(self):
        return {s.id for s in self}

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
