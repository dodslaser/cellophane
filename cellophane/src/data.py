"""Utilities for interacting with SLIMS"""

from collections import UserDict, UserList
from collections.abc import ItemsView, KeysView, ValuesView
from copy import deepcopy
from functools import partial, reduce
from pathlib import Path
from typing import Any, ClassVar, Iterable, Optional, Sequence, TypeVar

from attrs import define, field, fields_dict, has, make_class
from ruamel.yaml import YAML

_YAML = YAML(typ="safe")


@define(slots=False)
class Container(UserDict):
    """Base container class for the Config, Sample, and Samples classes.

    The container supports attribute-style access to its data and allows nested key
    access using Sequence[str] keys.

    Args:
        data (dict | None): The initial data for the container.
            Defaults to an empty dictionary.

    Attributes:
        data (dict): The dictionary that stores the data.

    Methods:
        as_dict: Returns the container's data as a dictionary.
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
        del args, kwargs  # unused
        instance = super().__new__(cls)
        object.__setattr__(instance, "data", {})
        return instance

    # FIXME: This is pretty slow, and should probably be implemented by each subclass
    @property
    def as_dict(self):
        """Dictionary representation of the container.

        The dictionary will have the same nested structure as the container.

        Returns:
            dict: A dictionary representation of the container object.

        Example:
            ```python
            data = Container(
                key_1 = "value_1",
                key_2 = Container(
                    key_3 = "value_3",
                    key_4 = "value_4"
                )
            )
            print(data.as_dict)
            # {
            #     "key_1": "value_1",
            #     "key_2": {
            #         "key_3": "value_3",
            #         "key_4": "value_4"
            #     }
            # }
            ```
        """

        ret = dict(self)
        for k, v in ret.items():
            if isinstance(v, Container):
                ret[k] = v.as_dict
        return ret

    @property
    def _container_attributes(self):
        return [*dir(self), *fields_dict(self.__class__)]

    def keys(self):
        """
        Keys view of the container object.

        Only returns keys on the top level of the container's data.

        Returns:
            KeysView: A view of the keys.
        """
        _fields = {k: None for k in fields_dict(self.__class__) if k != "data"}
        return KeysView({**_fields, **self.data})

    def values(self):
        """
        Values view of the container object.

        Only returns values on the top level of the container's data.

        Returns:
            ValuesView: A view of the values.
        """
        return ValuesView({k: self[k] for k in self.keys()})

    def items(self):
        """
        Items view of the container object.

        Only returns keys-value pairs on the top level of the container's data.

        Returns:
            ItemsView: A view of the keys.
        """
        return ItemsView({k: self[k] for k in self.keys()})

    def __contains__(self, key) -> bool:
        try:
            self[key]  # pylint: disable=pointless-statement]
            return True
        except (KeyError, TypeError):
            return False
            

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
    """
    Output class represents an output file or directory.

    Attributes:
        src (set[Path]): The set of source paths.
        dest_dir (Path): The destination directory.
        parent_id (str | None): The optional parent ID. Defaults to None.

    Methods:
        set_parent_id(value: str): Sets the parent ID to the specified value.
    """

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
        """
        Sets the value of the "parent_id" attribute to the specified value.

        Args:
            value (str): The value to set for the "parent_id" attribute.

        Returns:
            None
        """
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
    """
    Base sample class represents a sample with an ID, a list of files, a flag indicating
    if it's done, and a list of Output objects.
    Can be subclassed in a module to add additional functionality (mixin).

    Attributes:
        id (str): The ID of the sample.
        files (list[str]): The list of files associated with the sample.
        done (bool | None): The flag indicating if the sample is done. Defaults to None.
        output (list[Output]): The list of Output objects associated with the sample.

    Methods:
        with_mixins(mixins): Returns a new Sample class with the specified mixins
            applied.
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
        """
        Returns a new Sample class with the specified mixins as base classes.

        Internally called by Cellophane with the samples mixins specified
        in the loaded modules. Uses attrs.make_class to create a new class,
        so any attrs decorators in the mixins will be applied.

        Args:
            cls (type): The class to apply the mixins to.
            mixins (Iterable[type]): An iterable of mixin classes to apply.

        Returns:
            type: The new class with the mixins applied.
        """
        return _apply_mixins(cls, UserDict, mixins)


S = TypeVar("S", bound=Sample)


@define(slots=False, order=False, init=False)
class Samples(UserList[S]):
    """
    Base samples class represents a list of samples.
    Can be subclassed in a module to add additional functionality (mixin).

    Attributes:
        data (list[Sample]): The list of samples.

    Methods:
        from_file(path: Path): Returns a new Samples object with samples loaded from
            the specified YAML file.
        with_mixins(mixins): Returns a new Samples class with the specified mixins
            applied.
        with_sample_class(sample_class): Returns a new Samples class with the specified
            sample class.

    """

    data: list[S] = field(factory=list)
    sample_class: ClassVar[type[Sample]] = Sample

    def __init__(self, data: list | None = None, /, **kwargs):
        self.__attrs_init__(**kwargs)  # pylint: disable=no-member
        super().__init__(data or [])

    @classmethod
    def from_file(cls, path: Path):
        """Get samples from a YAML file"""
        samples = []
        for sample in _YAML.load(path):
            _id = sample.pop("id")
            samples.append(
                cls.sample_class(id=str(_id), **sample)  # type: ignore[call-arg]
            )
        return cls(samples)

    @classmethod
    def with_mixins(cls, mixins):
        """
        Returns a new Samples class with the specified mixins as base classes.

        Internally called by Cellophane with the samples mixins specified
        in the loaded modules. Uses attrs.make_class to create a new class,
        so any attrs decorators in the mixins will be applied.

        Args:
            cls (type): The class to apply the mixins to.
            mixins (Iterable[type]): An iterable of mixin classes to apply.

        Returns:
            type: The new class with the mixins applied.
        """

        return _apply_mixins(cls, UserList, mixins)

    @classmethod
    def with_sample_class(cls, sample_class):
        """
        Returns a new Samples class with the specified sample class as the
        class to use for samples.

        Internally called by Cellophane with the samples mixins specified
        in the loaded modules.

        Args:
            cls (type): The class to apply the mixins to.
            sample_class (type): The class to use for samples.

        Returns:
            type: The new class with the sample class applied.
        """

        return type(cls.__name__, (cls,), {"sample_class": sample_class})

    def split(self, link_by: Optional[str] = None):
        """
        Splits the data into groups based on the specified attribute value.

        Args:
            link_by (str | None): The attribute to link the samples by.
                Defaults to None, which results in Samples objects with one
                sample each.

        Yields:
            Iterable[Samples]: An iterable of Samples objects.

        Example:
            ```python
            Samples(
                [
                    Sample(id="sample1", files=["file1_1.txt"]),
                    Sample(id="sample1", files=["file1_2.txt"]),
                    Sample(id="sample2", files=["file2.txt"]),
                ]
            )

            # Splitting by the "id" attribute (eg. to merge data from multiple runs)
            for samples in data.split(link_by="id"):
                print(samples)
            # Samples(
            #     Sample(id='sample1', files=['file1_1.txt']),
            #     Sample(id='sample1', files=['file1_2.txt'])
            # )
            # Samples(Sample(id='sample2', files=['file2.txt']))

            # Splitting without linking (eg. to get individual samples)
            for sample in data.split():
                print(sample)
            # Samples(Sample(id='sample1', files=['file1_1.txt']))
            # Samples(Sample(id='sample1', files=['file1_2.txt']))
            # Samples(Sample(id='sample2', files=['file2.txt']))
            ```
        """
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
        """
        Removes invalid samples from the calling Samples object.

        Invalid samples are defined as samples that have:
        - None or missing files
        - None values in the files list
        - Files that do not exist
        - Non-string ID values

        Example:
            ```python
            data = Samples(
                [
                    Sample(id="sample1", files=["file1.txt"]),
                    Sample(id="sample2", files=[None, "file2.txt"]),
                    Sample(id="sample3", files=["file3.txt"]),
                ]
            )

            # Removing invalid samples
            data.remove_invalid()
            print(data)
            # Samples(
            #     Sample(id='sample1', files=['file1.txt']),
            #     Sample(id='sample3', files=['file3.txt'])
            # )
            ```
        """
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
        """
        Returns a set of unique IDs from the samples in the data.

        Returns:
            set[str]: The set of unique IDs.

        Example:
            ```python
            data = [
                Sample(id="sample1", files=["file1.txt"]),
                Sample(id="sample2", files=["file2.txt"]),
                Sample(id="sample1", files=["file3.txt"]),
            ]

            unique_ids = data.unique_ids
            print(unique_ids)  # {"sample1", "sample2"}
            ```
        """
        return {s.id for s in self}

    @property
    def complete(self):
        """
        Get only completed samples from a Samples object.

        Samples are considered as completed if all runners have completed
        successfully, and the sample is marked as done.

        Returns:
            Class: A new instance of the class with only the completed samples.
        """

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
        """
        Get only failed samples from a Samples object.

        Samples are considered as failed if one or more of the runners has not
        completed successfully, or has explicitly marked the sample as not done.

        Returns:
            Class: A new instance of the class with only the failed samples.
        """
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

    def __setstate__(self, state):
        for k, v in state.items():
            self.__setattr__(k, v)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (
            self.__class__,
            (self.data,),
            {k: self.__getattribute__(k) for k in fields_dict(self.__class__)},
        )
