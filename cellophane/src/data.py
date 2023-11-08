"""Utilities for interacting with SLIMS"""

from collections import UserList
from collections.abc import Mapping
from copy import deepcopy
from functools import reduce, wraps
from glob import glob
from logging import LoggerAdapter
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    Iterator,
    Literal,
    Sequence,
    TypeVar,
    overload,
)
from uuid import UUID, uuid4

from attrs import define, field, fields_dict, has, make_class
from ruamel.yaml import YAML

from . import util


class _BASE:
    ...


class _Merger:
    _imps: dict[str, Callable]

    def __init__(self) -> None:
        self._impls: dict[str, Callable] = {}

    def register(self, name: str) -> Callable:
        """
        Decorator for registering a new merge implementation for an attribute.

        The decorated function should take the two values to merge as arguments
        and return the merged value.

        This function will be called via reduce, so the first argument will be
        the result of the previous call to the function, or the first value if
        no previous calls have been made.

        Args:
            name (str): The name of attribute to merge.

        Returns:
            Callable: A decorator that registers the decorated function as the
                implementation for the specified name.
        """

        def wrapper(impl: Callable) -> Callable:
            self._impls[name] = impl
            return impl

        return wrapper

    def __call__(self, name: str, this: Any, that: Any) -> Any:
        """
        Reconciles two values based on their attribute name.

        Args:
            name (str): The name of the values (unused).
            this (Any): The first value.
            that (Any): The second value.

        Returns:
            Any: The reconciled value.
        """
        if name in self._impls:
            return self._impls[name](this, that)
        elif this is None or that is None:
            return this or that
        else:
            return (this, that)


@define
class Container(Mapping):
    """Base container class for the Config, Sample, and Samples classes.

    The container supports attribute-style access to its data and allows nested key
    access using Sequence[str] keys.

    Args:
        __data__ (dict | None): The initial data for the container.
            Defaults to an empty dictionary.

    Attributes:
        __data__ (dict): The dictionary that stores the data.
    """

    __data__: dict = field(factory=dict)

    def __or__(self, other: "Container") -> "Container":
        if self.__class__ != other.__class__:
            raise TypeError("Cannot merge containers of different types")
        return self.__class__(util.merge_mappings(self, other))

    def __init__(self, __data__: dict | None = None, *args: Any, **kwargs: Any) -> None:
        _data = __data__ or {}
        for key in [k for k in kwargs if k not in fields_dict(self.__class__)]:
            _data[key] = kwargs.pop(key)
        self.__attrs_init__(*args, **kwargs)
        for k, v in _data.items():
            self[k] = v

    def __new__(cls, *args: Any, **kwargs: Any) -> "Container":
        del args, kwargs  # unused
        instance = super().__new__(cls)
        object.__setattr__(instance, "__data__", {})
        return instance

    def __contains__(self, key: str | Sequence[str]) -> bool:
        try:
            self[key]  # pylint: disable=pointless-statement]
            return True
        except (KeyError, TypeError):
            return False

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in fields_dict(self.__class__):
            self[name] = value
        else:
            super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        if name in fields_dict(self.__class__):
            super().__delattr__(name)
        else:
            del self[name]

    def __setitem__(self, key: str | Sequence[str], item: Any) -> None:
        if isinstance(item, dict) and not isinstance(item, Container):
            item = Container(item)

        match key:
            case str(k) if k in fields_dict(self.__class__):
                self.__setattr__(k, item)
            case str(k) if k.isidentifier():
                self.__data__[k] = item
            case *k, if all(isinstance(k_, str) for k_ in k):

                def _set(d: Container, k: str) -> Container:
                    if k not in d:
                        d[k] = Container()
                    return d[k]

                reduce(_set, k[:-1], self.__data__)[k[-1]] = item
            case k:
                raise TypeError(f"Key {k} is not an string or a sequence of strings")

    def __delitem__(self, key: Any) -> None:
        if key in self.__data__:
            del self.__data__[key]

    def __getitem__(self, key: str | Sequence[str]) -> Any:
        match key:
            case str(k) if k in fields_dict(self.__class__):
                return super().__getattribute__(k)
            case str(k):
                return self.__data__[k]
            case *k,:
                return reduce(lambda d, k: d[k], k, self.__data__)
            case k:
                raise TypeError(f"Key {k} is not a string or a sequence of strings")

    def __getattr__(self, key: str) -> Any:
        if key in self.__data__:
            return self.__data__[key]
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{key}'"
            )

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        _instance = self.__class__(
            **{deepcopy(k): deepcopy(self[k]) for k in fields_dict(self.__class__)}
        )
        _instance.__data__ = deepcopy(self.__data__)
        return _instance

    def __len__(self) -> int:
        return len(self.__data__)

    def __iter__(self) -> Iterator[str]:
        return iter(self.__data__)


@define
class Output:
    """
    Define an output file to be copied to the another directory.
    """

    src: Path = field(kw_only=True, converter=Path)
    dst: Path = field(kw_only=True, converter=Path)

    def __hash__(self):
        return hash((self.src, self.dst))


@overload
def _apply_mixins(
    cls: type["Samples"],
    base: type,
    mixins: Sequence[type["Samples"]],
) -> type["Samples"]:
    ...  # pragma: no cover


@overload
def _apply_mixins(
    cls: type["Sample"],
    base: type,
    mixins: Sequence[type["Sample"]],
) -> type["Sample"]:
    ...  # pragma: no cover


def _apply_mixins(
    cls: type, base: type, mixins: Sequence[type], name: str | None = None
) -> type:
    _name = cls.__name__
    for m in mixins:
        _name += f"_{m.__name__}"
        m.__bases__ = (base,)
        m.__module__ = "__main__"
        if not has(m):
            m = define(m, slots=False)

    _cls = make_class(name or _name, (), (cls, *mixins), slots=False)
    _cls.__module__ = "__main__"

    return _cls


def as_dict(data: Container, exclude: list[str] | None = None) -> dict[str, Any]:
    """Dictionary representation of a container.

    The returned dictionary will have the same nested structure as the container.

    Args:
        exclude (list[str] | None): A list of keys to exclude from the returned
            dictionary. Defaults to None.

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
        print(as_dict(data))
        # {
        #     "key_1": "value_1",
        #     "key_2": {
        #         "key_3": "value_3",
        #         "key_4": "value_4"
        #     }
        # }
        ```
    """
    return dict(
        **{
            k: as_dict(v) if isinstance(v, Container) else v
            for k, v in data.__data__.items()
            if k not in (exclude or [])
        }
    )


@define(slots=False)
class Sample(_BASE):
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
    files: set[str] = field(factory=set, converter=set)
    processed: bool = False
    uuid: UUID = field(repr=False, factory=uuid4, init=False)
    meta: Container = field(default=Container(), converter=Container)
    _fail: str | None = field(default=None, repr=False)
    merge: ClassVar[_Merger] = _Merger()

    def __str__(self) -> str:
        return self.id

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    # def __reduce__(self) -> tuple[Callable[..., "Sample"], tuple[Any, ...]]:
    #     state = {k: self[k] for k in fields_dict(self.__class__)}
    #     builder = partial(self.__class__, state.pop("__data__"), **state)
    #     return (builder, ())

    @merge.register("files")
    @staticmethod
    def _merge_files(this: set[str], that: set[str]) -> set[str]:
        return this | that

    @merge.register("meta")
    @staticmethod
    def _merge_files(this: set[str], that: set[str]) -> set[str]:
        return Container(util.merge_mappings(this, that))

    @merge.register("_fail")
    @staticmethod
    def _merge_fail(this: str | None, that: str | None) -> str | None:
        return f"{this}\n{that}"

    @merge.register("_processed")
    @staticmethod
    def _merge_done(this: bool | None, that: bool | None) -> bool | None:
        return this and that

    def __and__(self, other: "Sample") -> "Sample":
        if self.__class__ != other.__class__:
            raise TypeError("Cannot merge samples of different types")
        elif self.uuid != other.uuid:
            raise ValueError("Cannot merge samples with different UUIDs")

        _sample = deepcopy(self)
        for _field in fields_dict(self.__class__):
            if _field in ["id", "uuid"]:
                continue
            _sample.__setattr__(
                _field,
                self.merge(
                    _field,
                    self.__getattribute__(_field),
                    other.__getattribute__(_field),
                ),
            )
        return _sample

    def fail(self, reason: str) -> None:
        """
        Marks the sample as failed with the specified reason.
        """
        self._fail = reason

    @property
    def failed(self) -> str | Literal[False]:
        """
        Checks if the sample is failed by any runner
        """
        if self._fail:
            return self._fail
        elif not self.processed:
            return "Sample was not processed"
        else:
            return False

    @classmethod
    def with_mixins(cls, mixins: Sequence[type["Sample"]]) -> type["Sample"]:
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
        return _apply_mixins(cls, _BASE, mixins)


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
    merge: ClassVar[_Merger] = _Merger()
    output: set[Output] = field(factory=set, converter=set)

    def __init__(self, data: list | None = None, /, **kwargs: Any) -> None:
        self.__attrs_init__(**kwargs)  # pylint: disable=no-member
        super().__init__(data or [])

    def __getitem__(self, key: int | UUID):
        if isinstance(key, int):
            return super().__getitem__(key)
        elif isinstance(key, UUID) and key in self:
            return next(s for s in self if s.uuid == key)
        else:
            raise KeyError(f"Sample with UUID {key.hex} not found")

    def __setitem__(self, key: int | UUID, value: S) -> None:
        if isinstance(key, int):
            super().__setitem__(key, value)
        elif isinstance(key, UUID) and key in self:
            self[self.index(self[key])] = value
        else:
            self.append(value)

    def __contains__(self, item: S | UUID) -> bool:
        if isinstance(item, UUID):
            return any(s.uuid == item for s in self)
        else:
            return super().__contains__(item)

    @merge.register("data")
    @staticmethod
    def _merge_data(this: list[Sample], that: list[Sample]) -> list[Sample]:
        return [a | next(b for b in that if b.uuid == a.uuid) for a in this]

    @merge.register("output")
    @staticmethod
    def _merge_output(this: set[Output], that: set[Output]) -> set[Output]:
        return this | that

    @classmethod
    def from_file(cls, path: Path) -> "Samples":
        """Get samples from a YAML file"""
        samples = []
        yaml = YAML(typ="safe")
        for sample in yaml.load(path):
            _id = sample.pop("id")
            samples.append(
                cls.sample_class(id=str(_id), **sample)  # type: ignore[call-arg]
            )
        return cls(samples)

    @classmethod
    def with_mixins(cls, mixins: Sequence[type["Samples"]]) -> type["Samples"]:
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
    def with_sample_class(cls, sample_class: type["Sample"]) -> type["Samples"]:
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

    def split(self, link_by: str | None = None) -> Iterable["Samples"]:
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

    @property
    def unique_ids(self) -> set[str]:
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
    def with_files(self):
        """
        Get only samples with existing files from a Samples object.

        Returns:
            Class: A new instance of the class with only the samples with files.
        """
        return self.__class__(
            [
                sample
                for sample in self
                if sample.files and all(Path(f).exists() for f in sample.files)
            ]
        )

    @property
    def complete(self) -> "Samples":
        """
        Get only completed samples from a Samples object.

        Samples are considered as completed if all runners have completed
        successfully, and the sample is marked as done.

        Returns:
            Class: A new instance of the class with only the completed samples.
        """

        return self.__class__(
            [sample for sample in self if not sample.failed], output=self.output
        )

    @property
    def failed(self) -> "Samples":
        """
        Get only failed samples from a Samples object.

        Samples are considered as failed if one or more of the runners has not
        completed successfully, or has explicitly marked the sample as not done.

        Returns:
            Class: A new instance of the class with only the failed samples.
        """
        return self.__class__([sample for sample in self if sample.failed])

    def __str__(self) -> str:
        return "\n".join([str(s) for s in self])

    def __repr__(self) -> str:
        attribs = ",\n".join(f"{k=}" for k in fields_dict(self.__class__))
        samples = ",\n".join([repr(s) for s in self])
        return f"Samples({samples},\n{attribs})"

    def __setstate__(self, state: dict) -> None:
        for k, v in state.items():
            self.__setattr__(k, v)

    def __reduce__(self) -> str | tuple[Any, ...]:
        return (
            self.__class__,
            (self.data,),
            {k: self.__getattribute__(k) for k in fields_dict(self.__class__)},
        )

    def __or__(self, other: "Samples") -> "Samples":
        if self.__class__ != other.__class__:
            raise TypeError("Cannot merge samples of different types")

        _samples = deepcopy(self)
        for sample in other:
            _samples[sample.uuid] = sample
        for key in fields_dict(self.__class__):
            if key not in ("data", "sample_class", "merge"):
                setattr(_samples, key, getattr(other, key))

        return _samples

    def __and__(self, other: "Samples") -> "Samples":
        if self.__class__ != other.__class__:
            raise TypeError("Cannot merge samples of different types")

        _samples = deepcopy(self)
        for _field in fields_dict(self.__class__):
            if _field == "sample_class":
                continue
            setattr(
                _samples,
                _field,
                self.merge(_field, getattr(self, _field), getattr(other, _field)),
            )
        return _samples


def output(
    pattern: str,
    /,
    dest_dir: str | None = None,
    dest_name: str | None = None,
) -> Callable:
    """
    Decorator to mark output files of a runner.

    Files matching the given pattern will be added to the output of the runner.

    Celophane does not handle the copying of the files. Instead, it is expected
    that a post-hook will be used to copy the files to the output directory.

    Args:
        pattern: A glob pattern to match files to be added to the output.
            The pattern will be formatted with the following variables:
            - `samples`: The samples being processed.
            - `sample`: The current sample being processed.
            - `config`: The configuration object.
            - `runner`: The runner being executed.
            - `workdir`: The working directory, with tag
                (and sample ID for individual_samples runenrs)
        dest_dir: The directory to copy the files to. If not specified, the
            directory of the matched file will be used. If the matched file is
        dest_name: The name to copy the files to. If not specified, the name
            of the matched file will be used.
    """

    def wrapper(runner: Callable) -> Callable:
        @wraps(runner)
        def inner(
            *args: Any,
            samples: Samples,
            workdir: Path,
            config: Container,
            logger: LoggerAdapter,
            **kwargs: Any,
        ) -> Samples:
            nonlocal pattern, dest_dir, dest_name
            match runner(
                *args,
                samples=samples,
                workdir=workdir,
                config=config,
                logger=logger,
                **kwargs,
            ):
                case _samples if isinstance(_samples, Samples):
                    samples = _samples
                case _:
                    pass

            _patterns = set()

            for sample in [s for s in samples if not s._fail]:
                _meta = {
                    "samples": samples,
                    "sample": sample,
                    "config": config,
                    "workdir": workdir,
                }
                _pattern = pattern.format(**_meta)
                _pattern = (
                    _pattern
                    if Path(_pattern).is_absolute()
                    or Path(_pattern).is_relative_to(workdir)
                    else str(workdir / _pattern)
                )
                _patterns.add(_pattern)

            for _pattern in _patterns:
                matches = [Path(p) for p in glob(_pattern)]
                if not matches:
                    logger.warning(f"No files matched pattern '{_pattern}'")
                elif len(matches) > 1 and dest_name is not None:
                    dest_name = None
                    logger.warning(
                        "Destination name will be ignored as "
                        f"'{pattern}' matched multiple files"
                    )

                for match in matches:
                    _dest_dir = (
                        config.resultdir / Path(dest_dir.format(**_meta))
                        if dest_dir is not None and not Path(dest_dir).is_absolute()
                        else Path(dest_dir.format(**_meta))
                        if dest_dir is not None
                        else match.parent
                        if match.is_absolute() or not match.is_relative_to(workdir)
                        else config.resultdir
                        / match.relative_to(config.workdir / config.tag).parent
                    )
                    _dest_name = (
                        dest_name.format(**_meta)
                        if dest_name is not None
                        else match.name
                    )
                    samples.output.add(
                        Output(
                            src=match,
                            dst=Path(_dest_dir) / _dest_name,
                        )
                    )

            return samples

        return inner

    return wrapper
