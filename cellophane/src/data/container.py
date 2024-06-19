"""Base container class for the Config, Sample, and Samples classes."""

from copy import deepcopy
from functools import reduce
from typing import Any, Iterator, Mapping, Sequence

from attrs import define, field, fields_dict

from .. import util


class PreservedDict(dict):
    """Dict subclass to allow dict inside Container"""


@define(init=False, slots=False)
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
        return self.__class__(**util.merge_mappings(self, other))

    def __init__(  # pylint: disable=keyword-arg-before-vararg
        self,
        __data__: dict | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
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

    def __contains__(self, key: str | Sequence[str]) -> bool:  # type: ignore[override]
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

    def __setitem__(self, key: str | Sequence[str], item: Any) -> None:
        if isinstance(item, dict) and not isinstance(item, (Container, PreservedDict)):
            item = Container(item)

        match key:
            case str(k) if k in fields_dict(self.__class__):
                self.__setattr__(k, item)
            case str(k) if k.isidentifier():
                self.__data__[k] = item
            case (*k,) if all(isinstance(k_, str) for k_ in k):

                def _set(d: dict, k: str) -> dict:
                    if k not in d:
                        d[k] = Container()
                    return d[k]

                reduce(_set, k[:-1], self.__data__)[k[-1]] = item
            case k:
                raise TypeError(f"Key {k} is not an string or a sequence of strings")

    def __getitem__(self, key: str | Sequence[str]) -> Any:
        match key:
            case str(k) if k in fields_dict(self.__class__):
                return super().__getattribute__(k)
            case str(k):
                return self.__data__[k]
            case (*k,):
                return reduce(lambda d, k: d[k], k, self.__data__)
            case k:
                raise TypeError(f"Key {k} is not a string or a sequence of strings")

    def __getattr__(self, key: str) -> Any:
        if key in self.__data__:
            return self.__data__[key]

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
