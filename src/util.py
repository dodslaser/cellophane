"""Utility functions and classes"""

from collections import UserDict
from functools import reduce
from typing import Any, Hashable, Mapping, Sequence


def map_nested_keys(data: Any) -> list[list[str]]:
    """Map keys of nested dicts"""
    match data:
        case dict():
            return [
                [k, *p]
                for k, v in data.items()
                for p in map_nested_keys(v)  # pylint: disable=not-an-iterable
            ]
        case _:
            return [[]]


def merge_mappings(m_1: Any, m_2: Any) -> Any:
    """Merge two nested mappings"""
    match m_1, m_2:
        case {**m_1}, {**m_2} if not any(k in m_1 for k in m_2):
            return m_1 | m_2
        case {**m_1}, {**m_2} if m_1:
            return {k: merge_mappings(v, m_2.get(k, v)) for k, v in (m_2 | m_1).items()}
        case _:
            return m_2


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
