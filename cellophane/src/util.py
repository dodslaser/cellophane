"""Utility functions and classes"""

import importlib.util
import sys
from collections.abc import Hashable
from functools import singledispatch
from types import ModuleType
from typing import Any

# from gelidum.collections import frozendict, frozenlist, frozenzet
from frozendict import frozendict


class frozenlist(tuple):
    """
    A frozen list. Actually a tuple, but with a different name.
    """


@singledispatch
def freeze(data: Any) -> Any:
    """
    Base freeze dispatch, returns the input object.

    Args:
        data (Any): The object to freeze.

    Returns:
        Any: Input data object
    """
    return data


@freeze.register
def _(data: dict) -> frozendict:
    """
    Freezes a dictionary.

    Args:
        data (dict): The dictionary to freeze.

    Returns:
        frozendict: The frozen dictionary.
    """
    return frozendict({k: freeze(v) for k, v in data.items()})


@freeze.register
def _(data: list | frozenlist) -> frozenlist:
    """
    Freezes a list by converting to a tuple.

    Args:
        data (list | tuple): The list or tuple to freeze.

    Returns:
        tuple: The frozen list or tuple.
    """
    return frozenlist(freeze(v) for v in data)


@singledispatch
def unfreeze(data: Any) -> Any:
    """
    Base unfreeze dispatch, returns the input object.

    Args:
        data (Any): The object to unfreeze.

    Returns:
        Any: Input data object
    """
    return data


@unfreeze.register
def _(data: dict | frozendict) -> dict:
    """
    Unfreezes a dictionary.

    Args:
        data (dict | frozendict): The dictionary to unfreeze.

    Returns:
        dict: The unfrozen dictionary.
    """
    return {k: unfreeze(v) for k, v in data.items()}


@unfreeze.register
def _(data: list | frozenlist) -> list:
    """
    Unfreezes a frozenlist.

    Args:
        data (list | frozenlist): The list or tuple to unfreeze.

    Returns:
        list: The unfrozen list.
    """
    return [unfreeze(v) for v in data]


def map_nested_keys(node: Any, path: list[str] | None = None) -> list[list[str]]:
    """
    Maps the keys of a nested mapping.

    Args:
        data (Any): Mapping for which to map the nested keys.

    Returns:
        List[List[str]]: A list of lists of the paths to mapping keys.

    Example:
        ```python
        data = {
            "key1": {
                "key2": "value1",
                "key3": "value2"
            },
            "key4": {
                "key5": "value3"
            }
        }

        map_nested_keys(data)   # [['key1', 'key2'], ['key1', 'key3'], ['key4', 'key5']]
        ```
    """
    if path is None:  # For the root node
        path = []

    if not isinstance(node, dict) or len(node) == 0:
        return [path] if path else []

    paths = []
    for key in node:
        # Add the current key to the path
        new_path = list(path) + [key]
        # Recurse on child nodes and extend paths
        paths.extend(map_nested_keys(node[key], new_path))

    return paths


def merge_mappings(m_1: Any, m_2: Any) -> Any:
    """
    Merges two nested mappings into a single mapping.

    Args:
        m_1 (Any): The first mapping.
        m_2 (Any): The second mapping.

    Returns:
        Any: The merged mapping.

    Example:
        ```python
        m_1 = {"k1": "v1", "k2": ["v2", "v3"]}
        m_2 = {"k2": ["v4", "v5"], "k3": "v6"}
        merge_mappings(m_1, m_2)

        # {
        #     "k1": "v1",
        #     "k2": ["v2", "v3", "v4", "v5"],
        #     "k3": "v6"
        # }
        ```
    """
    match m_1, m_2:
        case {**m_1}, {**m_2} if not any(k in m_1 for k in m_2):
            return m_1 | m_2
        case {**m_1,}, {**m_2,}:
            return {k: merge_mappings(v, m_2.get(k, v)) for k, v in (m_2 | m_1).items()}
        case [{**m_1,},], [{**m_2,},]:
            return [merge_mappings(m_1, m_2)]
        case [*m_1], [*m_2] if all(isinstance(v, Hashable) for v in m_1 + m_2):
            # dict is used to preserve order while removing duplicates
            # FIXME: Is this always the desired behavior?
            return [*dict.fromkeys(m_1 + m_2)]
        case _:
            return m_2


def lazy_import(name: str) -> ModuleType:
    """
    Performs a lazy import of a module. The module is added to `sys.modules`,
    but not loaded until it is accessed.

    Args:
        name (str): The name of the module to import.

    Returns:
        ModuleType: The imported module.

    Raises:
        ModuleNotFoundError: Raised when the module is not found.

    Example:
        ```python
        module = lazy_import("my_module")
        module.my_function()
        ```
    """

    spec = importlib.util.find_spec(name)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"No module named '{name}'")
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
