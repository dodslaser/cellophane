"""Utility functions and classes"""

import importlib.util
import sys
from types import ModuleType
from typing import Any, Hashable


def map_nested_keys(data: Any) -> list[list[str]]:
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

    match data:
        case dict():
            return [[k, *p] for k, v in data.items() for p in map_nested_keys(v)]
        case _:
            return [[]]


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
        case {**m_1}, {**m_2} if m_1:
            return {k: merge_mappings(v, m_2.get(k, v)) for k, v in (m_2 | m_1).items()}
        case [dict(m_1)], [dict(m_2)]:
            return [merge_mappings(m_1, m_2)]
        case [*m_1], [*m_2] if all(isinstance(v, Hashable) for v in m_1 + m_2):
            # dict is used to preserve order while removing duplicates
            # FIXME: Is this always the desired behavior?
            return [*dict.fromkeys(m_1 + m_2)]
        case _:
            return m_2


def lazy_import(name: str):
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
