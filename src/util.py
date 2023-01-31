"""Utility functions and classes"""

from typing import Any, Iterable
import importlib.util
import sys


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
        case [*m_1], [*m_2]:
            return [*{*m_1, *m_2}]
        case _:
            return m_2


def lazy_import(name: str):
    """Lazy import a module"""
    spec = importlib.util.find_spec(name)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"No module named '{name}'")
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module
