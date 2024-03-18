"""Utilities to freeze/unfreeze data structures."""

from functools import singledispatch
from typing import Any

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
