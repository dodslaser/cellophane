"""Test cellophane.src.util."""

from collections import UserList
from typing import Any, Callable

from pytest import mark, param

from cellophane.src import data, modules, util


class Test_map_nested_keys:
    """Test map_nested_keys."""

    @staticmethod
    @mark.parametrize(
        "data_,expected",
        [
            param(
                {"a": {"b": {"c": 1, "d": 2}, "e": 3}, "f": 4},
                [["a", "b", "c"], ["a", "b", "d"], ["a", "e"], ["f"]],
                id="nested dict",
            ),
            # FIXME: Add more test cases
        ],
    )
    def test_map_nested_keys(data_: dict, expected: list) -> None:
        """Test map_nested_keys."""
        assert util.map_nested_keys(data_) == expected


class Test_merge_mappings:
    """Test merge_mappings."""

    @staticmethod
    @mark.parametrize(
        "m_1,m_2,expected",
        [
            param(
                None,
                {"a": "b"},
                {"a": "b"},
                id="None, dict",
            ),
            param(
                {"a": "b"},
                {"c": "d"},
                {"a": "b", "c": "d"},
                id="dict, dict",
            ),
            param(
                {"a": ["b", "c"]},
                {"a": ["d", "e"]},
                {"a": ["b", "c", "d", "e"]},
                id="list, list",
            ),
            param(
                {"a": {"b": {"c": [1, 3]}}},
                {"a": {"b": {"c": [3, 7]}}},
                {"a": {"b": {"c": [1, 3, 7]}}},
                id="nested dict, nested dict",
            ),
            param(
                {"a": [{"b": 1}]},
                {"a": [{"c": 2}]},
                {"a": [{"b": 1, "c": 2}]},
                id="nested list, nested list",
            ),
        ],
    )
    def test_merge_mappings(m_1: dict, m_2: dict, expected: dict) -> None:
        """Test merge_mappings."""
        assert util.merge_mappings(m_1, m_2) == expected

class Test__instance_or_subclass:
    """Test _is_instance_or_subclass function."""

    class _SampleSub(data.Sample):
        pass

    class _SamplesSub(data.Samples):
        pass

    hook = modules.pre_hook()(lambda: ...)
    runner = modules.runner()(lambda: ...)

    @staticmethod
    @mark.parametrize(
        "obj,cls,expected",
        [
            (_SampleSub, data.Sample, True),
            (_SamplesSub, data.Samples, True),
            (_SamplesSub, UserList, True),
            (_SamplesSub, list, False),
            (hook, modules.Hook, True),
            (hook, Callable, True),
            (hook, str, False),
            (runner, modules.Runner, True),
            (runner, Callable, True),
            (runner, str, False),
        ],
    )
    def test_instance_or_subclass(
        obj: type[_SampleSub] | type[_SamplesSub] | Any | modules.Runner,
        cls: type,
        expected: bool,
    ) -> None:
        """Test _is_instance_or_subclass function."""
        assert util.is_instance_or_subclass(obj, cls) == expected
