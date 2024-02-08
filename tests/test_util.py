"""Test cellophane.src.util."""

import sys

from pytest import mark, param, raises

from cellophane.src import util


class Test_map_nested_keys:
    """Test map_nested_keys."""

    @staticmethod
    @mark.parametrize(
        "data,expected",
        [
            param(
                {"a": {"b": {"c": 1, "d": 2}, "e": 3}, "f": 4},
                [["a", "b", "c"], ["a", "b", "d"], ["a", "e"], ["f"]],
                id="nested dict",
            ),
            # FIXME: Add more test cases
        ],
    )
    def test_map_nested_keys(data: dict, expected: list) -> None:
        """Test map_nested_keys."""
        assert util.map_nested_keys(data) == expected


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


class Test_lazy_import:
    """Test lazy_import."""

    @staticmethod
    def test_lazy_import() -> None:
        """Test lazy_import."""
        assert raises(ImportError, util.lazy_import, ".INVALID")
        assert raises(ModuleNotFoundError, util.lazy_import, "INVALID")

        assert "lib.util.DUMMY" not in sys.modules
        dummy = util.lazy_import("lib.util.DUMMY")
        assert "lib.util.DUMMY" in sys.modules
        assert dummy.DUMMY_VALUE == 42
