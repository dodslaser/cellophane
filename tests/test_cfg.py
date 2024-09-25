"""Test cellophane.cfg."""

# pylint: disable=protected-access

import sys
from functools import reduce
from pathlib import Path
from typing import Any, Callable, Literal

import rich_click as click
from cellophane import cfg, data
from cellophane.cfg.click_ import (
    FormattedString,
    ParsedSize,
    StringMapping,
    TypedArray,
)
from click.testing import CliRunner
from pytest import fail, mark, param, raises
from ruamel.yaml import YAML

_YAML = YAML(typ="unsafe")
LIB = Path("__file__").parent / "tests" / "lib"


class Test_StringMapping:
    """Test StringMapping."""

    @staticmethod
    @mark.parametrize(
        "value,expected",
        [
            param(
                "a=b,c=d",
                {"a": "b", "c": "d"},
                id="simple",
            ),
            param(
                'a="a",c="d"',
                {"a": "a", "c": "d"},
                id="quoted",
            ),
            param(
                "a='a',c='d'",
                {"a": "a", "c": "d"},
                id="single quoted",
            ),
            param(
                "a.b.c=d",
                {"a": {"b": {"c": "d"}}},
                id="nested",
            ),
            param(
                "",
                {},
                id="empty",
            ),
        ],
    )
    def test_convert(value: str, expected: dict) -> None:
        """Test StringMapping.convert."""
        _mapping = cfg.click_.StringMapping()
        assert _mapping.convert(value, None, None) == expected

    @staticmethod
    @mark.parametrize(
        "value",
        [
            param(
                "INVALID",
                id="invalid string",
            ),
            param(
                "a=b,c=,d=e",
                id="missing value",
            ),
            param(
                "a=b,=d,d=e",
                id="missing key",
            ),
            param(
                "a=b,c=d,e",
                id="missing separator",
            ),
            param(
                "a=b,!c=d",
                id="invalid key",
            ),
        ],
    )
    def test_convert_exception(value: str) -> None:
        """Test StringMapping.convert exceptions."""
        _mapping = cfg.click_.StringMapping()
        with raises(click.BadParameter):
            _mapping.convert(value, None, None)


class Test_TypedArray:
    """Test TypedArray."""

    @staticmethod
    @mark.parametrize(
        "value,expected,item_type",
        [
            param(
                ["1", "3", "3", "7"],
                [1.0, 3.0, 3.0, 7.0],
                "number",
                id="float",
            ),
        ],
    )
    def test_convert(
        value: list,
        item_type: Literal["number"],
        expected: list[int],
    ) -> None:
        """Test TypedArray.convert."""
        _array = cfg.click_.TypedArray(item_type)
        assert _array.convert(value, None, None) == expected  # type: ignore[arg-type]

    @staticmethod
    @mark.parametrize(
        "value,item_type,exception",
        [
            param(
                ["DUMMY"],
                "INVALID",
                ValueError,
                id="invalid type",
            ),
            param(
                ["INVALID"],
                "number",
                click.BadParameter,
                id="invalid value",
            ),
        ],
    )
    def test_convert_exception(
        value: list,
        item_type: Literal["INVALID", "number"],
        exception: type[Exception],
    ) -> None:
        """Test TypedArray.convert exceptions."""
        with raises(exception):
            _array = cfg.click_.TypedArray(item_type)  # type: ignore[arg-type]
            _array.convert(value, None, None)  # type: ignore[arg-type]


class Test_ParsedSize:
    """Test ParsedSize."""

    @staticmethod
    @mark.parametrize(
        "value,expected",
        [
            param(
                1337,
                1337,
                id="int",
            ),
            param(
                "1337",
                1337,
                id="str",
            ),
            param(
                "1337B",
                1337,
                id="str_B",
            ),
            param(
                "1337K",
                1337 * 1000,
                id="str_K",
            ),
            param(
                "1337KB",
                1337 * 1000,
                id="str_KB",
            ),
            param(
                "1337KiB",
                1337 * 1024,
                id="str_KiB",
            ),
        ],
    )
    def test_convert(
        value: str | int,
        expected: list[int],
    ) -> None:
        """Test TypedArray.convert."""
        _array = cfg.click_.ParsedSize()
        assert _array.convert(value, None, None) == expected

    @staticmethod
    @mark.parametrize(
        "value,exception",
        [
            param(
                "INVALID",
                click.BadParameter,
                id="invalid value",
            ),
        ],
    )
    def test_convert_exception(
        value: str | int,
        exception: type[Exception],
    ) -> None:
        """Test TypedArray.convert exceptions."""
        with raises(exception):
            _array = cfg.click_.ParsedSize()
            _array.convert(value, None, None)


class Test_Flag:
    """Test cfg._click.Flag."""

    @staticmethod
    @mark.parametrize(
        "flag,click_option",
        [
            param(
                cfg.Flag(
                    required=True,
                    key=("a", "b"),
                    type="string",
                ),
                click.option("--a_b", type=FormattedString(), required=True),
                id="required",
            ),
            param(
                cfg.Flag(
                    key=("a", "b"),
                    type="string",
                    default="default",
                ),
                click.option(
                    "--a_b",
                    type=FormattedString(),
                    default="default",
                ),
                id="default",
            ),
            param(
                cfg.Flag(
                    key=("a", "b"),
                    type="string",
                    secret=True,
                ),
                click.option(
                    "--a_b",
                    type=FormattedString(),
                    show_default=False,
                ),
                id="secret",
            ),
            param(
                cfg.Flag(
                    key=("a", "b"),
                    type="boolean",
                ),
                click.option(
                    "--a_b/--a_no_b",
                    type=bool,
                    default=True,
                ),
                id="boolean",
            ),
            param(
                cfg.Flag(
                    key=("a", "b"),
                    type="string",
                    enum=["A", "B", "C"],
                ),
                click.option(
                    "--a_b",
                    type=click.Choice(["A", "B", "C"], case_sensitive=False),
                ),
                id="enum",
            ),
            *(
                param(
                    cfg.Flag(
                        key=("a", "b"),  # type: ignore[arg-type]
                        type=type_,  # type: ignore[arg-type]
                        **kwargs,
                    ),
                    click.option(
                        "--a_b",
                        type=pytype,
                    ),
                    id=type_,
                )
                for type_, pytype, kwargs in [
                    ("string", FormattedString(), {}),
                    ("integer", int, {}),
                    ("integer", click.IntRange(min=0), {"min": 0}),
                    ("number", float, {}),
                    ("number", click.FloatRange(min=0), {"min": 0}),
                    ("array", TypedArray("string"), {}),
                    ("mapping", StringMapping(), {}),
                    ("path", click.Path(), {}),
                    ("size", ParsedSize(), {}),
                    (None, FormattedString(), {}),
                ]
            ),
        ],
    )
    def test_flag(
        flag: cfg.Flag,
        click_option: Callable[[Callable], click.Parameter],
    ) -> None:
        """Test cfg._click.Flag."""
        _click_info = (
            click_option(lambda: ...)
            .__click_params__[0]  # type: ignore[attr-defined]
            .to_info_dict()
        )
        _flag_info = (
            flag.click_option(lambda: ...)
            .__click_params__[0]  # type: ignore[attr-defined]
            .to_info_dict()
        )

        assert _click_info == _flag_info


class Test_Schema:
    """Test cfg.Schema."""

    @staticmethod
    @mark.parametrize(
        "schema,expected",
        [
            param(
                LIB / "schema" / "parse" / "nested.yaml",
                {
                    "type": "object",
                    "properties": {
                        "a": {"type": "object", "properties": {"b": {"type": "string"}}},
                    },
                },
                id="nested",
            ),
            param(
                [
                    LIB / "schema" / "parse" / "merge_a.yaml",
                    LIB / "schema" / "parse" / "merge_b.yaml",
                    LIB / "schema" / "parse" / "merge_c.yaml",
                    LIB / "schema" / "parse" / "merge_d.yaml",
                ],
                {
                    "properties": {
                        "a": {
                            "properties": {
                                "b": {"type": "string", "default": "MERGE_B"},
                                "c": {"type": "string", "default": "MERGE_C"},
                            },
                        },
                        "d": {"type": "string", "default": "MERGE_D"},
                    },
                },
                id="merge",
            ),
        ],
    )
    def test_from_file(schema: Path | list[Path], expected: dict) -> None:
        """Test cfg.Schema.from_file."""
        _schema = cfg.Schema.from_file(schema)
        assert data.as_dict(_schema) == expected

    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(LIB / "schema" / "gen" / "basic.yaml", id="basic"),
            param(LIB / "schema" / "gen" / "no_default.yaml", id="no_default"),
            param(LIB / "schema" / "gen" / "array.yaml", id="array"),
            param(LIB / "schema" / "gen" / "mapping.yaml", id="mapping"),
            param(LIB / "schema" / "gen" / "nested.yaml", id="nested"),
            param(LIB / "schema" / "gen" / "required.yaml", id="required"),
            param(LIB / "schema" / "gen" / "multiline.yaml", id="multiline"),
        ],
    )
    def test_example_config(definition: Path) -> None:
        """Test cfg.Schema.example_config."""
        _definition = _YAML.load(definition.read_text())
        _schema = cfg.Schema(_definition["schema"])
        assert _schema.example_config == _definition["example"]


class Test__get_flags:
    """Test cfg._get_flags."""

    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(path, id=path.stem)
            for path in (LIB / "schema" / "flags").glob("*.yaml")
        ],
    )
    def test__get_flags(definition: Path) -> None:
        """Test cfg._get_flags."""
        _definition = _YAML.load(definition.read_text())
        _schema = cfg.Schema(_definition["schema"])
        _config = _definition.get("config", {})
        if flags := _definition.get("flags"):
            assert cfg.get_flags(_schema, _config) == [
                cfg.Flag(**flag) for flag in flags
            ]

        if flags_noconfig := _definition.get("flags_noconfig"):
            assert cfg.get_flags(_schema, {}) == [
                cfg.Flag(**flag) for flag in flags_noconfig
            ]

        if flags_base := _definition.get("flags_base"):
            assert cfg.get_flags(_schema) == [cfg.Flag(**flag) for flag in flags_base]


class Test_Config:
    """Test cfg.Config."""

    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(LIB / "schema" / "config" / "from_data.yaml", id="from_data"),
            param(LIB / "schema" / "config" / "from_cli.yaml", id="from_cli"),
            param(LIB / "schema" / "config" / "from_kwargs.yaml", id="from_kwargs"),
        ],
    )
    def test_config(definition: Path) -> None:
        """Test cfg.Config."""
        _definition = _YAML.load(definition.read_text())
        _schema: cfg.Schema = cfg.Schema(_definition["schema"])
        _config = _definition["config"]

        if _data := _definition.get("data"):
            assert _config == data.as_dict(cfg.Config(_schema, _data=_data))

        if _kwargs := _definition.get("kwargs"):
            assert _config == data.as_dict(cfg.Config(_schema, **_kwargs))

        if _cli := _definition.get("cli"):

            @click.command()
            def _cli(**kwargs: Any) -> None:
                kwargs.pop("config_file")
                _config = cfg.Config(_schema, **kwargs)
                _YAML.dump(data.as_dict(_config), sys.stdout)

            _cli = reduce(lambda x, y: y.click_option(x), cfg.get_flags(_schema), _cli)
            runner = CliRunner()

            result = runner.invoke(_cli, _definition["cli"])

            try:
                result_parsed = _YAML.load(result.stdout)
            except Exception:  # pylint: disable=broad-except
                fail(msg=result.stdout)
            else:
                assert result_parsed == _config, result.output

    @mark.parametrize(
        "kwargs,expected",
        [
            param(
                {"a": "CONFIG"},
                cfg.Flag(key=("a",), type="string", default="SCHEMA", value="CONFIG"),
                id="from_config",
            ),
            param(
                {},
                cfg.Flag(key=("a",), type="string", default="SCHEMA", value="SCHEMA"),
                id="from_schema",
            ),
            param(
                {"include_defaults": False},
                cfg.Flag(key=("a",), type="string", default="SCHEMA", value=None),
                id="no_include_defaults",
            ),
        ],
    )
    def test_flags(self, kwargs: dict, expected: cfg.Flag) -> None:
        """Test cfg.Config.flags."""
        _definition = _YAML.load(
            (LIB / "schema" / "flags" / "default.yaml").read_text(),
        )
        _schema = cfg.Schema(_definition["schema"])
        _config = cfg.Config(_schema, allow_empty=True, **kwargs)

        assert cfg.get_flags(_schema, data.as_dict(_config)) == [expected]
