import sys
from functools import reduce
from pathlib import Path

import rich_click as click
from click.testing import CliRunner
from pytest import mark, param, raises
from ruamel.yaml import YAML

from cellophane.src import cfg, data

_YAML = YAML(typ="unsafe")
LIB = Path("__file__").parent / "tests" / "lib"


class Test_StringMapping:
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
                "",
                {},
                id="empty",
            ),
        ],
    )
    def test_convert(value, expected):
        _mapping = cfg._click.StringMapping()
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
    def test_convert_exception(value):
        _mapping = cfg._click.StringMapping()
        with raises(click.BadParameter):
            _mapping.convert(value, None, None)


class Test__Flag:
    @staticmethod
    @mark.parametrize(
        "flag,click_option",
        [
            param(
                cfg._click.Flag(
                    required=True,
                    key=("a", "b"),
                    type="string",
                ),
                click.option("--a_b", type=str, required=True),
                id="required",
            ),
            param(
                cfg._click.Flag(key=("a", "b"), type="string", default="default"),
                click.option("--a_b", type=str, default="default"),
                id="default",
            ),
            param(
                cfg._click.Flag(key=("a", "b"), type="string", secret=True),
                click.option("--a_b", type=str, show_default=False),
                id="secret",
            ),
            param(
                cfg._click.Flag(key=("a", "b"), type="boolean"),
                click.option("--a_b/--a_no_b", type=bool, default=True),
                id="boolean",
            ),
            param(
                cfg._click.Flag(key=("a", "b"), type="string", enum=["A", "B", "C"]),
                click.option("--a_b", type=click.Choice(["A", "B", "C"])),
                id="boolean",
            ),
            *(
                param(
                    cfg._click.Flag(key=("a", "b"), type=_type),  # type: ignore[arg-type]
                    click.option("--a_b", type=pytype),
                    id=_type,
                )
                for _type, pytype, in [
                    ("string", str),
                    ("integer", int),
                    ("number", float),
                    ("array", cfg._click.TypedArray("string")),
                    ("mapping", cfg._click.StringMapping()),
                    ("path", click.Path()),
                    (None, str),
                ]
            ),
        ],
    )
    def test_flag(flag, click_option):
        _click_info = click_option(lambda: ...).__click_params__[0].to_info_dict()
        _flag_info = flag.click_option(lambda: ...).__click_params__[0].to_info_dict()

        assert _click_info == _flag_info

    @staticmethod
    def test_invalid_type():
        with raises(ValueError):
            cfg._click.Flag(key=("a", "b"), type="invalid")

    @staticmethod
    def test_invalid_key():
        _flag = cfg._click.Flag()
        with raises(ValueError):
            _flag.key = "INVALID"

    @staticmethod
    def test_unset_key():
        _flag = cfg._click.Flag()
        with raises(ValueError):
            _ = _flag.key


class Test_Schema:
    @staticmethod
    @mark.parametrize(
        "schema,expected",
        [
            param(
                LIB / "schema" / "parse" / "nested.yaml",
                {
                    "type": "object",
                    "properties": {
                        "a": {"type": "object", "properties": {"b": {"type": "string"}}}
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
                            }
                        },
                        "d": {"type": "string", "default": "MERGE_D"},
                    }
                },
                id="merge",
            ),
        ],
    )
    def test_from_file(schema, expected):
        _schema = cfg.Schema.from_file(schema)
        assert data.as_dict(_schema) == expected

    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(LIB / "schema" / "gen" / "basic.yaml", id="basic"),
            param(LIB / "schema" / "gen" / "no_default.yaml", id="no_default"),
            param(LIB / "schema" / "gen" / "array.yaml", id="array"),
            param(LIB / "schema" / "gen" / "mapping.yaml", id="object"),
            param(LIB / "schema" / "gen" / "nested.yaml", id="nested"),
        ],
    )
    def test_example_config(definition):
        _definition = _YAML.load(definition.read_text())
        _schema = cfg.Schema(_definition["schema"])
        assert _schema.example_config == _definition["example"]


class Test__get_flags:
    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(
                LIB / "schema" / "flags" / "nested.yaml",
                id="nested",
            ),
            param(
                LIB / "schema" / "flags" / "multiple.yaml",
                id="multiple",
            ),
            param(
                LIB / "schema" / "flags" / "default.yaml",
                id="default",
            ),
            param(
                LIB / "schema" / "flags" / "required_default.yaml",
                id="required_default",
            ),
            param(
                LIB / "schema" / "flags" / "required.yaml",
                id="required",
            ),
            param(
                LIB / "schema" / "flags" / "dependent_required.yaml",
                id="dependent_required",
            ),
            param(
                LIB / "schema" / "flags" / "dependent_schemas.yaml",
                id="dependent_schemas",
            ),
            param(
                LIB / "schema" / "flags" / "parent_required.yaml",
                id="parent_required",
            ),
            param(
                LIB / "schema" / "flags" / "nested_required.yaml",
                id="nested_required",
            ),
            param(
                LIB / "schema" / "flags" / "if_else.yaml",
                id="if_else",
            ),
            param(
                LIB / "schema" / "flags" / "all_of.yaml",
                id="all_of",
            ),
            param(
                LIB / "schema" / "flags" / "any_of.yaml",
                id="any_of",
            ),
            param(
                LIB / "schema" / "flags" / "one_of.yaml",
                id="one_of",
            ),
            param(
                LIB / "schema" / "flags" / "nested_conditional.yaml",
                id="nested_conditional",
            ),
            param(
                LIB / "schema" / "flags" / "typed_array.yaml",
                id="typed_array",
            ),
        ],
    )
    def test__get_flags(definition):
        _definition = _YAML.load(definition.read_text())
        _schema = cfg.Schema(_definition["schema"])
        _config = _definition.get("config", {})
        _expected = [cfg._click.Flag(**flag) for flag in _definition["flags"]]
        assert cfg._get_flags(_schema, _config) == _expected


class Test_Config:
    schema = cfg.Schema.from_file(LIB / "schema" / "config" / "simple.yaml")

    expected = {
        "string": "STRING",
        "integer": 1337,
        "number": 13.37,
        "boolean": True,
        "array": ["one", "two", "three"],
        "mapping": {"a": "X", "b": "Y"},
        "nested": {"a": {"b": {"c": "Z"}}},
    }

    kwargs = {
        "string": "STRING",
        "integer": 1337,
        "number": 13.37,
        "boolean": True,
        "array": ["one", "two", "three"],
        "mapping": {"a": "X", "b": "Y"},
        "nested_a_b_c": "Z",
    }

    def test_empty(self):
        assert raises(ValueError, cfg.Config, self.schema)

    def test_from_kwargs(self):
        _config = cfg.Config(
            self.schema,
            **self.kwargs,
        )
        assert data.as_dict(_config) == self.expected

    def test_from_cli(self):
        runner = CliRunner()

        @click.command()
        def _cli(**kwargs):
            kwargs.pop("config_file")
            _YAML.dump(kwargs, sys.stdout)

        _cli = reduce(lambda x, y: y.click_option(x), cfg._get_flags(self.schema), _cli)

        result = runner.invoke(
            _cli,
            [
                "--string",
                "STRING",
                "--integer",
                "1337",
                "--number",
                "13.37",
                "--boolean",
                "--array",
                ["one", "two", "three"],
                "--mapping",
                "a=X,b=Y",
                "--nested_a_b_c",
                "Z",
            ],
        )

        assert _YAML.load(result.stdout) == self.kwargs

    @mark.parametrize(
        "kwargs,expected",
        [
            param(
                {"a": "CONFIG"},
                cfg._click.Flag(key=["a"], type="string", default="CONFIG"),
                id="from_config",
            ),
            param(
                {},
                cfg._click.Flag(key=["a"], type="string", default="SCHEMA"),
                id="from_schema",
            ),
        ],
    )
    def test_flags(self, kwargs, expected):
        _definition = _YAML.load(
            (LIB / "schema" / "flags" / "default.yaml").read_text()
        )
        _schema = cfg.Schema(_definition["schema"])
        _config = cfg.Config(_schema, allow_empty=True, **kwargs)

        assert cfg._get_flags(_schema, data.as_dict(_config)) == [expected]
