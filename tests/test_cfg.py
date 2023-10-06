import sys
from functools import reduce
from pathlib import Path

import rich_click as click
from click.testing import CliRunner
from pytest import mark, param, raises
from ruamel.yaml import YAML

from cellophane.src import cfg, data

_YAML = YAML(typ="safe", pure=True)
LIB = Path("__file__").parent / "tests" / "lib"
SCHEMAS = {
    "nested": _YAML.load(
        (LIB / "schema" / "nested.yaml").read_text(),
    ),
    "multiple": _YAML.load(
        (LIB / "schema" / "multiple.yaml").read_text(),
    ),
    "default": _YAML.load(
        (LIB / "schema" / "default.yaml").read_text(),
    ),
    "required": _YAML.load(
        (LIB / "schema" / "required.yaml").read_text(),
    ),
    "parent_required": _YAML.load(
        (LIB / "schema" / "parent_required.yaml").read_text(),
    ),
}

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
        ],
    )
    def test_convert(value, expected):
        _mapping = cfg.StringMapping()
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
        _mapping = cfg.StringMapping()
        with raises(click.BadParameter):
            _mapping.convert(value, None, None)


class Test__Flag:
    @staticmethod
    @mark.parametrize(
        "flag,click_option",
        [
            param(
                cfg.Flag(key=("a", "b"), type="string", node_required=True, parent_required=True),
                click.option("--a_b", type=str, required=True),
                id="required",
            ),
            param(
                cfg.Flag(key=("a", "b"), type="string", default="default"),
                click.option("--a_b", type=str, default="default"),
                id="default",
            ),
            param(
                cfg.Flag(key=("a", "b"), type="string", secret=True),
                click.option("--a_b", type=str, show_default=False),
                id="secret",
            ),
            param(
                cfg.Flag(key=("a", "b"), type="boolean"),
                click.option("--a_b/--no-a_b", type=bool, default=True),
                id="boolean",
            ),
            param(
                cfg.Flag(key=("a", "b"), type="string", enum=["A", "B", "C"]),
                click.option("--a_b", type=click.Choice(["A", "B", "C"])),
                id="boolean",
            ),
            *(
                param(
                    cfg.Flag(key=("a", "b"), type=_type),  # type: ignore[arg-type]
                    click.option("--a_b", type=pytype),
                    id=_type,
                )
                for _type, pytype, in [
                    ("string", str),
                    ("integer", int),
                    ("number", float),
                    ("array", list),
                    ("mapping", cfg.StringMapping()),
                    ("path", click.Path()),
                ]
            ),
        ],
    )
    def test_flag(flag, click_option):
        _click_info = click_option(lambda: ...).__click_params__[0].to_info_dict()
        _flag_info = flag.click_option(lambda: ...).__click_params__[0].to_info_dict()

        assert _click_info == _flag_info

    @staticmethod
    def test_invalid_flag():
        with raises(ValueError):
            cfg.Flag(key=("a", "b"), type="invalid")


class Test_Schema:
    @staticmethod
    @mark.parametrize(
        "schema,expected",
        [
            param(
                LIB / "schema" / "nested.yaml",
                {"type": "object", "properties": {"a": {"type": "object", "properties": {"b": {"type": 'string'}}}}},
                id="nested"
            ),
            param(
                [
                    LIB / "schema" / "merge_a.yaml",
                    LIB / "schema" / "merge_b.yaml",
                    LIB / "schema" / "merge_c.yaml",
                    LIB / "schema" / "merge_d.yaml",
                ],
                {
                    "properties": {
                        "a": {
                            "properties": {
                                "b": {
                                    "type": "string",
                                    "default": "MERGE_B"
                                },
                                "c": {
                                    "type": "string",
                                    "default": "MERGE_C"
                                }
                            }
                        },
                        "d": {
                            "type": "string",
                            "default": "MERGE_D"
                        }
                    }
                },
                id="merge"
            )
        ]
    )
    def test_from_file(schema, expected):
        _schema = cfg.Schema.from_file(schema)
        assert _schema.as_dict == expected

    @staticmethod
    @mark.parametrize(
        "schema,expected",
        [
            param(
                LIB / "schema" / "gen_basic.yaml",
                "basic: BASIC  # DESCRIPTION (string)\n",
                id="basic",
            ),
            param(
                LIB / "schema" / "gen_no_default.yaml",
                "no_default: ~  # (string)\n",
                id="no_default",
            ),
            param(
                LIB / "schema" / "gen_array.yaml",
                "array:  # ARRAY (array)\n"
                "- A\n"
                "- B\n"
                "- C\n",
                id="array",
            ),
            param(
                LIB / "schema" / "gen_mapping.yaml",
                "mapping:  # MAPPING (mapping)\n"
                "  a: A\n"
                "  b: B\n",
                id="object",
            ),
            param(
                LIB / "schema" / "gen_nested.yaml",
                "nested:\n"
                "  a:\n"
                "    b:\n"
                "      c: C  # NESTED (string)\n",
                id="nested",
            ),


        ],
    )
    def test_example_config(schema, expected):
        _schema = cfg.Schema.from_file(schema)
        _example = _schema.example_config
        assert _example == expected

    @staticmethod
    @mark.parametrize(
        "expected,required",
        [
            param(
                [cfg.Flag(key=["a", "b"], type="string")],
                [False],
                id="nested",
            ),
            param(
                [
                    cfg.Flag(parent_present=True, key=["a"], type="string"),
                    cfg.Flag(parent_present=True, key=["b"], type="string"),
                ],
                [False, False],
                id="multiple",
            ),
            param(
                [cfg.Flag(parent_present=True, key=["a"], type="string", default="SCHEMA")],
                [False],
                id="default",
            ),
            param(
                [cfg.Flag(parent_present=True, key=["a"], type="string", node_required=True)],
                [True],
                id="required",
            ),
            param(
                [
                    cfg.Flag(parent_required=True, node_required=True, key=["a", "x"], type="string"),
                    cfg.Flag(parent_required=True, key=["a", "y"], type="string"),
                    cfg.Flag(parent_required=False, node_required=True, key=["b", "x"], type="string"),
                    cfg.Flag(parent_required=False, key=["b", "y"], type="string"),
                ],
                [True, False, False, False],
                id="parent_required",
            ),
        ],
    )
    def test_schema_flags(expected, required, request):
        _schema = cfg.Schema(SCHEMAS[request.node.callspec.id])
        _flags = [*_schema.flags]
        assert _flags == expected
        assert [f.required for f in _flags] == required


class Test_Config:
    schema = cfg.Schema.from_file(LIB / "schema" / "config_simple.yaml")

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

    def test_from_file(self):
        _config = cfg.Config.from_file(LIB / "config" / "simple.yaml", self.schema)
        assert _config.as_dict == self.expected

    def test_from_kwargs(self):
        _config = cfg.Config(
            self.schema,
            **self.kwargs,
        )
        assert _config.as_dict == self.expected

    def test_from_cli(self):
        runner = CliRunner()

        @click.command()
        def _cli(**kwargs):
            _YAML.dump(kwargs, sys.stdout)

        _cli = reduce(lambda x, y: y.click_option(x), self.schema.flags, _cli)

        result = runner.invoke(
            _cli,
            [
                "--string", "STRING",
                "--integer", "1337",
                "--number", "13.37",
                "--boolean",
                "--array", ["one", "two", "three"],
                "--mapping", "a=X,b=Y",
                "--nested_a_b_c", "Z",
            ],
        )

        assert _YAML.load(result.stdout) == self.kwargs

    @mark.parametrize(
        "kwargs,expected",
        [
            param(
                {"a": "CONFIG"},
                cfg.Flag(parent_present=True, key=["a"], type="string", default="CONFIG"),
                id="from_config",
            ),
            param(
                {},
                cfg.Flag(parent_present=True, key=["a"], type="string", default="SCHEMA"),
                id="from_schema",
            ),
        ],
    )
    def test_flags(self, kwargs, expected):
        _schema = cfg.Schema(SCHEMAS["default"])
        _config = cfg.Config(_schema, allow_empty=True, **kwargs)

        assert _config.flags == [expected]
