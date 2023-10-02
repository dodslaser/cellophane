import logging
import sys
from pathlib import Path
from typing import Literal
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import rich_click as click
from click.testing import CliRunner
from pytest import LogCaptureFixture, mark, param
from ruamel.yaml import YAML

import cellophane
from cellophane import cfg, data, modules

LIB = Path(__file__).parent / "lib"
_YAML = YAML(typ="safe", pure=True)


class Test__run_hooks:
    @staticmethod
    @mark.parametrize(
        "when,samples,calls",
        [
            param(
                "pre",
                MagicMock(),
                {
                    "pre": 1,
                    "post_complete": 0,
                    "post_always": 0,
                    "post_failed": 0,
                },
                id="pre",
            ),
            param(
                "post",
                MagicMock(complete=True, failed=False),
                {
                    "pre": 0,
                    "post_complete": 1,
                    "post_always": 1,
                    "post_failed": 0,
                },
                id="post_complete",
            ),
            param(
                "post",
                MagicMock(complete=False, failed=True),
                {
                    "pre": 0,
                    "post_complete": 0,
                    "post_always": 1,
                    "post_failed": 1,
                },
                id="post_failed",
            ),
            param(
                "post",
                MagicMock(complete=True, failed=True),
                {
                    "pre": 0,
                    "post_complete": 1,
                    "post_always": 1,
                    "post_failed": 1,
                },
                id="post_mixed",
            ),
        ],
    )
    def test__run_hooks(
        when: Literal["pre", "post"], samples: MagicMock, calls: dict[str, int]
    ):
        _hooks = {
            "pre": MagicMock(when="pre", condition="always"),
            "post_complete": MagicMock(when="post", condition="complete"),
            "post_always": MagicMock(when="post", condition="always"),
            "post_failed": MagicMock(when="post", condition="failed"),
        }

        cellophane._run_hooks(_hooks.values(), when, samples)  # type: ignore[arg-type]

        assert all(
            _hooks[hook].call_count == call_count for hook, call_count in calls.items()
        )


class Test__start_runners:
    class SampleMixin(data.Sample):
        custom_prop: str = "custom"
        runner: str | None = None
        call_uuid: UUID | None = None

    class SamplesMixin(data.Samples):
        custom_prop: str = "custom"

        def with_call_id(self, runner):
            _uuid = uuid4()
            for s in self:
                s.runner = runner
                s.call_uuid = _uuid
            return self

        @property
        def call_count(self):
            return {
                r: len({s.call_uuid for s in self if s.runner == r})
                for r in {s.runner for s in self}
                if r is not None
            }

    @staticmethod
    @modules.runner(
        individual_samples=False,
        link_by=None,
    )
    def _base_runner(samples, **_):
        return samples.with_call_id("base")

    @staticmethod
    @modules.runner(
        individual_samples=True,
        link_by="id",
    )
    def _linked_samples_runner(samples, **_):
        return samples.with_call_id("linked")

    @staticmethod
    @modules.runner(
        individual_samples=True,
        link_by=None,
    )
    def _individual_samples_runner(samples, **_):
        return samples.with_call_id("individual")

    def test__start_runners(self, tmp_path: Path):
        _SAMPLES = data.Samples.with_mixins([self.SamplesMixin])
        _SAMPLE = data.Sample.with_mixins([self.SampleMixin])
        _SAMPLES.sample_class = _SAMPLE

        _samples = _SAMPLES(
            [
                _SAMPLE(id="a", runner=None),
                _SAMPLE(id="b", runner=None),
                _SAMPLE(id="c", runner=None),
                _SAMPLE(id="c", runner=None),
            ]
        )
        _logger_mock = MagicMock()

        _config = cfg.Config(cfg.Schema(), allow_empty=True)
        _config.log_level = logging.DEBUG
        _config.timestamp = "DUMMY"
        _config.outdir = tmp_path

        _ret = cellophane._start_runners(
            [
                self._base_runner,
                self._linked_samples_runner,
                self._individual_samples_runner,
            ],
            samples=_samples,
            logger=_logger_mock,
            config=_config,
            root=tmp_path,
        )

        assert _ret.call_count == {
            "base": 1,
            "linked": 3,
            "individual": 4,
        }

    @staticmethod
    @mark.parametrize(
        "exception,log_lines",
        [
            param(
                Exception("DUMMY"),
                [["Unhandled exception in runner: DUMMY"]],
                id="Exception",
            ),
            param(
                KeyboardInterrupt,
                [["Received SIGINT, telling runners to shut down..."]],
                id="SystemExit",
            ),
        ],
    )
    def test__start_runners_exceptions(
        mocker,
        exception: Exception,
        log_lines: list[list[str]],
        caplog: LogCaptureFixture,
    ):
        mocker.patch(
            "cellophane.WorkerPool.apply_async",
            side_effect=exception,
        )
        cellophane._start_runners(
            [MagicMock(individual_samples=False, link_by=None)],
            samples=["DUMMY"],  # type: ignore[arg-type]
            logger=cellophane.logs.get_labeled_adapter("DUMMY"),
        )

        for line in log_lines:
            assert "\n".join(line) in "\n".join(caplog.messages)


class Test__add_config_defaults:
    def test__add_config_flags(self):
        _runner = CliRunner()
        _schema = cfg.Schema.from_file(LIB / "schema" / "config_simple.yaml")
        _logger_mock = MagicMock()

        @_schema.add_options
        @click.command()
        @click.option(
            "--config",
            is_eager=True,
            callback=lambda ctx, _, value: cellophane._add_config_defaults(
                ctx, value, _schema, _logger_mock
            ),
        )
        def _cli(**kwargs):
            _YAML.dump(kwargs, sys.stdout)

        result = _runner.invoke(_cli, ["--config", LIB / "config" / "simple.yaml"])

        assert _YAML.load(result.stdout) == {
            "string": "STRING",
            "integer": 1337,
            "number": 13.37,
            "boolean": True,
            "array": ["one", "two", "three"],
            "mapping": {"a": "X", "b": "Y"},
            "nested_a_b_c": "Z",
            "config": str(LIB / "config" / "simple.yaml"),
        }

    def test__add_config_flags_exception(self, mocker):
        _runner = CliRunner()
        _schema = cfg.Schema.from_file(LIB / "schema" / "config_simple.yaml")
        _logger_mock = MagicMock()

        mocker.patch(
            "cellophane.cfg.Config.from_file",
            side_effect=Exception("DUMMY"),
        )

        @_schema.add_options
        @click.command()
        @click.option(
            "--config",
            is_eager=True,
            callback=lambda ctx, _, value: cellophane._add_config_defaults(
                ctx, value, _schema, _logger_mock
            ),
        )
        def _cli(**kwargs):
            _YAML.dump(kwargs, sys.stdout)

        _result = _runner.invoke(_cli, ["--config", LIB / "config" / "simple.yaml"])

        assert isinstance(_result.exception, SystemExit)
        assert _result.exit_code == 1

class Test_cellophane:
    """
    This test suite serves as a small integration test for cellophane.

    It is not meant to be exhaustive, but rather to ensure that the
    basic functionality works as expected from the command line interface.
    """

    @staticmethod
    @mark.parametrize(
        "definition",
        [
            param(LIB / "integration" / "good_basic.yaml", id="good_basic"),
            param(LIB / "integration" / "bad_sample.yaml", id="bad_sample"),
            param(LIB / "integration" / "bad_hook_order.yaml", id="bad_hook_order"),
            param(LIB / "integration" / "bad_schema.yaml", id="bad_schema"),
            param(LIB / "integration" / "bad_args.yaml", id="bad_args"),
            param(LIB / "integration" / "bad_module.yaml", id="bad_module"),
        ],
    )
    def test_cellophane(
        definition: Path,
        run_definition,
    ):
        run_definition(definition)
