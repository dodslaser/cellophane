"""Tests for the data module."""

# pylint: disable=pointless-statement
from copy import deepcopy
from pathlib import Path
from typing import ClassVar, Generator

import dill
from attrs import define, field
from pytest import FixtureRequest, MonkeyPatch, fixture, mark, param, raises

from cellophane.src import data

LIB = Path(__file__).parent / "lib"


@define(init=False, slots=False)
class Dummy(data.Container):
    """Dummy Container subclass for testing."""

    a: int = field(default=1337)
    x: data.Container | None = field(default=None)

    @a.validator
    def _validate_a(self, attribute: str, value: int) -> None:
        del attribute  # unused
        if isinstance(value, int) and value > 9000:
            raise ValueError("It's over 9000!")


class Test_Container:
    """Test data.Container."""

    @staticmethod
    def test_init() -> None:
        """Test __init__."""
        _container = data.Container(a=1337)
        assert _container.__data__ == {"a": 1337}

    @staticmethod
    def test_setitem_getitem() -> None:
        """Test __setitem__ and __getitem__."""
        _container = data.Container()
        _container["a"] = 1337
        _container["b", "c"] = 1338
        _container["d"] = {"e": 1339}
        assert _container["a"] == 1337
        assert _container["b", "c"] == 1338
        assert _container["d", "e"] == 1339

        with raises(TypeError):
            _container[1337] = 1337  # type: ignore[index]

        with raises(TypeError):
            _container[1337]  # type: ignore[index]

    def test_setitem_attr(self) -> None:
        """Test __setitem__ for attr"""
        _dummy = Dummy()
        _dummy["a"] = 42
        assert _dummy.a == 42

    @staticmethod
    def test_setattr_getattr() -> None:
        """Test __setattr__ and __getattr__."""
        _container = data.Container()
        _container.a = 1337
        assert _container.a == 1337
        assert raises(AttributeError, lambda: _container.b)

    @staticmethod
    def test_deepcopy() -> None:
        """Test deepcopy."""
        _dummy = Dummy(a={"b": 1338})  # type: ignore[call-arg]
        _dummy_ref = _dummy
        _dummy_copy = deepcopy(_dummy)

        assert _dummy_ref == _dummy
        assert _dummy_copy == _dummy

        assert _dummy_ref is _dummy
        assert _dummy_copy is not _dummy

        assert _dummy_ref.a is _dummy.a
        assert _dummy_copy.a is not _dummy.a

    @staticmethod
    def test_as_dict() -> None:
        """Test as_dict."""
        _container = data.Container(a={"b": 1337})
        _container.a.f = 1338
        _container.c = 1339
        assert data.as_dict(_container) == {"a": {"b": 1337, "f": 1338}, "c": 1339}

    @staticmethod
    def test_contains() -> None:
        """Test __contains__."""
        _dummy = Dummy(x={"y": 1338})
        assert "a" in _dummy
        assert _dummy.x is not None and "y" in _dummy.x
        assert "z" not in _dummy

    @staticmethod
    def test_or() -> None:
        """Test __or__."""
        _dummy_a = Dummy(b={"c": 1338})
        _dummy_b = Dummy(b={"d": 1339})
        _dummy_a_or_b = _dummy_a | _dummy_b
        assert _dummy_a_or_b == Dummy(b={"c": 1338, "d": 1339})

    @staticmethod
    def test_invalid_or() -> None:
        """Test __or__ with invalid types."""
        _dummy_a = Dummy(b={"c": 1338})
        _dummy_b = data.Container(b={"d": 1339})
        with raises(TypeError):
            _dummy_a | _dummy_b


class Test_Sample:
    """Test data.Sample."""

    @staticmethod
    def test_init() -> None:
        """Test __init__."""
        _sample = data.Sample(id="a", files=["b"])
        assert _sample.id == "a"
        assert str(_sample) == "a"
        assert _sample.files == [Path("b")]
        assert _sample.processed is False

    @staticmethod
    def test_setitem() -> None:
        """Test __setitem__."""

        @define
        class _SampleSub(data.Sample):  # type: ignore[no-untyped-def]
            a: int = 1337

        _sample = _SampleSub(id="a", files=["b"])
        assert _sample.a == 1337
        _sample["a"] = 42
        assert _sample.a == 42

        with raises(KeyError):
            _sample["b"] = 42

    @staticmethod
    def test_and() -> None:
        """Test __and__."""

        class _SampleSubA(data.Sample):
            pass

        class _SampleSubB(data.Sample):
            pass

        _sample_a1 = _SampleSubA(id="a1", files=["a1"])
        _sample_a2 = _SampleSubA(id="a2", files=["a2"])
        _sample_a1_2 = deepcopy(_sample_a1)
        _sample_a1_2.files = [Path("a1_2")]

        _sample_a1_merge = _sample_a1 & _sample_a1_2

        assert _sample_a1_merge.files == [Path("a1"), Path("a1_2")]
        with raises(data.MergeSamplesUUIDError):
            _sample_a1 & _sample_a2

    @staticmethod
    def test_pickle() -> None:
        """Test pickling."""
        _sample = data.Sample(id="a", files=["b"])
        _pickle = dill.dumps(_sample)
        assert dill.loads(_pickle) == _sample

    @staticmethod
    def test_with_mixins() -> None:
        """Test with_mixins."""

        @define(slots=False)
        class _mixin(data.Sample):  # type: ignore[no-untyped-def]
            a: str = "Hello"
            b: str = field(default="World")
            c: int = 1337
            d: ClassVar[int] = 1338

        _sample_class: type[_mixin] = data.Sample.with_mixins(
            [_mixin]  # type: ignore[assignment]
        )

        assert _sample_class is not data.Samples
        assert _sample_class.d == 1338

        _sample = _sample_class(id="DUMMY", c=1339)

        assert _sample.a == "Hello"
        assert _sample.b == "World"
        assert _sample.c == 1339

    @staticmethod
    def test_slotted_mixin() -> None:
        """Test slotted mixin."""

        @define(slots=True)
        # FIXME: What triggers this mypy error?
        class _mixin(data.Sample):  # type: ignore[no-untyped-def]
            a: str = "Hello"

        with raises(TypeError):
            data.Sample.with_mixins([_mixin])


class Test_Samples:
    """Test data.Samples."""

    @staticmethod
    @fixture(scope="function")
    def samples() -> data.Samples[data.Sample]:
        """Dummy data.Samples fixture."""
        return data.Samples(
            [
                data.Sample(id="a", files=["a", "b"]),
                data.Sample(id="a", files=["c", "d"]),
                data.Sample(id="b", files=["e", "f"]),
            ]
        )

    @staticmethod
    @fixture(scope="function")
    def valid_samples() -> data.Samples[data.Sample]:
        """Dummy data.Samples fixture."""
        return data.Samples(
            [
                data.Sample(id="a", files=[LIB / "misc" / "dummy_1"]),
                data.Sample(id="b", files=[LIB / "misc" / "dummy_2"]),
            ]
        )

    @staticmethod
    def test_init(samples: data.Samples[data.Sample]) -> None:
        """Test __init__."""
        assert samples

    @staticmethod
    def test_from_file(samples: data.Samples[data.Sample]) -> None:
        """Test from_file."""
        _samples = data.Samples.from_file(LIB / "config" / "samples.yaml")
        assert not {s.id for s in _samples} - {s.id for s in samples}

    @staticmethod
    @mark.parametrize(
        "link,expected_groups",
        [
            param(None, [(0, 1, 2)], id="by_none"),
            param("uuid", [(0,), (1,), (2,)], id="by_uuid"),
            param("id", [(0, 1), (2,)], id="by_id"),
        ],
    )
    def test_split(
        samples: data.Samples[data.Sample],
        link: str | None,
        expected_groups: list[tuple[int, ...]],
    ) -> None:
        """Test split."""
        _split = [*samples.split(by=link)]

        assert len(_split) == len(expected_groups)
        for (_, group), expected in zip(_split, expected_groups):
            assert group == data.Samples([samples[i] for i in expected])

    @staticmethod
    def test_unique_ids(samples: data.Samples[data.Sample]) -> None:
        """Test unique_ids."""
        assert samples.unique_ids == {"a", "b"}

    @staticmethod
    def test_complete_failed(samples: data.Samples[data.Sample]) -> None:
        """Test complete and failed."""
        assert not samples.complete
        assert samples.failed == samples

        for s in samples:
            s.processed = True

        assert samples.complete == samples

        samples[1].fail("DUMMY")
        assert samples[1] in samples.failed

    @staticmethod
    def test_with_files(
        samples: data.Samples[data.Sample],
        valid_samples: data.Samples[data.Sample],
    ) -> None:
        """Test with_files."""
        assert len(samples.with_files) == 0
        assert len(valid_samples.with_files) == 2

    @staticmethod
    def test_without_files(
        samples: data.Samples[data.Sample],
        valid_samples: data.Samples[data.Sample],
    ) -> None:
        """Test with_files."""
        assert len(samples.without_files) == 3
        assert len(valid_samples.without_files) == 0

    @staticmethod
    def test_str(samples: data.Samples[data.Sample]) -> None:
        """Test __str__."""
        assert str(samples) == "\n".join([s.id for s in samples])

    @staticmethod
    def test_with_sample_class() -> None:
        """Test with_sample_class."""

        class _SampleSub(data.Sample):
            pass

        _samples = data.Samples.with_sample_class(_SampleSub)
        assert _samples is not data.Samples
        assert _samples.sample_class is _SampleSub

    @staticmethod
    def test_with_mixins() -> None:
        """Test with_mixins."""

        class _mixin(data.Samples):
            a: str = "Hello"
            b: str = field(default="World")
            c: int = 1337
            d: ClassVar[int] = 1338

        _samples_class: type[_mixin] = data.Samples.with_mixins(
            [_mixin]  # type: ignore[assignment]
        )
        assert _samples_class is not data.Samples
        assert _samples_class.d == 1338

        _samples = _samples_class(c=1339)
        assert _samples.a == "Hello"
        assert _samples.b == "World"
        assert _samples.c == 1339

    @staticmethod
    def test_pickle(samples: data.Samples[data.Sample]) -> None:
        """Test pickling."""
        _samples_pickle = dill.dumps(samples)
        _samples_unpickle = dill.loads(_samples_pickle)

        assert samples == _samples_unpickle

    @staticmethod
    def test_getitem_setitem() -> None:
        """Test __getitem__ and __setitem__."""
        samples: data.Samples[data.Sample] = data.Samples(
            [
                data.Sample(id="a", files=["a", "b"]),
                data.Sample(id="b", files=["c", "d"]),
            ]
        )
        assert samples[samples[0].uuid] == samples[0]  # pylint: disable=no-member

        sample_c = data.Sample(id="c", files=["e", "f"])
        with raises(KeyError):
            samples[sample_c.uuid]

        sample_c.processed = True
        samples[sample_c.uuid] = sample_c
        assert samples[sample_c.uuid].processed is True

        samples[sample_c.uuid] = sample_c
        assert samples[sample_c.uuid] == sample_c

        sample_d = data.Sample(id="d", files=["g", "h"])
        samples[0] = sample_d
        assert samples[sample_d.uuid] == samples[0] == sample_d

        with raises(TypeError):
            samples["INVALID"] = sample_d  # type: ignore[index]

        with raises(TypeError):
            samples["INVALID"]  # type: ignore[index]

    @staticmethod
    def test_contains() -> None:
        """Test __contains__."""
        sample_a = data.Sample(id="a", files=["a", "b"])
        sample_b = data.Sample(id="b", files=["c", "d"])
        samples: data.Samples = data.Samples([sample_a, sample_b])

        assert sample_a in samples
        assert sample_b in samples
        assert sample_a.uuid in samples
        assert sample_b.uuid in samples

    @staticmethod
    def test_and() -> None:
        """Test __and__."""

        class _SamplesSubA(data.Samples):
            pass

        class _SamplesSubB(data.Samples):
            pass

        _samples_a1 = _SamplesSubA(
            [
                data.Sample(id="a1_1", files=["a1_1"]),
                data.Sample(id="a1_2", files=["a1_2"]),
            ]
        )

        _samples_a2 = _SamplesSubA(
            [
                data.Sample(id="a2_1", files=["a2_1"]),
                data.Sample(id="a2_2", files=["a2_2"]),
            ]
        )

        assert _samples_a1 & _samples_a2

    @staticmethod
    def test_or() -> None:
        """Test __or__."""

        class _SamplesSub(data.Samples):
            pass

        _sample_a = data.Sample(id="a", files=["a", "b"])
        _sample_b = data.Sample(id="b", files=["c", "d"])
        _sample_c = data.Sample(id="c", files=["e", "f"])

        _samples_1: data.Samples = data.Samples([_sample_a, _sample_b])
        _samples_2: data.Samples = data.Samples([_sample_b, _sample_c])
        _samples_3 = _SamplesSub([_sample_c])

        _samples_1_or_2 = _samples_1 | _samples_2

        assert _samples_1_or_2 == data.Samples([_sample_a, _sample_b, _sample_c])

        with raises(data.MergeSamplesTypeError):
            _samples_1 | _samples_3


class Test_Output:
    """Test data.Output."""

    def test_hash(self) -> None:
        """Test __hash__."""
        a = data.Output(src="src", dst="dst")
        b = data.Output(src="src", dst="dst")
        c = data.Output(src="src", dst="dst_2")

        assert {a, b, c} == {a, c}


class Test_OutputGlob:
    """Test data.OutputGlob."""

    @fixture(scope="function")
    @staticmethod
    def meta(
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
    ) -> Generator[dict[str, Path], None, None]:
        """Dummy metadata for output formatting."""
        workdir = tmp_path / "workdir"
        workdir.mkdir(exist_ok=True)
        (workdir / "x_a").touch()
        (workdir / "x_b").touch()
        (workdir / "y_a").touch()
        (workdir / "y_b").touch()
        (workdir / "z").mkdir()
        (workdir / "z" / "a").touch()
        (workdir / "z" / "b").touch()
        (workdir / "z" / "c").mkdir()
        (workdir / "z" / "c" / "a").touch()

        monkeypatch.chdir(tmp_path)

        yield {
            "_workdir": tmp_path / "workdir",
            "_resultdir": tmp_path / "resultdir",
        }

    @fixture(scope="function")
    @staticmethod
    def expected_outputs(
        meta: dict,
        request: FixtureRequest,
    ) -> Generator[set[data.Output], None, None]:
        """Append tmp_path to expected outputs."""
        outputs = request.param
        for output in outputs:
            output.src = Path(str(output.src).format(**meta))
            output.dst = Path(str(output.dst).format(**meta))
        yield {*outputs}

    @fixture(scope="function")
    @staticmethod
    def config() -> Generator[data.Container, None, None]:
        """Dummy config fixture."""
        yield data.Container(resultdir=Path("resultdir"))

    @staticmethod
    def test_hash() -> None:
        """Test __hash__."""
        a = data.OutputGlob(
            src="src", dst_dir="dst_parent/dst_dir", dst_name="dst_name"
        )
        b = data.OutputGlob(
            src="src", dst_dir="dst_parent/dst_dir", dst_name="dst_name"
        )
        c = data.OutputGlob(
            src="src", dst_dir="dst_parent/dst_dir", dst_name="dst_name_2"
        )

        assert {a, b, c} == {a, c}

    @staticmethod
    @mark.parametrize(
        "kwargs,expected_outputs",
        [
            param(
                {"src": "*", "dst_dir": None, "dst_name": None},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/x_a"),
                    data.Output(src="workdir/x_b", dst="resultdir/x_b"),
                    data.Output(src="workdir/y_a", dst="resultdir/y_a"),
                    data.Output(src="workdir/y_b", dst="resultdir/y_b"),
                    data.Output(src="workdir/z", dst="resultdir/z"),
                ],
                id="wildcard",
            ),
            param(
                {"src": "x_*", "dst_dir": None, "dst_name": None},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/x_a"),
                    data.Output(src="workdir/x_b", dst="resultdir/x_b"),
                ],
                id="partial_wildcard",
            ),
            param(
                {"src": "{_workdir}/x_*", "dst_dir": None, "dst_name": None},
                [
                    data.Output(src="{_workdir}/x_a", dst="resultdir/x_a"),
                    data.Output(src="{_workdir}/x_b", dst="resultdir/x_b"),
                ],
                id="absolute_src",
            ),
            param(
                {"src": "workdir/x_*", "dst_dir": None, "dst_name": None},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/x_a"),
                    data.Output(src="workdir/x_b", dst="resultdir/x_b"),
                ],
                id="relative_src",
            ),
            param(
                {"src": "workdir/x_*", "dst_dir": "{_resultdir}", "dst_name": None},
                [
                    data.Output(src="workdir/x_a", dst="{_resultdir}/x_a"),
                    data.Output(src="workdir/x_b", dst="{_resultdir}/x_b"),
                ],
                id="absolute_dst",
            ),
            param(
                {"src": "workdir/x_*", "dst_dir": "DUMMY", "dst_name": None},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/DUMMY/x_a"),
                    data.Output(src="workdir/x_b", dst="resultdir/DUMMY/x_b"),
                ],
                id="relative_dst",
            ),
            param(
                {"src": "workdir/x_a", "dst_dir": None, "dst_name": "RENAME"},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/RENAME"),
                ],
                id="rename",
            ),
            param(
                {"src": "workdir/*_a", "dst_dir": None, "dst_name": "RENAME"},
                [
                    data.Output(src="workdir/x_a", dst="resultdir/x_a"),
                    data.Output(src="workdir/y_a", dst="resultdir/y_a"),
                ],
                id="invalid_multi_rename",
            ),
            param(
                {"src": "INVALID", "dst_dir": None, "dst_name": None},
                [],
                id="no_match",
            ),
        ],
        indirect=["expected_outputs"],
    )
    def test_resolve(
        meta: dict,
        config: data.Container,
        kwargs: dict,
        expected_outputs: set[data.Output],
    ) -> None:
        """Test resolve."""
        glob = data.OutputGlob(
            src=kwargs.pop("src").format(**meta),
            dst_dir=(
                dst_dir.format(**meta) if (dst_dir := kwargs.pop("dst_dir")) else None
            ),
            dst_name=(
                dst_name.format(**meta)
                if (dst_name := kwargs.pop("dst_name"))
                else None
            ),
            **kwargs,
        )
        outputs = glob.resolve(
            samples=[None],  # type: ignore[arg-type]
            workdir=Path("workdir"),
            config=config,
        )
        assert outputs == expected_outputs
