from copy import deepcopy
from pathlib import Path
from typing import ClassVar

import cloudpickle
import dill
from attrs import define, field
from pytest import fixture, mark, param, raises
from uuid import uuid4

from cellophane.src import data

LIB = Path(__file__).parent / "lib"


@define(init=False, slots=False)
class Dummy(data.Container):
    a: int = field(default=1337)

    @a.validator
    def _validate_a(self, _, value):
        if isinstance(value, int) and value > 9000:
            raise ValueError("It's over 9000!")


class Test_Container:
    @staticmethod
    def test_init():
        _container = data.Container(a=1337)
        assert _container.__data__ == {"a": 1337}

    @staticmethod
    def test_setitem_getitem():
        _container = data.Container()
        _container["a"] = 1337
        _container["b", "c"] = 1338
        _container["d"] = {"e": 1339}
        assert _container["a"] == 1337
        assert _container["b", "c"] == 1338
        assert _container["d", "e"] == 1339

        with raises(TypeError):
            _container[1337] = 1337

        with raises(TypeError):
            _container[1337]  # pylint: disable=pointless-statement

    def test_setitem_attr(self):
        _dummy = Dummy()
        _dummy["a"] = 42
        assert _dummy.a == 42

    @staticmethod
    def test_setattr_getattr():
        _container = data.Container()
        _container.a = 1337
        assert _container.a == 1337
        assert raises(AttributeError, lambda: _container.b)

    @staticmethod
    def test_deepcopy() -> None:
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
    def test_as_dict():
        _container = data.Container(a={"b": 1337})
        _container.a.f = 1338
        _container.c = 1339
        assert data.as_dict(_container) == {"a": {"b": 1337, "f": 1338}, "c": 1339}

    @staticmethod
    def test_contains():
        _dummy = Dummy(a={"b": 1338})
        assert "a" in _dummy
        assert "b" in _dummy.a
        assert "c" not in _dummy

    @staticmethod
    def test_or():
        _dummy_a = Dummy(b={"c": 1338})
        _dummy_b = Dummy(b={"d": 1339})
        _dummy_a_or_b = _dummy_a | _dummy_b
        assert _dummy_a_or_b == Dummy(b={"c": 1338, "d": 1339})

    @staticmethod
    def test_invalid_or():
        _dummy_a = Dummy(b={"c": 1338})
        _dummy_b = data.Container(b={"d": 1339})
        with raises(TypeError):
            _dummy_a | _dummy_b
        
class Test_Sample:
    @staticmethod
    def test_init():
        _sample = data.Sample(id="a", files=["b"])
        assert _sample.id == "a"
        assert str(_sample) == "a"
        assert _sample.files == [Path("b")]
        assert _sample.processed == False

    @staticmethod
    def test_setitem():
        @define
        class SampleSub(data.Sample):
            a: int = 1337
        
        _sample = SampleSub(id="a", files=["b"])
        assert _sample.a == 1337
        _sample["a"] = 42
        assert _sample.a == 42

        with raises(KeyError):
            _sample["b"] = 42
    
    @staticmethod
    def test_and():
        class _SampleSubA(data.Sample):
            ...
        
        class _SampleSubB(data.Sample):
            ...
        
        _sample_a1 = _SampleSubA(id="a1", files=["a1"])
        _sample_a2 = _SampleSubA(id="a2", files=["a2"])
        _sample_a1_2 = deepcopy(_sample_a1)
        _sample_a1_2.files = ["a1_2"]
        _sample_b = _SampleSubB(id="b", files=["d"])

        _sample_a1_merge = _sample_a1 & _sample_a1_2

        assert  _sample_a1_merge.files == [Path("a1"), Path("a1_2")]
        with raises(TypeError):
            _sample_a1 & _sample_b
        with raises(ValueError):
            _sample_a1 & _sample_a2

    @staticmethod
    def test_pickle():
        _sample = data.Sample(id="a", files=["b"])
        _pickle = dill.dumps(_sample)
        assert dill.loads(_pickle) == _sample

    @staticmethod
    def test_with_mixins():
        class _mixin(data.Sample):
            a: str = "Hello"
            b: str = field(default="World")
            c: int = 1337
            d: ClassVar[int] = 1338

        _sample_class = data.Sample.with_mixins([_mixin])

        assert _sample_class is not data.Samples
        assert _sample_class.d == 1338

        _sample = _sample_class(id="DUMMY", c=1339)

        assert _sample.a == "Hello"
        assert _sample.b == "World"
        assert _sample.c == 1339



class Test_Samples:
    @staticmethod
    @fixture(scope="function")
    def samples():
        return data.Samples(
            [
                data.Sample(
                    id="a",
                    files=["a", "b"],
                ),  # type: ignore[call-arg]
                data.Sample(
                    id="a",
                    files=["c", "d"],
                ),  # type: ignore[call-arg]
                data.Sample(
                    id="b",
                    files=["e", "f"],
                ),  # type: ignore[call-arg]
            ]
        )

    @staticmethod
    @fixture(scope="function")
    def valid_samples():
        return data.Samples(
            [
                data.Sample(
                    id="a",
                    files=[LIB / "misc" / "dummy_1"],
                ),  # type: ignore[call-arg]
                data.Sample(
                    id="b",
                    files=[LIB / "misc" / "dummy_2"],
                ),  # type: ignore[call-arg]
            ]
        )

    @staticmethod
    def test_init(samples):
        assert samples

    @staticmethod
    def test_from_file(samples):
        _samples = data.Samples.from_file(LIB / "config" / "samples.yaml")
        assert not {s.id for s in _samples} - {s.id for s in samples}

    @staticmethod
    @mark.parametrize(
        "link,expected_groups",
        [
            param(None, [(0,), (1,), (2,)], id="by_none"),
            param("id", [(0, 1), (2,)], id="by_id"),
        ],
    )
    def test_split(samples, link, expected_groups):
        _split = [*samples.split(link_by=link)]

        assert len(_split) == len(expected_groups)
        for group, expected in zip(_split, expected_groups):
            assert group == data.Samples([samples[i] for i in expected])

    @staticmethod
    def test_unique_ids(samples):
        assert samples.unique_ids == {"a", "b"}

    @staticmethod
    def test_complete_failed(samples):
        assert not samples.complete
        assert samples.failed == samples

        for s in samples:
            s.processed = True

        assert samples.complete == samples

        samples[1].fail("DUMMY")
        assert samples[1] in samples.failed

    @staticmethod
    def test_with_files(samples, valid_samples):
        assert len(samples.with_files) == 0
        assert len(valid_samples.with_files) == 2

    @staticmethod
    def test_str(samples):
        assert str(samples) == "\n".join([s.id for s in samples])

    @staticmethod
    def test_with_sample_class():
        _samples = data.Samples.with_sample_class(Dummy)
        assert _samples is not data.Samples
        assert _samples.sample_class is Dummy

    @staticmethod
    def test_with_mixins():
        class _mixin(data.Samples):
            a: str = "Hello"
            b: str = field(default="World")
            c: int = 1337
            d: ClassVar[int] = 1338

        _samples_class = data.Samples.with_mixins([_mixin])
        assert _samples_class is not data.Samples
        assert _samples_class.d == 1338

        _samples = _samples_class(c=1339)
        assert _samples.a == "Hello"
        assert _samples.b == "World"
        assert _samples.c == 1339


    @staticmethod
    def test_pickle(samples):
        _samples_pickle = cloudpickle.dumps(samples)
        _samples_unpickle = cloudpickle.loads(_samples_pickle)

        assert samples == _samples_unpickle
    
    @staticmethod
    def test_getitem_setitem():
        samples = data.Samples(
            [
                data.Sample(id="a", files=["a", "b"]),
                data.Sample(id="b", files=["c", "d"]),
            ]
        )
        assert samples[samples[0].uuid] == samples[0]

        sample_c = data.Sample(id="c", files=["e", "f"])
        with raises(KeyError):
            samples[sample_c.uuid]
        
        sample_c.processed = True
        samples[sample_c.uuid] = sample_c
        assert samples[sample_c.uuid].processed == True
        
        samples[sample_c.uuid] = sample_c
        assert samples[sample_c.uuid] == sample_c

        sample_d = data.Sample(id="d", files=["g", "h"])
        samples[0] = sample_d
        assert samples[sample_d.uuid] == samples[0] == sample_d

        with raises(TypeError):
            samples["INVALID"] = sample_d

        with raises(TypeError):
            samples["INVALID"]

    @staticmethod
    def test_contains():
        sample_a = data.Sample(id="a", files=["a", "b"])
        sample_b = data.Sample(id="b", files=["c", "d"])
        samples = data.Samples([sample_a, sample_b])

        assert sample_a in samples
        assert sample_b in samples
        assert sample_a.uuid in samples
        assert sample_b.uuid in samples

    @staticmethod
    def test_and():
        class _SamplesSubA(data.Samples):
            ...
        
        class _SamplesSubB(data.Samples):
            ...
        
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

        _samples_b = _SamplesSubB(
            [
                data.Sample(id="b1", files=["b1"]),
                data.Sample(id="b2", files=["b2"]),
            ]
        )

        _samples_a_merge = _samples_a1 & _samples_a2

        # assert  _sample_a1_merge.files == [Path("a1"), Path("a1_2")]
        with raises(TypeError):
            _samples_a1 & _samples_b
    
    @staticmethod
    def test_or():
        class SamplesSub(data.Samples):
            ...
        
        _sample_a = data.Sample(id="a", files=["a", "b"])
        _sample_b = data.Sample(id="b", files=["c", "d"])
        _sample_c = data.Sample(id="c", files=["e", "f"])

        _samples_1 = data.Samples([_sample_a, _sample_b])
        _samples_2 = data.Samples([_sample_b, _sample_c])
        _samples_3 = SamplesSub([_sample_c])


        _samples_1_or_2 = _samples_1 | _samples_2

        assert _samples_1_or_2 == data.Samples([_sample_a, _sample_b, _sample_c])

        with raises(TypeError):
            _samples_1 | _samples_3
