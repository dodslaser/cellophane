from copy import deepcopy
from pathlib import Path
from typing import ClassVar

import cloudpickle
import dill
from attrs import define, field
from pytest import fixture, mark, param, raises

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

    @staticmethod
    def test_setattr_getattr():
        _container = data.Container()
        _container.a = 1337
        assert _container.a == 1337
        assert raises(AttributeError, lambda: _container.b)

    # @staticmethod
    # def test_views() -> None:
    #     _dummy = Dummy(b=1338)  # type: ignore[call-arg]

    #     assert _dummy.__data__ == {"b": 1338}
    #     assert _dummy.a == 1337
    #     assert _dummy.b == 1338
    #     assert [*_dummy.keys()] == ["a", "b"]
    #     assert [*_dummy.values()] == [1337, 1338]
    #     assert [*_dummy.items()] == [("a", 1337), ("b", 1338)]

    #     with raises(ValueError):
    #         _dummy["a"] = 9001
    #     with raises(ValueError):
    #         _dummy.a = 9001

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


class Test_Output:
    @staticmethod
    @mark.parametrize(
        "src,dest_dir,parent_id",
        [
            param("a", "b", None, id="str"),
            param(["a", "b"], "c", None, id="list"),
            param(["a", "a", "b"], "c", None, id="duplicate"),
            param(Path("a"), Path("b"), None, id="path"),
            param("a", "b", "c", id="parent_id"),
        ],
    )
    def test_init(src, dest_dir, parent_id):
        _src = {Path(p) for p in src} if isinstance(src, list) else {Path(src)}
        _output = data.Output(src=src, dest_dir=dest_dir, parent_id=parent_id)
        assert _output.src == _src
        assert _output.dest_dir == Path(dest_dir)
        assert _output.parent_id == parent_id

    @staticmethod
    def test_set_parent_id():
        _output = data.Output(src="a", dest_dir="b")
        _output.set_parent_id("c")
        assert _output.parent_id == "c"

    @staticmethod
    @mark.parametrize(
        "a,b,expected",
        [
            param(
                data.Output(src={Path("a")}, dest_dir=Path("b")),
                data.Output(src={Path("a")}, dest_dir=Path("b")),
                True,
                id="equal",
            ),
            param(
                data.Output(src={Path("a")}, dest_dir=Path("b")),
                data.Output(src={Path("a")}, dest_dir=Path("c")),
                False,
                id="not_equal",
            ),
            param(
                data.Output(src={Path("a")}, dest_dir=Path("b")),
                data.Output(src={Path("a")}, dest_dir=Path("b"), parent_id="c"),
                False,
                id="not_equal_parent_id",
            ),
        ],
    )
    def test_hash(a, b, expected):
        assert (hash(a) == hash(b)) == expected

    @staticmethod
    @mark.parametrize(
        "src,expected",
        [
            param(["a"], 1, id="1"),
            param(["a", "b"], 2, id="2"),
            param(["a", "a", "b"], 2, id="duplicate"),
        ],
    )
    def test_len(src, expected):
        _output = data.Output(src=src, dest_dir="b")
        assert len(_output) == expected


class Test_Sample:
    @staticmethod
    def test_init():
        _sample = data.Sample(id="a", files=["b"])
        assert _sample.id == "a"
        assert str(_sample) == "a"
        assert _sample.files == ["b"]
        assert _sample.done is None
        assert _sample.output == []

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
        assert _samples == samples

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
    def test_validate(samples, valid_samples):
        samples.remove_invalid()
        assert len(samples) == 0

        valid_samples.remove_invalid()
        assert len(valid_samples) == 2

    @staticmethod
    def test_unique_ids(samples):
        assert samples.unique_ids == {"a", "b"}

    @staticmethod
    def test_complete_failed(samples):
        assert not samples.complete
        assert samples.failed == samples

        samples[0].done = True
        samples[2].done = True
        assert samples.failed == samples[:2]
        assert samples.complete == samples[2:]

        samples[1].done = True
        assert samples.complete == samples
        assert not samples.failed

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
