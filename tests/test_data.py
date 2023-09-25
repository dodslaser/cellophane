from cellophane.src import data
from pytest import fixture, raises, param, mark
from attrs import define, field
from copy import deepcopy
from pathlib import Path
import pickle

LIB = Path(__file__).parent / "lib"


@define(init=False, slots=False)
class Dummy(data.Container):
    a: int = field(default=1337)

    @a.validator
    def _validate_a(self, attribute, value):
        if isinstance(value, int) and value > 9000:
            raise ValueError("It's over 9000!")


class Test_Container:
    @staticmethod
    def test_init():
        _container = data.Container(a=1337)
        assert _container.data == {"a": 1337}

    @staticmethod
    def test_setitem_getitem():
        _container = data.Container()
        _container["a"] = 1337
        _container["b", "c"] = 1338
        assert _container["a"] == 1337
        assert _container["b", "c"] == 1338
        assert _container.as_dict == {"a": 1337, "b": {"c": 1338}}

        with raises(TypeError):
            _container[1337] = 1337

        with raises(TypeError):
            _container[1337]

    @staticmethod
    def test_setattr_getattr():
        _container = data.Container()
        _container.a = 1337
        _container.b = {"c": 1338}
        assert _container.a == 1337
        assert _container.b.c == 1338
        assert _container.as_dict == {"a": 1337, "b": {"c": 1338}}

    @staticmethod
    def test_views() -> None:
        _dummy = Dummy(b=1338)  # type: ignore[call-arg]

        assert _dummy.data == {"b": 1338}
        assert _dummy.a == 1337
        assert _dummy.b == 1338
        assert _dummy.as_dict == {"a": 1337, "b": 1338}
        assert [*_dummy.keys()] == ["a", "b"]
        assert [*_dummy.values()] == [1337, 1338]
        assert [*_dummy.items()] == [("a", 1337), ("b", 1338)]

        with raises(ValueError):
            _dummy["a"] = 9001
        with raises(ValueError):
            _dummy.a = 9001

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
        _pickle = pickle.dumps(_sample)
        assert pickle.loads(_pickle) == _sample


class Test_Samples:
    @staticmethod
    @fixture(scope="function")
    def samples():
        return data.Samples(
            [
                data.Sample(
                    id="a",
                    files=["a", "b"],
                    meta="x",
                ),  # type: ignore[call-arg]
                data.Sample(
                    id="a",
                    files=["c", "d"],
                    meta="y",
                ),  # type: ignore[call-arg]
                data.Sample(
                    id="b",
                    files=["e", "f"],
                    meta="y",
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
            param("meta", [(0,), (1, 2)], id="by_meta"),
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
