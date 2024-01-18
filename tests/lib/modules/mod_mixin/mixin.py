# type: ignore
# pylint: disable=all

from attrs import define, field

from cellophane.src import data


class SampleMixin_base(data.Sample):
    base: str = "expected_base"


class SamplesMixin_base(data.Samples):
    base: str = "expected_base"


@define(init=False, slots=False)
class SampleMixin_attrs_default(data.Sample):
    attrs_default: str = "expected_attrs_default"


@define(init=False, slots=False, order=False)
class SamplesMixin_attrs_default(data.Samples):
    attrs_default: str = "expected_attrs_default"


@define(init=False, slots=False)
class SampleMixin_attrs_field(data.Sample):
    attrs_field: str = field(default="expected_attrs_field")


@define(init=False, slots=False, order=False)
class SamplesMixin_attrs_field(data.Samples):
    attrs_field: str = field(default="expected_attrs_field")
