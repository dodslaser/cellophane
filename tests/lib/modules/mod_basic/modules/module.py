# type: ignore
# pylint: disable=all

from cellophane import data, modules


@modules.pre_hook()
def pre_hook_basic(*args, **kwargs):
    pass


@modules.post_hook()
def post_hook_basic(*args, **kwargs):
    pass


@modules.runner()
def runner_basic(*args, **kwargs):
    pass


class SampleMixinBasic(data.Sample):
    attr_a: str


class SamplesMixinBasic(data.Samples):
    attr_a: str
