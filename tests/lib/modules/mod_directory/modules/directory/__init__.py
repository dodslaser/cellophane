# type: ignore
# pylint: disable=all

from cellophane import data, modules


@modules.pre_hook()
def pre_hook_directory(*args, **kwargs):
    pass


@modules.post_hook()
def post_hook_directory(*args, **kwargs):
    pass


@modules.runner()
def runner_directory(*args, **kwargs):
    pass


class SampleMixinDirectory(data.Sample):
    attr_b: str


class SamplesMixinDirectory(data.Samples):
    attr_b: str
