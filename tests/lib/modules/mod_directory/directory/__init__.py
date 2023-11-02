from cellophane.src import data, modules


@modules.pre_hook()
def pre_hook_directory(*args, **kwargs):
    ...


@modules.post_hook()
def post_hook_directory(*args, **kwargs):
    ...


@modules.runner()
def runner_directory(*args, **kwargs):
    ...


class SampleMixinDirectory(data.Sample):
    attr_b: str


class SamplesMixinDirectory(data.Samples):
    attr_b: str
