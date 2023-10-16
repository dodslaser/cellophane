from cellophane.src import modules, data


@modules.pre_hook()
def pre_hook_basic(*args, **kwargs):
    ...


@modules.post_hook()
def post_hook_basic(*args, **kwargs):
    ...


@modules.runner()
def runner_basic(*args, **kwargs):
    ...


class SampleMixinBasic(data.Sample):
    attr_a: str


class SamplesMixinBasic(data.Samples):
    attr_a: str
