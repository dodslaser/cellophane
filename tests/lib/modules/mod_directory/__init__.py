from cellophane.src import modules, data


@modules.pre_hook()
def pre_hook_b(*args, **kwargs):
    ...


@modules.post_hook()
def post_hook_b(*args, **kwargs):
    ...


@modules.runner()
def runner_b(*args, **kwargs):
    ...


class SampleMixinB(data.Sample):
    attr_b: str


class SamplesMixinB(data.Samples):
    attr_b: str
