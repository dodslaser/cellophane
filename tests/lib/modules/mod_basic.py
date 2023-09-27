from cellophane.src import modules, data


@modules.pre_hook()
def pre_hook_a(*args, **kwargs):
    ...


@modules.post_hook()
def post_hook_a(*args, **kwargs):
    ...


@modules.runner()
def runner_a(*args, **kwargs):
    ...


class SampleMixinA(data.Sample):
    attr_a: str


class SamplesMixinA(data.Samples):
    attr_a: str
