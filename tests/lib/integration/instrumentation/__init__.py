from uuid import uuid4, UUID
from cellophane import data, modules


class CallIDMixin(data.Sample):
    runner: str | None = None
    call_uuid: UUID | None = None


class CallCountMixin(data.Samples):
    def with_call_id(self, runner):
        _uuid = uuid4()
        for s in self:
            s.runner = runner
            s.call_uuid = _uuid
        return self

    @property
    def call_count(self):
        return {
            r: len({s.call_uuid for s in self if s.runner == r})
            for r in {s.runner for s in self}
            if r is not None
        }


@modules.pre_hook(before="all")
def ensure_files(samples, config, **_):
    (config.outdir / "inputs").mkdir(parents=True, exist_ok=True)
    for sample in samples:
        sample.files = [
            (config.outdir / "inputs" / f) for s in samples for f in s.files
        ]
        for file in sample.files:
            file.touch(exist_ok=True)

    return samples


@modules.runner()
def set_failed(samples, config, **_):
    for s in samples:
        if s.id in config.failed_samples:
            s.done = False
    return samples
