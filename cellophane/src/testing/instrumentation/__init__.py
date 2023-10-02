"""Instrumentation for testing cellophane."""

from uuid import UUID, uuid4

from cellophane import data, modules


class CallIDMixin(data.Sample):
    """Mixin for adding a call ID to a sample."""
    runner: str | None = None
    call_uuid: UUID | None = None


class CallCountMixin(data.Samples):
    """Mixin for counting the number of calls per runner."""
    def with_call_id(self, runner):
        """Update samples with a call ID for a given runner."""
        _uuid = uuid4()
        for s in self:
            s.runner = runner
            s.call_uuid = _uuid
        return self

    @property
    def call_count(self):
        """Return the number of calls per runner."""
        return {
            r: len({s.call_uuid for s in self if s.runner == r})
            for r in {s.runner for s in self}
            if r is not None
        }


@modules.pre_hook(before="all")
def ensure_files(samples, config, **_):
    """Ensure that all samples have files."""
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
    """Set samples as failed if they are in the config."""
    for s in samples:
        if s.id in config.failed_samples:
            s.done = False
    return samples
