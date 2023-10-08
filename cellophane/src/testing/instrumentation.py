"""Instrumentation for testing cellophane."""

from uuid import UUID, uuid4

from cellophane import data


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
