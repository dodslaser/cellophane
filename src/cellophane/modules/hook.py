from __future__ import annotations

from graphlib import TopologicalSorter
from logging import LoggerAdapter, getLogger
from typing import TYPE_CHECKING

from cellophane.data import Samples

from .checkpoint import Checkpoints

if TYPE_CHECKING:
    from multiprocessing.queues import Queue
    from pathlib import Path
    from typing import Any, Literal, TypeAlias

    from cellophane.cfg import Config
    from cellophane.cleanup import Cleaner, DeferredCleaner
    from cellophane.executors import Executor
    from cellophane.util import NamedCallable, Timestamp


class _AFTER_ALL: ...


class _BEFORE_ALL: ...


DEPENDENCY_TYPE: TypeAlias = list[type[_BEFORE_ALL] | type[_AFTER_ALL] | str]


class _BaseHook:
    """Base class for cellophane hooks."""

    name: str
    label: str
    func: NamedCallable
    when: Literal["pre", "post", "exception"]
    condition: Literal["always", "complete", "unprocessed", "failed"]
    before: DEPENDENCY_TYPE
    after: DEPENDENCY_TYPE
    per: Literal["session", "sample", "runner"] = "session"

    def __init__(
        self,
        func: NamedCallable,
        when: Literal["pre", "post", "exception"],
        label: str | None = None,
        condition: Literal["always", "complete", "unprocessed", "failed"] = "always",
        before: str | DEPENDENCY_TYPE | None = None,
        after: str | DEPENDENCY_TYPE | None = None,
        per: Literal["session", "sample", "runner"] = "session",
    ) -> None:
        if isinstance(before, str) and before != "all":
            before = [before]
        elif before is None:
            before = []

        if isinstance(after, str) and after != "all":
            after = [after]
        elif after is None:
            after = []

        match before, after:
            case "all", list(after):
                self.before = [_BEFORE_ALL]
                self.after = after
            case list(before), "all":
                self.before = before
                self.after = [_AFTER_ALL]
            case list(before), list(after) if "all" in before and "all" not in after:
                self.before = [_BEFORE_ALL, *before]
                self.before.remove("all")
                self.after = after
            case list(before), list(after) if "all" not in before and "all" in after:
                self.before = before
                self.after = [*after, _AFTER_ALL]
                self.after.remove("all")
            case list(before), list(after) if "all" not in before and "all" not in after:
                self.before = [*before, _AFTER_ALL]
                self.after = [*after, _BEFORE_ALL]
            case _:
                raise ValueError(f"{func.__name__}: {before=}, {after=}")
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.__module__ = func.__module__
        self.name = func.__name__
        self.label = label or func.__name__
        self.condition = condition
        self.func = staticmethod(func)
        self.when = when
        self.per = per

    def __call__(
        self,
        config: Config,
        root: Path,
        executor_cls: type[Executor],
        log_queue: Queue,
        timestamp: Timestamp,
        dispatcher: Any,
        **kwargs: Any,
    ) -> Any:
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        logger.debug(f"Running {self.label} hook")
        _workdir = config.workdir / config.tag  # ty: ignore[unsupported-operator]
        with executor_cls(
            config=config,
            log_queue=log_queue,
            workdir_base=_workdir,
            dispatcher=dispatcher,
        ) as executor:
            return self.func(
                config=config,
                timestamp=timestamp,
                logger=logger,
                root=root,
                workdir=_workdir,
                executor=executor,
                **kwargs,
            )


class _PrePostHook(_BaseHook):
    """Cellophane pre/post-hook."""

    when: Literal["pre", "post"]

    def __init__(
        self,
        func: NamedCallable,
        when: Literal["pre", "post"],
        label: str | None = None,
        condition: Literal["always", "complete", "unprocessed", "failed"] = "always",
        before: str | DEPENDENCY_TYPE | None = None,
        after: str | DEPENDENCY_TYPE | None = None,
        per: Literal["session", "sample", "runner"] = "session",
    ) -> None:
        super().__init__(
            func,
            when=when,
            label=label,
            condition=condition,
            before=before,
            after=after,
            per=per,
        )

    def __call__(  # type: ignore[override]
        self,
        samples: Samples,
        config: Config,
        root: Path,
        executor_cls: type[Executor],
        log_queue: Queue,
        timestamp: Timestamp,
        cleaner: Cleaner | DeferredCleaner,
        checkpoints: Checkpoints,
        dispatcher: Any,
    ) -> Samples:
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        match super().__call__(
            samples=samples,
            config=config,
            root=root,
            executor_cls=executor_cls,
            log_queue=log_queue,
            timestamp=timestamp,
            cleaner=cleaner,
            checkpoints=checkpoints,
            dispatcher=dispatcher,
        ):
            case returned if isinstance(returned, Samples):
                _ret = returned
            case None:
                logger.debug("Hook did not return any samples")
                _ret = samples
            case returned:
                logger.warning(f"Unexpected return type {type(returned)}")
                _ret = samples
        return _ret


class PreHook(_PrePostHook):
    """Cellophane pre-hook."""

    when: Literal["pre"]

    def __init__(
        self,
        func: NamedCallable,
        label: str | None = None,
        condition: Literal["always", "unprocessed", "failed"] = "unprocessed",
        before: str | DEPENDENCY_TYPE | None = None,
        after: str | DEPENDENCY_TYPE | None = None,
        per: Literal["session", "sample", "runner"] = "session",
    ) -> None:
        super().__init__(
            func,
            when="pre",
            label=label,
            condition=condition,
            before=before,
            after=after,
            per=per,
        )


class PostHook(_PrePostHook):
    """Cellophane post-hook."""

    when: Literal["post"]

    def __init__(
        self,
        func: NamedCallable,
        label: str | None = None,
        condition: Literal["always", "complete", "failed"] = "always",
        before: str | DEPENDENCY_TYPE | None = None,
        after: str | DEPENDENCY_TYPE | None = None,
        per: Literal["session", "sample", "runner"] = "session",
    ) -> None:
        super().__init__(
            func,
            when="post",
            label=label,
            condition=condition,
            before=before,
            after=after,
            per=per,
        )


class ExceptionHook(_BaseHook):
    """Cellophane exception-hook."""

    when: Literal["exception"]
    condition: Literal["always"]

    def __init__(
        self,
        func: NamedCallable,
        label: str | None = None,
        before: str | DEPENDENCY_TYPE | None = None,
        after: str | DEPENDENCY_TYPE | None = None,
    ) -> None:
        super().__init__(
            func,
            when="exception",
            label=label,
            condition="always",
            before=before,
            after=after,
            per="session",
        )

    def __call__(  # type: ignore[override]
        self,
        exception: BaseException,
        config: Config,
        root: Path,
        executor_cls: type[Executor],
        log_queue: Queue,
        timestamp: Timestamp,
        dispatcher: Any,
    ) -> Any:
        super().__call__(
            exception=exception,
            config=config,
            root=root,
            executor_cls=executor_cls,
            log_queue=log_queue,
            timestamp=timestamp,
            dispatcher=dispatcher,
        )


def resolve_dependencies(
    hooks: list[PreHook | PostHook | ExceptionHook],
) -> list[PreHook | PostHook | ExceptionHook]:
    """Resolves hook dependencies and returns the hooks in the resolved order.
    Uses a topological sort to resolve dependencies. If the order of two hooks
    cannot be determined, the order is not guaranteed.

    # FIXME: It should be possible to determine the order of all hooks

    Args:
    ----
        hooks (list[Hook]): The list of hooks to resolve.

    Returns:
    -------
        list[Hook]: The hooks in the resolved order.

    """
    deps = {
        name: {
            *[d for h in hooks if h.__name__ == name for d in h.after],
            *[h.__name__ for h in hooks if name in h.before],
        }
        for name in {
            *[n for h in hooks for n in h.before + h.after],
            *[h.__name__ for h in hooks],
        }
    }

    order = [*TopologicalSorter(deps).static_order()]
    return [*sorted(hooks, key=lambda h: order.index(h.__name__))]
