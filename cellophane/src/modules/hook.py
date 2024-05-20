from copy import deepcopy
from graphlib import TopologicalSorter
from logging import LoggerAdapter, getLogger
from multiprocessing import Queue
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

from mpire import WorkerPool

from cellophane.src import cfg, data, executors
from cellophane.src.cleanup import Cleaner


class Hook:
    """Base class for cellophane pre/post-hooks."""

    name: str
    label: str
    func: Callable
    when: Literal["pre", "post"]
    condition: Literal["always", "complete", "failed"]
    before: list[str]
    after: list[str]

    def __init__(
        self,
        func: Callable,
        when: Literal["pre", "post"],
        label: str | None = None,
        condition: Literal["always", "complete", "failed"] = "always",
        before: str | list[str] | None = None,
        after: str | list[str] | None = None,
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
                self.before = ["before_all"]
                self.after = after
            case list(before), "all":
                self.before = before
                self.after = ["after_all"]
            case list(before), list(after):
                self.before = [*before, "after_all"]
                self.after = [*after, "before_all"]
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

    def __call__(
        self,
        samples: data.Samples,
        config: cfg.Config,
        root: Path,
        executor_cls: type[executors.Executor],
        log_queue: Queue,
        timestamp: str,
        cleaner: Cleaner,
    ) -> data.Samples:
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        logger.debug(f"Running {self.label} hook")

        with WorkerPool(
            use_dill=True,
            daemon=False,
        ) as pool:
            match self.func(
                samples=samples,
                config=config,
                timestamp=timestamp,
                logger=logger,
                root=root,
                workdir=config.workdir / config.tag,
                executor=executor_cls(
                    config=config,
                    pool=pool,
                    log_queue=log_queue,
                ),
                cleaner=cleaner,
            ):
                case returned if isinstance(returned, data.Samples):
                    _ret = returned
                case None:
                    logger.debug("Hook did not return any samples")
                    _ret = samples
                case returned:
                    logger.warning(f"Unexpected return type {type(returned)}")
                    _ret = samples
            pool.stop_and_join()
            return _ret

def resolve_dependencies(
    hooks: list[Hook],
) -> list[Hook]:
    """
    Resolves hook dependencies and returns the hooks in the resolved order.
    Uses a topological sort to resolve dependencies. If the order of two hooks
    cannot be determined, the order is not guaranteed.

    # FIXME: It should be possible to determine the order of all hooks

    Args:
        hooks (list[Hook]): The list of hooks to resolve.

    Returns:
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


def run_hooks(
    hooks: Sequence[Hook],
    *,
    when: Literal["pre", "post"],
    samples: data.Samples,
    config: cfg.Config,
    root: Path,
    executor_cls: type[executors.Executor],
    log_queue: Queue,
    timestamp: str,
    cleaner: Cleaner,
) -> data.Samples:
    """
    Run hooks at the specified time and update the samples object.

    Args:
        hooks (Sequence[Hook]): The hooks to run.
        when (Literal["pre", "post"]): The time to run the hooks.
        samples (data.Samples): The samples object to update.
        **kwargs (Any): Additional keyword arguments to pass to the hooks.

    Returns:
        data.Samples: The updated samples object.
    """
    samples = deepcopy(samples)

    for hook in [h for h in hooks if h.when == when]:
        if hook.when == "pre" or hook.condition == "always":
            samples = hook(
                samples=samples,
                config=config,
                root=root,
                executor_cls=executor_cls,
                log_queue=log_queue,
                timestamp=timestamp,
                cleaner=cleaner,
            )
        elif hook.condition == "complete" and (s := samples.complete):
            samples = (
                hook(
                    samples=s,
                    config=config,
                    root=root,
                    executor_cls=executor_cls,
                    log_queue=log_queue,
                    timestamp=timestamp,
                    cleaner=cleaner,
                )
                | samples.failed
            )
        elif hook.condition == "failed" and (s := samples.failed):
            samples = (
                hook(
                    samples=s,
                    config=config,
                    root=root,
                    executor_cls=executor_cls,
                    log_queue=log_queue,
                    timestamp=timestamp,
                    cleaner=cleaner,
                )
                | samples.complete
            )

    return samples
