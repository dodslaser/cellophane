from copy import deepcopy
from functools import partial
from logging import LoggerAdapter, getLogger
from multiprocessing import Queue
from pathlib import Path
from typing import Callable, Literal, Sequence

from graphlib import TopologicalSorter

from cellophane.cfg import Config
from cellophane.cleanup import Cleaner
from cellophane.data import Samples
from cellophane.executors import Executor


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
            case list(before), list(after) if "all" in before and "all" not in after:
                self.before = ["before_all", *before]
                self.before.remove("all")
                self.after = after
            case list(before), list(after) if "all" not in before and "all" in after:
                self.before = before
                self.after = [*after, "after_all"]
                self.after.remove("all")
            case list(before), list(after) if "all" not in before and "all" not in after:
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
        samples: Samples,
        config: Config,
        root: Path,
        executor_cls: type[Executor],
        log_queue: Queue,
        timestamp: str,
        cleaner: Cleaner,
    ) -> Samples:
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        logger.debug(f"Running {self.label} hook")

        with executor_cls(
            config=config,
            log_queue=log_queue,
        ) as executor:
            match self.func(
                samples=samples,
                config=config,
                timestamp=timestamp,
                logger=logger,
                root=root,
                workdir=config.workdir / config.tag,
                executor=executor,
                cleaner=cleaner,
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


def resolve_dependencies(
    hooks: list[Hook],
) -> list[Hook]:
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


def run_hooks(
    hooks: Sequence[Hook],
    *,
    when: Literal["pre", "post"],
    samples: Samples,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    log_queue: Queue,
    timestamp: str,
    cleaner: Cleaner,
    logger: LoggerAdapter,
) -> Samples:
    """Run hooks at the specified time and update the samples object.

    Args:
    ----
        hooks (Sequence[Hook]): The hooks to run.
        when (Literal["pre", "post"]): The time to run the hooks.
        samples (data.Samples): The samples object to update.
        **kwargs (Any): Additional keyword arguments to pass to the hooks.

    Returns:
    -------
        data.Samples: The updated samples object.

    """
    samples_ = deepcopy(samples)

    for hook in [h for h in hooks if h.when == when]:
        hook_ = partial(
            hook,
            config=config,
            root=root,
            executor_cls=executor_cls,
            log_queue=log_queue,
            timestamp=timestamp,
            cleaner=cleaner,
        )
        if hook.when == "pre":
            # Catch exceptions to allow post-hooks to run even if a pre-hook fails
            try:
                samples_ = hook_(samples=samples_)
            except KeyboardInterrupt:
                logger.warning("Keyboard interrupt received, failing samples and stopping execution")
                for sample in samples_:
                    sample.fail(f"Hook {hook.name} interrupted")
                break
            except BaseException as exc:
                logger.error(f"Exception in {hook.label}: {exc}")
                for sample in samples_:
                    sample.fail(f"Hook {hook.name} failed: {exc}")
                break
        elif hook.condition == "always":
            samples_ = hook_(samples=samples_)
        elif hook.condition == "complete" and (s := samples.complete):
            samples_ = hook_(samples=s) | samples_.failed
        elif hook.condition == "failed" and (s := samples.failed):
            samples_ = hook_(samples=s) | samples_.complete

    return samples_
