from __future__ import annotations

from typing import TYPE_CHECKING, overload

from cellophane.data import OutputGlob

from .hook import ExceptionHook, PostHook, PreHook
from .runner_ import Runner

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, Callable, Literal

    from cellophane.data import Samples
    from cellophane.modules.hook import DEPENDENCY_TYPE
    from cellophane.util import NamedCallable


def output(
    src: str,
    /,
    dst_dir: str | Path | None = None,
    dst_name: str | None = None,
    checkpoint: str = "main",
    optional: bool = False,
) -> Callable:
    """Decorator to mark output files of a runner.

    Files matching the given pattern will be added to the output of the runner.

    Celophane does not handle the copying of the files. Instead, it is expected
    that a post-hook will be used to copy the files to the output directory.

    Args:
    ----
        pattern: A glob pattern to match files to be added to the output.
            The pattern will be formatted with the following variables:
            - `samples`: The samples being processed.
            - `sample`: The current sample being processed.
            - `config`: The configuration object.
            - `runner`: The runner being executed.
            - `workdir`: The working directory
                with tag and the value of the split_by attribute (if any) appended.
        dst_dir: The directory to copy the files to. If not specified, the
            directory of the matched file will be used. If the matched file is
        dst_name: The name to copy the files to. If not specified, the name
            of the matched file will be used.
        checkpoint: The checkpoint to use for the output. Defaults to "main".

    """

    @overload
    def wrapper(func: NamedCallable[..., Samples | None]) -> NamedCallable[..., Samples | None]: ...

    @overload
    def wrapper(func: Runner) -> Runner: ...

    def wrapper(func: NamedCallable | Runner) -> NamedCallable[..., Samples | None] | Runner:
        if isinstance(func, Runner):
            func.main = wrapper(func.main)
            return func

        def inner(
            *args: Any,
            samples: Samples,
            **kwargs: Any,
        ) -> Samples | None:
            glob_ = OutputGlob(
                src=src,
                dst_dir=str(dst_dir) if dst_dir is not None else None,
                dst_name=dst_name,
                checkpoint=checkpoint,
                optional=optional,
            )
            samples.output.add(glob_)
            return func(*args, samples=samples, **kwargs)

        inner.__name__ = func.__name__
        inner.__qualname__ = func.__qualname__
        return inner

    return wrapper


def runner(
    label: str | None = None,
    split_by: str | None = None,
) -> Callable:
    """Decorator for creating a runner.

    Args:
    ----
        label (str | None): The label for the runner. Defaults to None.
        split_by (str | None): The attribute to link samples by. Defaults to None.

    Returns:
    -------
        Callable: The decorator function.

    """

    def wrapper(func: NamedCallable) -> Runner:
        return Runner(
            label=label,
            func=func,
            split_by=split_by,
        )

    return wrapper


def pre_hook(
    label: str | None = None,
    condition: Literal["always", "unprocessed", "failed"] = "unprocessed",
    per: Literal["session", "runner"] = "session",
    before: str | DEPENDENCY_TYPE | None = None,
    after: str | DEPENDENCY_TYPE | None = None,
) -> Callable:
    """Decorator for creating a pre-hook.

    Args:
    ----
        label (str | None): The label for the pre-hook. Defaults to None.
        before (list[str] | Literal["all"] | None): List of pre-hooks guaranteed to
            execute after the resulting pre-hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of pre-hooks guaratneed to
            execute before the resulting pre-hook. Defaults to an empty list.

    Returns:
    -------
        Callable: The decorator function.

    """

    def wrapper(func: NamedCallable) -> PreHook:
        return PreHook(
            label=label,
            func=func,
            condition=condition,
            per=per if per in ("session", "runner") else "session",
            before=before,
            after=after,
        )

    return wrapper


def post_hook(
    label: str | None = None,
    condition: Literal["always", "complete", "failed"] = "always",
    per: Literal["session", "sample", "runner"] = "session",
    before: str | DEPENDENCY_TYPE | None = None,
    after: str | DEPENDENCY_TYPE | None = None,
) -> Callable:
    """Decorator for creating a post-hook.

    Args:
    ----
        label (str | None): The label for the pre-hook. Defaults to None.
        condition (Literal["always", "complete", "failed"]): The condition for
            the post-hook to execute.
            - "always": The post-hook will always execute.
            - "complete": The post-hook will recieve only completed samples.
            - "failed": The post-hook will recieve only failed samples.
            Defaults to "always".
        per (Literal["session", "sample", "runner"]): The level at which the hook
            will be executed.
            - "session": The hook will be executed after all runners are complete.
            - "sample": The hook will be executed when all runners finish processing
                an individual sample.
            - "runner": The hook will be executed upon completion of a single runner.
        before (list[str] | Literal["all"] | None): List of post-hooks guaranteed to
            execute after the resulting pre-hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of post-hooks guaratneed to
            execute before the resulting pre-hook. Defaults to an empty list.

    Returns:
    -------
        Callable: The decorator function.

    """
    if condition not in ["always", "complete", "failed"]:
        raise ValueError(f"{condition=} must be one of 'always', 'complete', 'failed'")

    def wrapper(func: NamedCallable) -> PostHook:
        return PostHook(
            label=label,
            func=func,
            condition=condition,
            per=per if per in ("session", "sample", "runner") else "session",
            before=before,
            after=after,
        )

    return wrapper

def exception_hook(
    label: str | None = None,
    before: str | DEPENDENCY_TYPE | None = None,
    after: str | DEPENDENCY_TYPE | None = None,
) -> Callable:
    """Decorator for creating an exception hook.

    Args:
    ----
        label (str | None): The label for the exception hook. Defaults to None.
        per (Literal["session", "runner"]): The level at which the hook will be executed.
            Defaults to "session".
        before (list[str] | Literal["all"] | None): List of exception hooks guaranteed to
            execute after the resulting exception hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of exception hooks guaranteed to
            execute before the resulting exception hook. Defaults to an empty list.

    Returns:
    -------
        Callable: The decorator function.

    """

    def wrapper(func: NamedCallable) -> ExceptionHook:
        return ExceptionHook(
            label=label,
            func=func,
            before=before,
            after=after,
        )

    return wrapper