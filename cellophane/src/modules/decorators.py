from pathlib import Path
from typing import Any, Callable, Literal

from cellophane.src import data

from .hook import Hook
from .runner_ import Runner


def output(
    src: str,
    /,
    dst_dir: Path | None = None,
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

    def wrapper(func: Callable) -> Callable:
        if isinstance(func, Runner):
            func.main = wrapper(func.main)
            return func

        def inner(
            *args: Any,
            samples: data.Samples,
            **kwargs: Any,
        ) -> data.Samples | None:
            glob_ = data.OutputGlob(
                src=src,
                dst_dir=dst_dir,
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

    def wrapper(func: Callable) -> Runner:
        return Runner(
            label=label,
            func=func,
            split_by=split_by,
        )

    return wrapper


def pre_hook(
    label: str | None = None,
    before: list[str] | Literal["all"] | None = None,
    after: list[str] | Literal["all"] | None = None,
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

    def wrapper(func: Callable) -> Hook:
        return Hook(
            label=label,
            func=func,
            when="pre",
            condition="always",
            before=before or [],
            after=after or [],
        )

    return wrapper


def post_hook(
    label: str | None = None,
    condition: Literal["always", "complete", "failed"] = "always",
    before: list[str] | Literal["all"] | None = None,
    after: list[str] | Literal["all"] | None = None,
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

    def wrapper(func: Callable) -> Hook:
        return Hook(
            label=label,
            func=func,
            when="post",
            condition=condition,
            before=before or [],
            after=after or [],
        )

    return wrapper
