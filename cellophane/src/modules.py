"""Base classes and functions for cellophane modules."""
import logging
import sys
from copy import deepcopy
from graphlib import TopologicalSorter
from importlib.util import module_from_spec, spec_from_file_location
from multiprocessing import Queue
from pathlib import Path
from signal import SIGTERM, signal
from typing import Any, Callable, Literal

import psutil
from cloudpickle import dumps, loads
from mpire import WorkerPool

from . import cfg, data, executors, logs


def _cleanup(logger: logging.LoggerAdapter) -> Callable:
    def inner(*args: Any) -> None:
        del args  # Unused
        for proc in psutil.Process().children(recursive=True):
            try:
                logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
                proc.terminate()
                proc.wait(10)
            except psutil.TimeoutExpired:
                logger.warning(
                    f"Killing unresponsive process {proc.name()} ({proc.pid})"
                )
                proc.kill()
                proc.wait()
        raise SystemExit(1)

    return inner


def _is_instance_or_subclass(obj: Any, cls: type) -> bool:
    if isinstance(obj, type):
        return issubclass(obj, cls) and obj != cls
    else:
        return isinstance(obj, cls)


class Runner:
    """
    A runner for executing a function as a job.

    Args:
        func (Callable): The function to be executed as a job.
        label (str | None): The label for the runner.
            Defaults to the name of the function.
        split_by (str | None): The attribute to split samples by.
    """

    label: str
    split_by: str | None
    func: Callable
    wait: bool
    main: Callable[..., data.Samples | None]
    done: bool = False

    def __init__(
        self,
        func: Callable,
        label: str | None = None,
        split_by: str | None = None,
    ) -> None:
        self.__name__ = func.__name__
        self.__qualname__ = func.__qualname__
        self.name = func.__name__
        self.label = label or func.__name__
        self.main = staticmethod(func)
        self.label = label or self.__name__
        self.split_by = split_by
        super().__init_subclass__()

    def __call__(
        self,
        log_queue: Queue,
        /,
        config: cfg.Config,
        root: Path,
        samples_pickle: str,
        executor_cls: type[executors.Executor],
    ) -> bytes:
        samples: data.Samples = loads(samples_pickle)
        logs.setup_queue_logging(log_queue)
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": self.label})

        signal(SIGTERM, _cleanup(logger))
        workdir = config.workdir / config.tag / self.label
        if self.split_by:
            workdir /= samples[0][self.split_by]

        workdir.mkdir(parents=True, exist_ok=True)

        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            try:
                match self.main(
                    samples=deepcopy(samples),
                    config=config,
                    timestamp=config.timestamp,
                    label=self.label,
                    logger=logger,
                    root=root,
                    workdir=workdir,
                    executor=executor_cls(
                        config=config,
                        pool=pool,
                        log_queue=log_queue,
                    ),
                ):
                    case None:
                        logger.debug("Runner did not return any samples")

                    case returned if isinstance(returned, data.Samples):
                        samples = returned

                    case returned:
                        logger.warning(f"Unexpected return type {type(returned)}")

                for sample in samples:
                    sample.processed = True

            except Exception as exc:  # pylint: disable=broad-except
                samples.output = set()
                for sample in samples:
                    sample.fail(str(exc))

            finally:
                if samples.complete:
                    for output_ in samples.output.copy():
                        if isinstance(output_, data.OutputGlob):
                            samples.output.remove(output_)
                            samples.output |= output_.resolve(
                                samples=samples.complete,
                                workdir=workdir,
                                config=config,
                                logger=logger,
                            )
                for sample in samples.complete:
                    logger.debug(f"Sample {sample.id} processed successfully")
                if n_failed := len(samples.failed):
                    logger.error(f"{n_failed} samples failed")
                for sample in samples.failed:
                    logger.debug(f"Sample {sample.id} failed - {sample.failed}")

            pool.stop_and_join()
            return dumps(samples)


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
        before: Literal["all"] | list[str] | None = None,
        after: Literal["all"] | list[str] | None = None,
    ) -> None:
        before = before or []
        after = after or []
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
    ) -> data.Samples:
        logger = logging.LoggerAdapter(logging.getLogger(), {"label": self.label})
        logger.debug(f"Running {self.label} hook")

        with WorkerPool(
            use_dill=True,
            daemon=False,
        ) as pool:
            match self.func(
                samples=samples,
                config=config,
                timestamp=config.timestamp,
                logger=logger,
                root=root,
                workdir=config.workdir / config.tag,
                log_queue=log_queue,
                executor=executor_cls(
                    config=config,
                    pool=pool,
                    log_queue=log_queue,
                ),
            ):
                case returned if isinstance(returned, data.Samples):
                    _ret = returned
                case None:
                    logger.debug("Hook did not return any samples")
                    _ret = samples
                case _:
                    logger.warning(f"Unexpected return type {type(returned)}")
                    _ret = samples
            pool.stop_and_join()
            return _ret


def _resolve_hook_dependencies(
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


def load(
    path: Path,
) -> tuple[
    list[Hook],
    list[Runner],
    list[type[data.Sample]],
    list[type[data.Samples]],
    list[type[executors.Executor]],
]:
    """
    Loads module(s) from the specified path and returns the hooks, runners,
    sample mixins, and samples mixins found within.

    Args:
        path (Path): The path to the directory containing the modules.

    Returns:
        tuple[
            list[Hook],
            list[Runner],
            list[type[data.Sample]],
            list[type[data.Samples]],
        ]: A tuple containing the lists of hooks, runners, sample mixins,
            and samples mixins.
    """

    hooks: list[Hook] = []
    runners: list[Runner] = []
    sample_mixins: list[type[data.Sample]] = []
    samples_mixins: list[type[data.Samples]] = []
    executors_: list[type[executors.Executor]] = [executors.SubprocesExecutor]

    for file in [*path.glob("*.py"), *path.glob("*/__init__.py")]:
        base = file.stem if file.stem != "__init__" else file.parent.name
        name = f"_cellophane_module_{base}"
        spec = spec_from_file_location(name, file)
        original_handlers = logging.root.handlers.copy()

        try:
            module = module_from_spec(spec)  # type: ignore[arg-type]
            sys.modules[name] = module
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except Exception as exc:
            raise ImportError(f"Unable to import module '{base}': {exc}") from exc

        # Reset logging handlers to avoid duplicate messages
        for handler in {*logging.root.handlers} ^ {*original_handlers}:
            handler.close()
            logging.root.removeHandler(handler)

        for obj in [getattr(module, a) for a in dir(module)]:
            if _is_instance_or_subclass(obj, Hook):
                hooks.append(obj)
            elif _is_instance_or_subclass(obj, data.Sample):
                sample_mixins.append(obj)
            elif _is_instance_or_subclass(obj, data.Samples):
                samples_mixins.append(obj)
            elif _is_instance_or_subclass(obj, Runner):
                runners.append(obj)
            elif _is_instance_or_subclass(obj, executors.Executor):
                executors_.append(obj)
    try:
        hooks = _resolve_hook_dependencies(hooks)
    except Exception as exc:  # pylint: disable=broad-except
        raise ImportError(f"Unable to resolve hook dependencies: {exc}") from exc

    return hooks, runners, sample_mixins, samples_mixins, executors_


def pre_hook(
    label: str | None = None,
    before: list[str] | Literal["all"] | None = None,
    after: list[str] | Literal["all"] | None = None,
) -> Callable:
    """
    Decorator for creating a pre-hook.

    Args:
        label (str | None): The label for the pre-hook. Defaults to None.
        before (list[str] | Literal["all"] | None): List of pre-hooks guaranteed to
            execute after the resulting pre-hook. Defaults to an empty list.
        after (list[str] | Literal["all"] | None): List of pre-hooks guaratneed to
            execute before the resulting pre-hook. Defaults to an empty list.

    Returns:
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
    """
    Decorator for creating a post-hook.

    Args:
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


def runner(
    label: str | None = None,
    split_by: str | None = None,
) -> Callable:
    """
    Decorator for creating a runner.

    Args:
        label (str | None): The label for the runner. Defaults to None.
        split_by (str | None): The attribute to link samples by. Defaults to None.

    Returns:
        Callable: The decorator function.
    """

    def wrapper(func: Callable) -> Runner:
        return Runner(
            label=label,
            func=func,
            split_by=split_by,
        )

    return wrapper


def output(
    src: str,
    /,
    dst_dir: Path | None = None,
    dst_name: str | None = None,
) -> Callable:
    """
    Decorator to mark output files of a runner.

    Files matching the given pattern will be added to the output of the runner.

    Celophane does not handle the copying of the files. Instead, it is expected
    that a post-hook will be used to copy the files to the output directory.

    Args:
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
    """

    def wrapper(func: Callable) -> Callable:
        if isinstance(func, Runner):
            func.main = wrapper(func.main)
            return func

        def inner(*args: Any, samples: data.Samples, **kwargs: Any) -> data.Samples | None:
            glob_ = data.OutputGlob(src=src, dst_dir=dst_dir, dst_name=dst_name)
            samples.output.add(glob_)
            return func(*args, samples=samples, **kwargs)
        inner.__name__ = func.__name__
        inner.__qualname__ = func.__qualname__
        return inner

    return wrapper
