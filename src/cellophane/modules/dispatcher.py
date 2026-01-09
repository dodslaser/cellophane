from __future__ import annotations

from copy import deepcopy
from functools import partial, wraps
from logging import LoggerAdapter, getLogger
from multiprocessing import Lock
from typing import TYPE_CHECKING, overload

from mpire.exception import InterruptWorker
from mpire.pool import WorkerPool

from cellophane.logs import handle_warnings, redirect_logging_to_queue

from .checkpoint import Checkpoints
from .hook import ExceptionHook, PostHook, PreHook

if TYPE_CHECKING:
    from multiprocessing import Queue
    from multiprocessing.synchronize import Lock as LockType
    from pathlib import Path
    from typing import Any, Callable, Literal, Sequence
    from uuid import UUID

    from cellophane.cfg import Config
    from cellophane.cleanup import Cleaner, DeferredCleaner
    from cellophane.data import Samples
    from cellophane.executors.executor import Executor
    from cellophane.modules import Runner
    from cellophane.util import Timestamp


def _poolable(func: Callable) -> Callable:
    """Decorator to adapt a function to be run in a cellophane worker pool.

    All worker pools in cellophane are expected to have a shared reference to a logging queue,
    so this decorator handles the logging redirection and warning handling automatically.
    """

    @wraps(func)
    def inner(log_queue: Queue, /, log_label: str, dispatcher: "Dispatcher", **kwargs: Any) -> object:
        handle_warnings()
        redirect_logging_to_queue(log_queue)
        dispatcher._log_queue = log_queue
        logger = LoggerAdapter(getLogger(), {"label": log_label})
        return func(logger=logger, **kwargs, log_queue=log_queue, dispatcher=dispatcher)

    return inner

def _hook_error_callback(exc: BaseException, logger: LoggerAdapter) -> None:
    logger.error(f"Unhandled exception when submitting hook to pool: {exc!r}", exc_info=exc)

def _hook_callback(
    result: tuple[Samples, DeferredCleaner],
    *,
    logger: LoggerAdapter,
    samples: Samples,
    cleaner: Cleaner,
    pool: WorkerPool | None = None,
    dispatcher: "Dispatcher",
) -> None:
    exception = None
    with dispatcher.cleaner_lock:
        try:
            cleaner &= result[1]
        except Exception as exc:
            logger.error(f"Unhandled exception when merging cleaners: {exc!r}", exc_info=exc)
            exception = exc

    with dispatcher.samples_lock:
        try:
            samples &= result[0]
        except Exception as exc:
            logger.error(f"Unhandled exception when merging samples: {exc!r}", exc_info=exc)
            exception = exc

    if exception is not None:
        dispatcher.run_exception_hooks(exception=exception, pool=pool)


def _runner_error_callback(
    exc: BaseException, runner_lock: LockType, logger: LoggerAdapter) -> None:
    logger.error(f"Unhandled exception when submitting runner to pool: {exc!r}", exc_info=exc)
    runner_lock.release()

def _runner_callback(
    result: tuple[Samples, DeferredCleaner],
    *,
    logger: LoggerAdapter,
    samples: Samples,
    cleaner: Cleaner,
    sample_runner_count: dict[UUID, int],
    runner_lock: LockType,
    dispatcher: "Dispatcher",
    pool: WorkerPool,
) -> None:
    exception = None
    try:
        with dispatcher.cleaner_lock:
            try:
                cleaner &= result[1]
            except Exception as exc:
                exception = exc
                logger.error(f"Unhandled exception when merging cleaners: {exc!r}", exc_info=exc)

        try:
            if len(samples) == 0:
                # No samples have been returned yet, so do an in-place copy of the result samples into 'samples'
                samples ^= result[0]
            else:
                samples &= result[0]
        except Exception as exc:
            exception = exc
            logger.error(f"Unhandled exception when merging samples: {exc!r}", exc_info=exc)
            for sample in result[0]:
                if sample.uuid not in samples:
                    samples.append(sample)
                samples[sample.uuid].fail(repr(exc))

        try:
            for uuid, sample in ((u, s) for u, s in samples.split() if u in result[0]):
                with dispatcher.samples_lock:
                    sample_runner_count[uuid] -= 1
                if sample_runner_count[uuid] == 0:
                    dispatcher.run_post_hooks(
                        per="sample",
                        samples=sample,
                        cleaner=cleaner,
                        pool=pool,
                    )
        except Exception as exc:
            exception = exc
            logger.error(f"Unhandled exception in runner callback: {exc!r}", exc_info=exc)

        if exception is not None:
            dispatcher.run_exception_hooks(exception=exception, pool=pool)

    finally:
        runner_lock.release()


def _run_pre_post_hooks(
    hooks: Sequence[PreHook | PostHook | ExceptionHook],
    *,
    when: Literal["pre", "post"],
    per: Literal["session", "sample", "runner"],
    samples: Samples,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    log_queue: Queue,
    timestamp: Timestamp,
    checkpoint_suffix: str | None = None,
    cleaner: Cleaner | DeferredCleaner,
    logger: LoggerAdapter,
    dispatcher: "Dispatcher",
) -> Samples:
    samples_ = deepcopy(samples)

    for hook in [h for h in hooks if isinstance(h, (PreHook, PostHook)) and (h.when, h.per) == (when, per)]:
        match hook.condition:
            case "always" if not (hook_samples := samples_):
                hook_samples = samples_
            case "unprocessed" if len(samples_) == 0:
                hook_samples = samples_
            case "unprocessed" if not (hook_samples := samples_.unprocessed):
                continue
            case "complete" if not (hook_samples := samples_.complete):
                continue
            case "failed" if not (hook_samples := samples_.failed):
                continue

        checkpoints = Checkpoints(
            samples=hook_samples,
            prefix=(
                f"{hook.when}-hook.{hook.name}"
                if checkpoint_suffix is None
                else f"{hook.when}-hook.{hook.name}.{checkpoint_suffix}"
            ),
            workdir=config.workdir / config.tag,  # ty: ignore[unsupported-operator]
            config=config,
        )
        try:
            samples_ |= hook(
                samples=hook_samples,
                config=config,
                root=root,
                executor_cls=executor_cls,
                log_queue=log_queue,
                timestamp=timestamp,
                cleaner=cleaner,
                checkpoints=checkpoints,
                dispatcher=dispatcher,
            )
        except (KeyboardInterrupt, InterruptWorker):
            logger.warning("Keyboard interrupt received, failing samples and stopping execution")
            for sample in samples_:
                sample.fail(f"{when.capitalize()} hook {hook.label} interrupted")
        except BaseException as exc:
            logger.error(f"Unhandled exception in {when} hook '{hook.label}': {exc!r}")
            dispatcher.run_exception_hooks(exception=exc)
            for sample in samples_:
                sample.fail(f"Hook {hook.name} failed: {exc}")

    return samples_


def _run_exception_hooks(
    hooks: Sequence[PreHook | PostHook | ExceptionHook],
    *,
    exception: BaseException,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    log_queue: Queue,
    timestamp: Timestamp,
    logger: LoggerAdapter,
    dispatcher: "Dispatcher",
) -> None:
    for hook in [h for h in hooks if isinstance(h, ExceptionHook)]:
        try:
            hook(
                exception=exception,
                config=config,
                root=root,
                executor_cls=executor_cls,
                log_queue=log_queue,
                timestamp=timestamp,
                dispatcher=dispatcher,
            )
        except Exception as exc:
            logger.error(f"Unhandled exception in exception hook '{hook.label}': {exc!r}", exc_info=True)


def _start_runners(
    runners: Sequence[Runner],
    *,
    samples: Samples,
    logger: LoggerAdapter,
    log_queue: Queue,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    timestamp: Timestamp,
    cleaner: Cleaner,
    dispatcher: "Dispatcher",
) -> Samples:
    """Start cellphane runners in parallel and collect the results.

    Args:
    ----
        runners (Sequence[Runner]): The runners to execute.
        samples (data.Samples): The samples to process.
        logger (LoggerAdapter): The logger.
        log_queue (Queue): The queue for logging.
        kwargs (Any): Additional keyword arguments to pass to the runners.

    Returns:
    -------
        data.Samples: The samples after processing.

    """
    if not samples:
        logger.warning("No samples to process")
        return samples

    if not runners:
        logger.warning("No runners to execute")
        for sample in samples:
            sample.fail("Sample was not processed")
        return samples

    result_samples = samples.__class__()
    sample_runner_count: dict[UUID, int] = {sample.uuid: 0 for sample in samples}
    runner_locks: list[LockType] = []

    with WorkerPool(
        use_dill=True,
        daemon=False,
        start_method="fork",
        shared_objects=log_queue,
    ) as pool:
        try:
            for runner_, group, samples_ in (
                (r, g, s)
                for r in runners
                for g, s in (samples.split(by=r.split_by) if r.split_by else [(None, samples)])
            ):
                workdir = config.workdir / config.tag / runner_.name  # ty: ignore[unsupported-operator]
                if runner_.split_by is not None:
                    workdir /= str(group or "unknown")
                runner_lock = Lock()
                runner_lock.acquire()
                runner_locks.append(runner_lock)
                for sample in samples_:
                    sample_runner_count[sample.uuid] += 1

                pool.apply_async(
                    runner_,
                    kwargs={
                        "config": config,
                        "root": root,
                        "samples": samples_,
                        "executor_cls": executor_cls,
                        "timestamp": timestamp,
                        "workdir": workdir,
                        "group": group,
                        "dispatcher": dispatcher,
                    },
                    callback=partial(
                        _runner_callback,
                        logger=logger,
                        samples=result_samples,
                        sample_runner_count=sample_runner_count,
                        cleaner=cleaner,
                        runner_lock=runner_lock,
                        dispatcher=dispatcher,
                        pool=pool,
                    ),
                    error_callback=partial(
                        _runner_error_callback,
                        runner_lock=runner_lock,
                        logger=logger,
                    ),
                )
            for lock in runner_locks:
                lock.acquire()
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling runners to shut down...")
            pool.terminate()

        except BaseException as exc:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception when starting runners: {exc!r}", exc_info=exc)
            dispatcher.run_exception_hooks(exception=exc)
            pool.terminate()
        finally:
            pool.stop_and_join()

    return result_samples if len(result_samples) > 0 else samples


class Dispatcher:
    """Convienience class to dispatch hooks, optionally in a separate process."""
    _common_kwargs: dict[str, Any]
    _hooks: Sequence[PreHook | PostHook | ExceptionHook]
    _runners: Sequence[Runner]
    _logger: LoggerAdapter
    _log_queue: Queue
    _samples_lock: LockType | None
    _cleaner_lock: LockType | None

    def __init__(
        self,
        hooks: Sequence[PreHook | PostHook | ExceptionHook],
        runners: Sequence[Runner],
        config: Config,
        root: Path,
        executor_cls: type[Executor],
        log_queue: Queue,
        timestamp: Timestamp,
        logger: LoggerAdapter,
    ) -> None:
        self._common_kwargs = {
            "config": config,
            "root": root,
            "executor_cls": executor_cls,
            "timestamp": timestamp,
        }
        self._hooks = hooks
        self._runners = runners
        self._logger = logger
        self._log_queue = log_queue
        self._samples_lock = None
        self._cleaner_lock = None

    @property
    def samples_lock(self) -> LockType:
        if self._samples_lock is None:
            self._samples_lock = Lock()
        return self._samples_lock

    @property
    def cleaner_lock(self) -> LockType:
        if self._cleaner_lock is None:
            self._cleaner_lock = Lock()
        return self._cleaner_lock

    def __getstate__(self):
        return self.__dict__ | {
            "_cleaner_lock": None,
            "_samples_lock": None,
            "_log_queue": None,
        }

    def _run_hooks(
        self,
        hook_runner_fn: Callable,
        hook_kwargs: dict,
        pool: WorkerPool | None = None,
        callback: Callable | None = None,
    ):
        if pool is not None:
            pool.apply_async(
                _poolable(hook_runner_fn),
                kwargs={
                    **hook_kwargs,
                    "hooks": self._hooks,
                    "log_label": (self._logger.extra or {"label": "cellophane"})["label"],
                    "dispatcher": self,
                },
                error_callback=partial(
                    _hook_error_callback,
                    logger=self._logger,
                ),
            )
        else:
            return hook_runner_fn(
                **hook_kwargs,
                hooks=self._hooks,
                log_queue=self._log_queue,
                logger=self._logger,
                dispatcher=self,
            )

    @overload
    def run_pre_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: None = None,
        checkpoint_suffix: str | None = None,
    ) -> Samples: ...
    @overload
    def run_pre_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: WorkerPool,
        checkpoint_suffix: str | None = None,
    ) -> None: ...
    def run_pre_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: WorkerPool | None = None,
        checkpoint_suffix: str | None = None,
    ) -> Samples:
        """Run pre-hooks for the given scope."""
        return self._run_hooks(
            hook_runner_fn=_run_pre_post_hooks,
            hook_kwargs={
                **self._common_kwargs,
                "when": "pre",
                "per": per,
                "samples": samples,
                "cleaner": cleaner,
                "checkpoint_suffix": checkpoint_suffix,
            },
            pool=pool,
            callback=partial(
                _hook_callback,
                samples=samples,
                cleaner=cleaner,
                pool=pool,
                dispatcher=self,
                logger=self._logger,
            ),
        )

    @overload
    def run_post_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: None = None,
        checkpoint_suffix: str | None = None,
    ) -> Samples: ...
    @overload
    def run_post_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: WorkerPool,
        checkpoint_suffix: str | None = None,
    ) -> None: ...
    def run_post_hooks(
        self,
        per: Literal["session", "sample", "runner"],
        samples: Samples,
        cleaner: Cleaner | DeferredCleaner,
        pool: WorkerPool | None = None,
        checkpoint_suffix: str | None = None,
    ) -> Samples:
        """Run post-hooks for the given scope."""
        return self._run_hooks(
            hook_runner_fn=_run_pre_post_hooks,
            hook_kwargs={
                **self._common_kwargs,
                "when": "post",
                "per": per,
                "samples": samples,
                "cleaner": cleaner,
                "checkpoint_suffix": checkpoint_suffix,
            },
            pool=pool,
            callback=partial(
                _hook_callback,
                samples=samples,
                cleaner=cleaner,
                pool=pool,
                dispatcher=self,
                logger=self._logger,
            ),
        )

    def run_exception_hooks(
        self,
        exception: BaseException,
        pool: WorkerPool | None = None,
    ) -> None:
        """Run exception-hooks for the given scope."""
        self._run_hooks(
            hook_runner_fn=_run_exception_hooks,
            hook_kwargs={
                **self._common_kwargs,
                "exception": exception,
            },
            pool=pool,
        )

    def start_runners(
        self,
        samples: Samples,
        cleaner: Cleaner,
    ) -> Samples:
        """Start runners using this dispatcher's configuration."""
        return _start_runners(
            **self._common_kwargs,  # ty: ignore[invalid-argument-type]
            runners=self._runners,
            samples=samples,
            log_queue=self._log_queue,
            cleaner=cleaner,
            dispatcher=self,
            logger=self._logger,
        )
