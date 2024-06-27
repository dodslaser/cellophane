"""Runners for executing functions as jobs."""

from functools import partial, reduce
from logging import LoggerAdapter, getLogger
from multiprocessing import Queue
from pathlib import Path
from typing import Callable, Sequence

from mpire import WorkerPool
from mpire.exception import InterruptWorker
from psutil import Process, TimeoutExpired

from cellophane.src.cfg import Config
from cellophane.src.cleanup import Cleaner, DeferredCleaner
from cellophane.src.data import OutputGlob, Samples
from cellophane.src.executors import Executor
from cellophane.src.logs import handle_warnings, redirect_logging_to_queue

from .checkpoint import Checkpoints


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
    main: Callable[..., Samples | None]

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
        config: Config,
        root: Path,
        samples: Samples,
        executor_cls: type[Executor],
        timestamp: str,
        workdir: Path,
    ) -> tuple[Samples, DeferredCleaner]:
        handle_warnings()
        redirect_logging_to_queue(log_queue)

        workdir.mkdir(parents=True, exist_ok=True)
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        cleaner = DeferredCleaner(root=workdir)

        with executor_cls(
            config=config,
            log_queue=log_queue,
        ) as executor:
            cleanup = partial(
                _cleanup,
                logger=logger,
                samples=samples,
                executor=executor,
                reason=None,
            )
            try:
                match self.main(
                    samples=samples,
                    config=config,
                    timestamp=timestamp,
                    logger=logger,
                    root=root,
                    workdir=workdir,
                    executor=executor,
                    cleaner=cleaner,
                    checkpoints=Checkpoints(
                        samples=samples,
                        workdir=workdir,
                        config=config,
                    ),
                ):
                    case None:
                        logger.debug("Runner did not return any samples")

                    case returned if isinstance(returned, Samples):
                        samples = returned

                    case returned:
                        logger.warning(f"Unexpected return type {type(returned)}")

                for sample in samples:
                    sample.processed = True

            except InterruptWorker:
                logger.warning("Runner interrupted")
                cleanup(reason=f"Runner '{self.name}' interrupted")

            except SystemExit as exc:
                logger.warning(
                    "Runner exited with non-zero status"
                    + (f"({exc.code})" if exc.code is not None else "")
                )
                cleanup(
                    reason=(
                        f"Runner '{self.name}' exitded with non-zero status"
                        + (f"({exc.code})" if exc.code is not None else "")
                    )
                )

            except BaseException as exc:  # pylint: disable=broad-except
                logger.warning(f"Unhandeled exception: {exc!r}", exc_info=exc)
                cleanup(reason=f"Unhandeled exception in runner '{self.name}' {exc!r}")

        _resolve_outputs(samples, workdir, config, logger)
        for sample in samples.complete:
            logger.debug(f"Sample {sample.id} processed successfully")
        for sample in samples.unprocessed:
            sample.fail("Sample was not processed")
        if n_failed := len(samples.failed):
            logger.error(f"{n_failed} samples failed")
            cleaner.unregister(workdir)
        for sample in samples.failed:
            logger.debug(f"Sample {sample.id} failed - {sample.failed}")

        return samples, cleaner


def _resolve_outputs(
    samples: Samples,
    workdir: Path,
    config: Config,
    logger: LoggerAdapter,
) -> None:
    for output_ in samples.output.copy():
        if not isinstance(output_, OutputGlob):
            continue
        samples.output.remove(output_)
        if not samples.complete:
            continue
        try:
            samples.output |= output_.resolve(
                samples=samples.complete,
                workdir=workdir,
                config=config,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(f"Failed to resolve output {output_}: {exc!r}")
            logger.debug(exc, exc_info=True)


def _cleanup(
    logger: LoggerAdapter,
    samples: Samples,
    executor: Executor,
    reason: str,
) -> None:
    reason_ = repr(reason) if isinstance(reason, BaseException) else reason
    executor.terminate()
    logger.debug("Clearing outputs and failing samples")
    samples.output = set()
    for sample in samples:
        sample.fail(reason_)
    for proc in Process().children(recursive=True):
        try:
            logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
            proc.terminate()
            proc.wait(10)
        except TimeoutExpired:
            logger.warning(f"Killing unresponsive process {proc.name()} ({proc.pid})")
            proc.kill()
            proc.wait()


def start_runners(
    *,
    runners: Sequence[Runner],
    samples: Samples,
    logger: LoggerAdapter,
    log_queue: Queue,
    config: Config,
    root: Path,
    executor_cls: type[Executor],
    timestamp: str,
    cleaner: Cleaner,
) -> Samples:
    """
    Start cellphane runners in parallel and collect the results.

    Args:
        runners (Sequence[Runner]): The runners to execute.
        samples (data.Samples): The samples to process.
        logger (LoggerAdapter): The logger.
        log_queue (Queue): The queue for logging.
        kwargs (Any): Additional keyword arguments to pass to the runners.

    Returns:
        data.Samples: The samples after processing.
    """
    if not samples:
        logger.warning("No samples to process")
        return samples

    if not runners:
        logger.warning("No runners to execute")
        for sample in samples.unprocessed:
            sample.fail("Sample was not processed")
        return samples

    with WorkerPool(
        use_dill=True,
        daemon=False,
        start_method="fork",
        shared_objects=log_queue,
    ) as pool:
        try:
            results = []
            samples_: Samples
            for runner_, group, samples_ in (
                (r, g, s)
                for r in runners
                for g, s in (
                    samples.split(by=r.split_by) if r.split_by else [(None, samples)]
                )
            ):
                workdir = config.workdir / config.tag / runner_.label
                if runner_.split_by is not None:
                    workdir /= group or "unknown"

                result = pool.apply_async(
                    runner_,
                    kwargs={
                        "config": config,
                        "root": root,
                        "samples": samples_,
                        "executor_cls": executor_cls,
                        "timestamp": timestamp,
                        "workdir": workdir,
                    },
                )
                results.append(result)
            pool.stop_and_join()
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling runners to shut down...")
            pool.terminate()
            return samples

        except BaseException as exc:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception in runner: {exc!r}")
            pool.terminate()
            return samples

    try:
        cleaners: Sequence[DeferredCleaner]
        samples_, cleaners = zip(*(r.get() for r in results))
        samples_ = reduce(lambda a, b: a & b, iter(samples_))
        for cleaner_ in cleaners:
            cleaner &= cleaner_
    except Exception as exc:  # pylint: disable=broad-except
        logger.critical(
            f"Unhandled exception when collecting results: {exc!r}",
            exc_info=True,
        )
        return samples
    return samples_
