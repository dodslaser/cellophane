"""Runners for executing functions as jobs."""

from functools import reduce
from logging import LoggerAdapter, getLogger
from multiprocessing import Queue
from pathlib import Path
from signal import SIGTERM, signal
from typing import Any, Callable, Sequence

from cloudpickle import dumps, loads
from mpire import WorkerPool
from psutil import Process, TimeoutExpired

from cellophane.src import cfg, data, executors, logs

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
    main: Callable[..., data.Samples | None]

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
        timestamp: str,
    ) -> bytes:
        samples: data.Samples = loads(samples_pickle)
        logs.redirect_logging_to_queue(log_queue)
        logger = LoggerAdapter(getLogger(), {"label": self.label})
        signal(SIGTERM, _cleanup(logger))

        workdir = config.workdir / config.tag / self.label
        if self.split_by:
            workdir /= samples[0][self.split_by] or "unknown"

        workdir.mkdir(parents=True, exist_ok=True)

        with WorkerPool(
            daemon=False,
            use_dill=True,
        ) as pool:
            try:
                match self.main(
                    samples=samples,
                    config=config,
                    timestamp=timestamp,
                    logger=logger,
                    root=root,
                    workdir=workdir,
                    executor=executor_cls(
                        config=config,
                        pool=pool,
                        log_queue=log_queue,
                    ),
                    checkpoints=Checkpoints(
                        samples=samples,
                        workdir=workdir,
                        config=config,
                    )
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

            pool.stop_and_join()

            _resolve_outputs(samples, workdir, config, logger)
            for sample in samples.complete:
                logger.debug(f"Sample {sample.id} processed successfully")
            if n_failed := len(samples.failed):
                logger.error(f"{n_failed} samples failed")
            for sample in samples.failed:
                logger.debug(f"Sample {sample.id} failed - {sample.failed}")

            return dumps(samples)

def _resolve_outputs(
    samples: data.Samples,
    workdir: Path,
    config: cfg.Config,
    logger: LoggerAdapter,
) -> None:
    if samples.complete:
        for output_ in samples.output.copy():
            if isinstance(output_, data.OutputGlob):
                samples.output.remove(output_)
                try:
                    o, w = output_.resolve(
                        samples=samples.complete,
                        workdir=workdir,
                        config=config,
                    )
                    for warning in w:
                        logger.warning(warning)
                    samples.output |= o
                except Exception as exc:  # pylint: disable=broad-except
                    logger.warning(f"Failed to resolve output {output_}: {exc}")
                    logger.debug(exc, exc_info=True)


def _cleanup(logger: LoggerAdapter) -> Callable:
    def inner(*args: Any) -> None:
        del args  # Unused
        for proc in Process().children(recursive=True):
            try:
                logger.debug(f"Waiting for {proc.name()} ({proc.pid})")
                proc.terminate()
                proc.wait(10)
            except TimeoutExpired:
                logger.warning(
                    f"Killing unresponsive process {proc.name()} ({proc.pid})"
                )
                proc.kill()
                proc.wait()
        raise SystemExit(1)

    return inner


def start_runners(
    *,
    runners: Sequence[Runner],
    samples: data.Samples,
    logger: LoggerAdapter,
    log_queue: Queue,
    **kwargs: Any,
) -> data.Samples:
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
    if not runners:
        logger.warning("No runners to execute")
        return samples

    with WorkerPool(
        use_dill=True,
        daemon=False,
        start_method="fork",
        shared_objects=log_queue,
    ) as pool:
        try:
            results = []
            for runner_, samples_ in (
                (r, s)
                for r in runners
                for _, s in (
                    samples.split(by=r.split_by) if r.split_by else [(None, samples)]
                )
            ):
                result = pool.apply_async(
                    runner_,
                    kwargs={
                        **kwargs,
                        "samples_pickle": dumps(samples_),
                    },
                )
                results.append(result)

            pool.stop_and_join(keep_alive=True)
        except KeyboardInterrupt:
            logger.critical("Received SIGINT, telling runners to shut down...")
            pool.terminate()

            return samples

        except Exception as exception:  # pylint: disable=broad-except
            logger.critical(f"Unhandled exception in runner: {exception}")
            pool.terminate()
            return samples

    try:
        return reduce(lambda a, b: a & b, (loads(r.get()) for r in results))
    except Exception as exception:  # pylint: disable=broad-except
        logger.critical(
            f"Unhandled exception when collecting results: {exception}",
            exc_info=True,
        )
        return samples
