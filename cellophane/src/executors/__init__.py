"""Executors module for cellophane."""

from .executor import Executor
from .subprocess_executor import SubprocesExecutor

EXECUTOR: type[Executor] = Executor

__all__ = [
    "EXECUTOR",
    "Executor",
    "SubprocesExecutor",
]
