"""Executors module for cellophane."""

from .executor import Executor
from .mock_executor import MockExecutor
from .subprocess_executor import SubprocessExecutor

EXECUTOR: type[Executor] = Executor

__all__ = [
    "EXECUTOR",
    "Executor",
    "SubprocessExecutor",
    "MockExecutor",
]
