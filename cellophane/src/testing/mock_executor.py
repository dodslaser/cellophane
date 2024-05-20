from typing import Any, Callable
from unittest.mock import MagicMock
from uuid import uuid4

from attrs import define
from mpire.async_result import AsyncResult

from cellophane.src.executors.executor import Executor


@define(slots=False)
class MockExecutor(Executor, name="mock"):
    def __attrs_post_init__(self, *args: Any, **kwargs: Any) -> None:
        object.__setattr__(
            self,
            "submit",
            MagicMock(
                return_value=(MagicMock(), uuid4()),
                side_effect=self.submit_,
            ),
        )

    @staticmethod
    def submit_(
        *args: Any,
        callback: Callable | None = None,
        error_callback: Callable | None = None,
        **kwargs: Any,
    ) -> None:
        del args, kwargs

        result_ = AsyncResult({}, callback, error_callback)
        if callback:
            callback(result_)
