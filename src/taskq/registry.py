from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Type

from pydantic import BaseModel

Handler = Callable[[BaseModel], Awaitable[None]]


@dataclass
class HandlerSpec:
    func: Handler
    payload_model: Type[BaseModel]
    timeout_s: int


class HandlerRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, HandlerSpec] = {}

    def handler(
        self,
        task_type: str,
        *,
        payload_model: Type[BaseModel],
        timeout_s: int = 60,
    ) -> Callable[[Handler], Handler]:
        def decorator(func: Handler) -> Handler:
            if task_type in self._specs:
                raise ValueError(f"task_type {task_type!r} already registered")
            self._specs[task_type] = HandlerSpec(
                func=func, payload_model=payload_model, timeout_s=timeout_s
            )
            return func

        return decorator

    def get(self, task_type: str) -> HandlerSpec:
        try:
            return self._specs[task_type]
        except KeyError as exc:
            raise KeyError(f"no handler registered for task_type {task_type!r}") from exc


registry = HandlerRegistry()
