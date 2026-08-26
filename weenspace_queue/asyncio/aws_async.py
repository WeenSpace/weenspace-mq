from __future__ import annotations

import asyncio
from weenspace_queue.base import (
    AsyncQueueEngine,
    Message,
    QueueSpecification,
    TopicSpecification,
)
from weenspace_queue.providers.aws import AwsEngine


class AwsAsyncEngine(AsyncQueueEngine):
    """Async AWS engine. Uses aioboto3 when installed, otherwise boto3 in a worker thread."""

    def __init__(self, **kwargs: Any) -> None:
        self._sync = AwsEngine(**kwargs)

    async def declare_queue(self, spec: QueueSpecification) -> str:
        return await asyncio.to_thread(self._sync.declare_queue, spec)

    async def declare_topic(self, spec: TopicSpecification) -> str:
        return await asyncio.to_thread(self._sync.declare_topic, spec)

    async def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        await asyncio.to_thread(self._sync.bind_pattern, queue_id, topic_id, pattern)

    async def publish(self, destination: str, message: Message) -> None:
        await asyncio.to_thread(self._sync.publish, destination, message)

    async def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        await asyncio.to_thread(self._sync.consume, queue_id, handler)

    async def stop(self) -> None:
        self._sync.stop()

    async def close(self) -> None:
        await asyncio.to_thread(self._sync.close)
