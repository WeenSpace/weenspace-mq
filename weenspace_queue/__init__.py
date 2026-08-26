from typing import Any, Callable, Dict, Type

from .asyncio.aws_async import AwsAsyncEngine
from .asyncio.rabbitmq_async import RabbitMqAsyncEngine
from .base import (
    AsyncQueueEngine,
    Message,
    QueueEngine,
    QueueSpecification,
    TopicSpecification,
)
from .constants import (
    PROVIDER_AWS,
    PROVIDER_RABBITMQ,
    ExchangeKind,
    Provider,
    QueueKind,
)
from .providers.aws import AwsEngine
from .providers.rabbitmq import RabbitMqEngine


class QueueClient:
    """Single client: pass provider name, then use the same publish/consume/topology methods."""

    _ENGINES: Dict[str, Type[QueueEngine]] = {
        PROVIDER_AWS: AwsEngine,
        PROVIDER_RABBITMQ: RabbitMqEngine,
    }

    def __init__(self, provider: str, **config: Any) -> None:
        prov_key = provider.lower().strip()
        engine_cls = self._ENGINES.get(prov_key)
        if engine_cls is None:
            supported = ", ".join(sorted(self._ENGINES))
            raise ValueError(
                f"Unsupported provider '{provider}'. Supported: {supported}"
            )
        self.provider = prov_key
        self.engine: QueueEngine = engine_cls(**config)

    def declare_queue(self, spec: QueueSpecification) -> str:
        return self.engine.declare_queue(spec)

    def declare_topic(self, spec: TopicSpecification) -> str:
        return self.engine.declare_topic(spec)

    def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        self.engine.bind_pattern(queue_id, topic_id, pattern)

    def publish(self, destination: str, message: Message) -> None:
        self.engine.publish(destination, message)

    def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        self.engine.consume(queue_id, handler)

    def stop(self) -> None:
        self.engine.stop()

    def close(self) -> None:
        self.engine.close()

    def __enter__(self) -> "QueueClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class AsyncQueueClient:
    """Async twin of QueueClient. Same method names, same provider argument."""

    _ENGINES: Dict[str, Type[AsyncQueueEngine]] = {
        PROVIDER_AWS: AwsAsyncEngine,
        PROVIDER_RABBITMQ: RabbitMqAsyncEngine,
    }

    def __init__(self, provider: str, **config: Any) -> None:
        prov_key = provider.lower().strip()
        engine_cls = self._ENGINES.get(prov_key)
        if engine_cls is None:
            supported = ", ".join(sorted(self._ENGINES))
            raise ValueError(
                f"Unsupported provider '{provider}'. Supported: {supported}"
            )
        self.provider = prov_key
        self.engine: AsyncQueueEngine = engine_cls(**config)

    async def declare_queue(self, spec: QueueSpecification) -> str:
        return await self.engine.declare_queue(spec)

    async def declare_topic(self, spec: TopicSpecification) -> str:
        return await self.engine.declare_topic(spec)

    async def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        await self.engine.bind_pattern(queue_id, topic_id, pattern)

    async def publish(self, destination: str, message: Message) -> None:
        await self.engine.publish(destination, message)

    async def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        await self.engine.consume(queue_id, handler)

    async def stop(self) -> None:
        await self.engine.stop()

    async def close(self) -> None:
        await self.engine.close()

    async def __aenter__(self) -> "AsyncQueueClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()


__all__ = [
    "AsyncQueueClient",
    "AsyncQueueEngine",
    "ExchangeKind",
    "Message",
    "Provider",
    "QueueClient",
    "QueueEngine",
    "QueueKind",
    "QueueSpecification",
    "TopicSpecification",
]
