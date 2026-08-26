from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .constants import (
    DEFAULT_MAX_RECEIVE_COUNT,
    ExchangeKind,
    QueueKind,
)


@dataclass
class Message:
    body: bytes
    routing_key: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    accept: Optional[Callable[[], None]] = None
    reject: Optional[Callable[[], None]] = None
    requeue: Optional[Callable[[], None]] = None


@dataclass
class QueueSpecification:
    name: str
    is_durable: bool = True
    kind: QueueKind = QueueKind.CLASSIC
    dead_letter_target: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None
    max_receive_count: int = DEFAULT_MAX_RECEIVE_COUNT
    visibility_timeout_seconds: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopicSpecification:
    name: str
    is_durable: bool = True
    kind: ExchangeKind = ExchangeKind.TOPIC
    extra: Dict[str, Any] = field(default_factory=dict)


class QueueEngine(ABC):
    """Provider strategy: one method set for publish, consume, and topology."""

    @abstractmethod
    def declare_queue(self, spec: QueueSpecification) -> str:
        pass

    @abstractmethod
    def declare_topic(self, spec: TopicSpecification) -> str:
        pass

    @abstractmethod
    def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        pass

    @abstractmethod
    def publish(self, destination: str, message: Message) -> None:
        pass

    @abstractmethod
    def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class AsyncQueueEngine(ABC):
    """Async counterpart of QueueEngine with identical method names."""

    @abstractmethod
    async def declare_queue(self, spec: QueueSpecification) -> str:
        pass

    @abstractmethod
    async def declare_topic(self, spec: TopicSpecification) -> str:
        pass

    @abstractmethod
    async def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        pass

    @abstractmethod
    async def publish(self, destination: str, message: Message) -> None:
        pass

    @abstractmethod
    async def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass
