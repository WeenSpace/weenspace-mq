from __future__ import annotations

from typing import Any, Callable, Optional

from weenspace_queue import (
    ClassicQueueSpecification,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    ExchangeType,
    Message as ProtonMessage,
    QuorumQueueSpecification,
    StreamSpecification,
)
from weenspace_queue.asyncio import AsyncEnvironment
from weenspace_queue.delivery_context import DeliveryContext
from weenspace_queue.amqp_consumer_handler import AMQPMessagingHandler
from weenspace_queue.qpid.proton._events import Event

from weenspace_queue.base import (
    AsyncQueueEngine,
    Message,
    QueueSpecification,
    TopicSpecification,
)
from weenspace_queue.constants import (
    DEFAULT_RABBITMQ_URI,
    ExchangeKind,
    QueueKind,
)
from weenspace_queue.utils import (
    encode_body,
    rabbitmq_exchange_address,
    rabbitmq_queue_address,
    rabbitmq_resource_name,
)

_EXCHANGE_KIND_MAP = {
    ExchangeKind.DIRECT: ExchangeType.DIRECT,
    ExchangeKind.TOPIC: ExchangeType.TOPIC,
    ExchangeKind.FANOUT: ExchangeType.FANOUT,
    ExchangeKind.HEADERS: ExchangeType.HEADERS,
}


class _CallbackHandler(AMQPMessagingHandler):
    def __init__(self, handler: Callable[[Message], None]) -> None:
        super().__init__(auto_accept=False, auto_settle=True)
        self._handler = handler

    def on_amqp_message(self, event: Event) -> None:
        proton_msg = event.message
        routing_key = proton_msg.subject or ""
        attributes = dict(proton_msg.properties or {})
        context = DeliveryContext()

        def accept(evt: Event = event) -> None:
            context.accept(evt)

        def reject(evt: Event = event) -> None:
            context.discard(evt)

        def requeue(evt: Event = event) -> None:
            context.requeue(evt)

        self._handler(
            Message(
                body=encode_body(proton_msg.body),
                routing_key=routing_key,
                attributes=attributes,
                accept=accept,
                reject=reject,
                requeue=requeue,
            )
        )


class RabbitMqAsyncEngine(AsyncQueueEngine):
    def __init__(self, **kwargs: Any) -> None:
        uri = kwargs.get("uri")
        uris = kwargs.get("uris")
        if uri is None and uris is None:
            uri = DEFAULT_RABBITMQ_URI
        env_kwargs: dict[str, Any] = {"uri": uri, "uris": uris}
        for key in ("ssl_context", "oauth2_options", "recovery_configuration"):
            if kwargs.get(key) is not None:
                env_kwargs[key] = kwargs[key]
        self._env = AsyncEnvironment(**env_kwargs)
        self._conn: Any = None
        self._mgmt: Any = None
        self._consumer: Any = None

    async def _ensure(self) -> None:
        if self._conn is not None:
            return
        self._conn = await self._env.connection()
        await self._conn.dial()
        self._mgmt = await self._conn.management()

    async def declare_queue(self, spec: QueueSpecification) -> str:
        await self._ensure()
        if spec.kind == QueueKind.STREAM:
            await self._mgmt.declare_queue(StreamSpecification(name=spec.name))
        elif spec.kind == QueueKind.QUORUM:
            await self._mgmt.declare_queue(
                QuorumQueueSpecification(
                    name=spec.name,
                    dead_letter_exchange=spec.dead_letter_target,
                    dead_letter_routing_key=spec.dead_letter_routing_key,
                    deliver_limit=spec.max_receive_count,
                )
            )
        else:
            await self._mgmt.declare_queue(
                ClassicQueueSpecification(
                    name=spec.name,
                    is_durable=spec.is_durable,
                    dead_letter_exchange=spec.dead_letter_target,
                    dead_letter_routing_key=spec.dead_letter_routing_key,
                )
            )
        return rabbitmq_queue_address(spec.name)

    async def declare_topic(self, spec: TopicSpecification) -> str:
        await self._ensure()
        await self._mgmt.declare_exchange(
            ExchangeSpecification(
                name=spec.name,
                exchange_type=_EXCHANGE_KIND_MAP.get(spec.kind, ExchangeType.TOPIC),
                is_durable=spec.is_durable,
            )
        )
        return spec.name

    async def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        await self._ensure()
        await self._mgmt.bind(
            ExchangeToQueueBindingSpecification(
                source_exchange=rabbitmq_resource_name(topic_id),
                destination_queue=rabbitmq_resource_name(queue_id),
                binding_key=pattern,
            )
        )

    async def publish(self, destination: str, message: Message) -> None:
        await self._ensure()
        address = self._publish_address(destination, message.routing_key)
        publisher = await self._conn.publisher(address)
        try:
            proton_msg = ProtonMessage(body=encode_body(message.body))
            proton_msg.inferred = True
            if message.routing_key:
                proton_msg.subject = message.routing_key
            await publisher.publish(proton_msg)
        finally:
            await publisher.close()

    async def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        await self._ensure()
        destination = rabbitmq_queue_address(queue_id)
        self._consumer = await self._conn.consumer(
            destination, message_handler=_CallbackHandler(handler)
        )
        await self._consumer.run()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def close(self) -> None:
        await self.stop()
        if self._conn is not None:
            await self._conn.close()
        await self._env.close()

    def _publish_address(self, destination: str, routing_key: str) -> str:
        if destination.startswith("/queues/"):
            return destination
        if destination.startswith("/exchanges/"):
            return rabbitmq_exchange_address(destination, routing_key)
        if routing_key:
            return rabbitmq_exchange_address(destination, routing_key)
        return rabbitmq_queue_address(destination)
