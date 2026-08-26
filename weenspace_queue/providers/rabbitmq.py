from __future__ import annotations

from typing import Any, Callable, Optional

from weenspace_queue import (
    AMQPMessagingHandler,
    ClassicQueueSpecification,
    Environment,
    Event,
    ExchangeSpecification,
    ExchangeToQueueBindingSpecification,
    ExchangeType,
    Message as ProtonMessage,
    QuorumQueueSpecification,
    StreamSpecification,
)
from weenspace_queue.delivery_context import DeliveryContext

from weenspace_queue.base import Message, QueueEngine, QueueSpecification, TopicSpecification
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
        if proton_msg.address:
            attributes["address"] = proton_msg.address
        if proton_msg.correlation_id is not None:
            attributes["correlation_id"] = proton_msg.correlation_id
        if proton_msg.reply_to:
            attributes["reply_to"] = proton_msg.reply_to

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


class RabbitMqEngine(QueueEngine):
    def __init__(self, **kwargs: Any) -> None:
        uri = kwargs.get("uri")
        uris = kwargs.get("uris")
        if uri is None and uris is None:
            uri = DEFAULT_RABBITMQ_URI
        env_kwargs: dict[str, Any] = {"uri": uri, "uris": uris}
        for key in ("ssl_context", "oauth2_options", "recovery_configuration"):
            if kwargs.get(key) is not None:
                env_kwargs[key] = kwargs[key]
        self._env = Environment(**env_kwargs)
        self._conn = self._env.connection()
        self._conn.dial()
        self._mgmt = self._conn.management()
        self._consumer: Optional[Any] = None

    def declare_queue(self, spec: QueueSpecification) -> str:
        if spec.kind == QueueKind.STREAM:
            self._mgmt.declare_queue(
                StreamSpecification(
                    name=spec.name,
                    **{
                        key: value
                        for key, value in spec.extra.items()
                        if key in StreamSpecification.__dataclass_fields__
                    },
                )
            )
            return rabbitmq_queue_address(spec.name)

        if spec.kind == QueueKind.QUORUM:
            self._mgmt.declare_queue(
                QuorumQueueSpecification(
                    name=spec.name,
                    dead_letter_exchange=spec.dead_letter_target,
                    dead_letter_routing_key=spec.dead_letter_routing_key,
                    deliver_limit=spec.max_receive_count,
                    **{
                        key: value
                        for key, value in spec.extra.items()
                        if key in QuorumQueueSpecification.__dataclass_fields__
                    },
                )
            )
            return rabbitmq_queue_address(spec.name)

        self._mgmt.declare_queue(
            ClassicQueueSpecification(
                name=spec.name,
                is_durable=spec.is_durable,
                dead_letter_exchange=spec.dead_letter_target,
                dead_letter_routing_key=spec.dead_letter_routing_key,
                **{
                    key: value
                    for key, value in spec.extra.items()
                    if key in ClassicQueueSpecification.__dataclass_fields__
                },
            )
        )
        return rabbitmq_queue_address(spec.name)

    def declare_topic(self, spec: TopicSpecification) -> str:
        self._mgmt.declare_exchange(
            ExchangeSpecification(
                name=spec.name,
                exchange_type=_EXCHANGE_KIND_MAP.get(spec.kind, ExchangeType.TOPIC),
                is_durable=spec.is_durable,
            )
        )
        return spec.name

    def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        self._mgmt.bind(
            ExchangeToQueueBindingSpecification(
                source_exchange=rabbitmq_resource_name(topic_id),
                destination_queue=rabbitmq_resource_name(queue_id),
                binding_key=pattern,
            )
        )

    def publish(self, destination: str, message: Message) -> None:
        address = self._publish_address(destination, message.routing_key)
        publisher = self._conn.publisher(address)
        try:
            proton_msg = ProtonMessage(body=encode_body(message.body))
            proton_msg.inferred = True
            if message.routing_key:
                proton_msg.subject = message.routing_key
            correlation_id = (message.attributes or {}).get("correlation_id")
            if correlation_id is not None:
                proton_msg.correlation_id = correlation_id
            reply_to = (message.attributes or {}).get("reply_to")
            if reply_to:
                proton_msg.reply_to = reply_to
            if message.attributes:
                proton_msg.properties = {
                    key: value
                    for key, value in message.attributes.items()
                    if key not in {"correlation_id", "reply_to"}
                    and isinstance(value, (str, int, float, bool))
                }
            publisher.publish(proton_msg)
        finally:
            publisher.close()

    def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        destination = rabbitmq_queue_address(queue_id)
        wrapped = _CallbackHandler(handler)
        self._consumer = self._conn.consumer(destination, message_handler=wrapped)
        self._consumer.run()

    def stop(self) -> None:
        if self._consumer is not None:
            self._consumer.stop()

    def close(self) -> None:
        self.stop()
        self._conn.close()

    def _publish_address(self, destination: str, routing_key: str) -> str:
        if destination.startswith("/queues/"):
            return destination
        if destination.startswith("/exchanges/"):
            return rabbitmq_exchange_address(destination, routing_key)
        if routing_key:
            return rabbitmq_exchange_address(destination, routing_key)
        return rabbitmq_queue_address(destination)
