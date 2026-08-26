"""Unified QueueClient examples for AWS and RabbitMQ.

The application code stays the same; only the provider name (and config) changes.
"""

from weenspace_queue import (
    Message,
    QueueClient,
    QueueKind,
    QueueSpecification,
    TopicSpecification,
)


def handle_event(msg: Message) -> None:
    print(f"received {msg.body!r} routing_key={msg.routing_key}")
    if msg.accept:
        msg.accept()


def event_driven_task_queue(provider: str, **config) -> None:
    client = QueueClient(provider=provider, **config)
    client.declare_queue(
        QueueSpecification(
            name="order-worker-queue",
            kind=QueueKind.CLASSIC,
            dead_letter_target=config.get("dead_letter_target"),
            dead_letter_routing_key=config.get("dead_letter_routing_key"),
            max_receive_count=3,
        )
    )
    destination = config.get("queue_destination", "order-worker-queue")
    client.publish(destination, Message(body=b"Process Order #991"))
    # client.consume(destination, handler=handle_event)
    client.close()


def streaming_wildcard_pipeline(provider: str, **config) -> None:
    client = QueueClient(provider=provider, **config)
    topic = config["topic"]
    queue = config["queue"]
    client.declare_topic(TopicSpecification(name=config.get("topic_name", topic)))
    client.declare_queue(QueueSpecification(name=config.get("queue_name", queue)))
    client.bind_pattern(queue, topic, "user.click.*")
    client.publish(
        topic,
        Message(body=b'{"x": 14, "y": 82}', routing_key="user.click.cart"),
    )
    client.close()


if __name__ == "__main__":
    # Swap provider="aws" / provider="rabbitmq". Method names stay identical.
    event_driven_task_queue(
        "rabbitmq",
        uri="amqp://guest:guest@localhost:5672/",
        dead_letter_target="dlx.exchange",
        dead_letter_routing_key="dlq-fallback",
        queue_destination="order-worker-queue",
    )
    streaming_wildcard_pipeline(
        "rabbitmq",
        uri="amqp://guest:guest@localhost:5672/",
        topic="ClickStreamExchange",
        topic_name="ClickStreamExchange",
        queue="analytics-dashboard-stream",
        queue_name="analytics-dashboard-stream",
    )
