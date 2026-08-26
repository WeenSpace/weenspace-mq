from weenspace_queue import QueueClient
from weenspace_queue.utils import (
    inject_aws_routing_attrs,
    rabbitmq_exchange_address,
    rabbitmq_pattern_matches,
    rabbitmq_queue_address,
    rmq_pattern_to_aws_sns,
    sns_filter_matches,
    unwrap_sqs_body,
)


CASES = [
    ("order.placed", "order.placed", True),
    ("order.placed", "order.shipped", False),
    ("order.*.completed", "order.eu.completed", True),
    ("order.*.completed", "order.eu.uk.completed", False),
    ("global.orders.#", "global.orders", True),
    ("global.orders.#", "global.orders.eu.completed", True),
    ("global.orders.#", "global.other.eu", False),
    ("eu.*.truck.#", "eu.uk.truck.delayed", True),
    ("eu.*.truck.#", "eu.uk.car.delayed", False),
    ("#.delayed", "eu.uk.truck.delayed", True),
    ("#.delayed", "delayed", True),
    ("#.delayed", "eu.uk.truck.delivered", False),
    ("region.#.failed", "region.east.failed", True),
    ("region.#.failed", "region.failed", True),
    ("region.#.failed", "other.east.failed", False),
    ("#", "anything.at.all", True),
    ("user.click.*", "user.click.cart", True),
    ("user.click.*", "user.click.cart.extra", False),
]


def test_rabbitmq_and_aws_wildcard_cases_agree() -> None:
    for pattern, routing_key, expected in CASES:
        rmq = rabbitmq_pattern_matches(pattern, routing_key)
        aws = sns_filter_matches(rmq_pattern_to_aws_sns(pattern), routing_key)
        assert rmq is expected, f"rmq {pattern} vs {routing_key}"
        assert aws is expected, f"aws {pattern} vs {routing_key}"


def test_inject_routing_attributes() -> None:
    attrs = inject_aws_routing_attrs("eu.uk.truck.delayed")
    assert attrs["routing_key"]["StringValue"] == "eu.uk.truck.delayed"
    assert attrs["rk_len"]["StringValue"] == "4"
    assert attrs["rk_idx_0"]["StringValue"] == "eu"
    assert attrs["rk_ridx_0"]["StringValue"] == "delayed"


def test_unwrap_sns_envelope() -> None:
    raw = (
        '{"Type":"Notification","Message":"hello",'
        '"MessageAttributes":{"routing_key":{"Value":"order.placed"}}}'
    )
    body, routing_key, extra = unwrap_sqs_body(raw)
    assert body == b"hello"
    assert routing_key == "order.placed"
    assert extra["sns"]["Type"] == "Notification"


def test_rabbitmq_address_helpers() -> None:
    assert rabbitmq_queue_address("orders") == "/queues/orders"
    assert rabbitmq_queue_address("/queues/orders") == "/queues/orders"
    assert (
        rabbitmq_exchange_address("OrdersExchange", "order.placed")
        == "/exchanges/OrdersExchange/order.placed"
    )


def test_queue_client_rejects_unknown_provider() -> None:
    try:
        QueueClient(provider="kafka")
    except ValueError as exc:
        assert "Unsupported provider" in str(exc)
    else:
        raise AssertionError("expected ValueError")
