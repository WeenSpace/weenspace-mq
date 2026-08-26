from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .constants import (
    AWS_NUMBER_DATA_TYPE,
    AWS_STRING_DATA_TYPE,
    RABBITMQ_EXCHANGE_PREFIX,
    RABBITMQ_QUEUE_PREFIX,
    ROUTING_KEY_ATTR,
    ROUTING_KEY_INDEX_PREFIX,
    ROUTING_KEY_LENGTH_ATTR,
    ROUTING_KEY_REVERSE_INDEX_PREFIX,
    ROUTING_SEPARATOR,
    SNS_ARN_MARKER,
    SNS_NOTIFICATION_TYPE,
    SQS_ARN_MARKER,
    SQS_BODY_KEY,
    SQS_FIFO_SUFFIX,
    SQS_URL_MARKER,
    WILDCARD_MULTI,
    WILDCARD_SINGLE,
)


def encode_body(body: Any) -> bytes:
    if isinstance(body, bytes):
        return body
    if body is None:
        return b""
    if isinstance(body, str):
        return body.encode("utf-8")
    return json.dumps(body).encode("utf-8")


def decode_body(body: Any) -> str:
    if isinstance(body, bytes):
        return body.decode("utf-8")
    return str(body)


def split_routing(value: str) -> List[str]:
    if value == "":
        return []
    return value.split(ROUTING_SEPARATOR)


def is_sns_destination(destination: str) -> bool:
    return SNS_ARN_MARKER in destination


def is_sqs_destination(destination: str) -> bool:
    return SQS_ARN_MARKER in destination or SQS_URL_MARKER in destination


def is_fifo_queue(name_or_url: str) -> bool:
    return name_or_url.rstrip("/").endswith(SQS_FIFO_SUFFIX)


def rabbitmq_queue_address(queue_id: str) -> str:
    if queue_id.startswith(RABBITMQ_QUEUE_PREFIX) or queue_id.startswith(
        RABBITMQ_EXCHANGE_PREFIX
    ):
        return queue_id
    return f"{RABBITMQ_QUEUE_PREFIX}{queue_id.lstrip('/')}"


def rabbitmq_exchange_address(topic_id: str, routing_key: str = "") -> str:
    if topic_id.startswith(RABBITMQ_EXCHANGE_PREFIX) or topic_id.startswith(
        RABBITMQ_QUEUE_PREFIX
    ):
        if routing_key and topic_id.count("/") == 2:
            return f"{topic_id}/{routing_key}"
        return topic_id
    if routing_key:
        return f"{RABBITMQ_EXCHANGE_PREFIX}{topic_id}/{routing_key}"
    return f"{RABBITMQ_EXCHANGE_PREFIX}{topic_id}"


def rabbitmq_resource_name(resource_id: str) -> str:
    if resource_id.startswith(RABBITMQ_QUEUE_PREFIX):
        return resource_id[len(RABBITMQ_QUEUE_PREFIX) :]
    if resource_id.startswith(RABBITMQ_EXCHANGE_PREFIX):
        remainder = resource_id[len(RABBITMQ_EXCHANGE_PREFIX) :]
        return remainder.split("/", 1)[0]
    return resource_id


def inject_aws_routing_attrs(routing_key: str) -> Dict[str, Any]:
    """Hydrate SNS/SQS MessageAttributes so wildcard filter policies can match."""
    if not routing_key:
        return {}
    segments = split_routing(routing_key)
    attributes: Dict[str, Any] = {
        ROUTING_KEY_ATTR: {
            "DataType": AWS_STRING_DATA_TYPE,
            "StringValue": routing_key,
        },
        ROUTING_KEY_LENGTH_ATTR: {
            "DataType": AWS_NUMBER_DATA_TYPE,
            "StringValue": str(len(segments)),
        },
    }
    for idx, value in enumerate(segments):
        attributes[f"{ROUTING_KEY_INDEX_PREFIX}{idx}"] = {
            "DataType": AWS_STRING_DATA_TYPE,
            "StringValue": value,
        }
        attributes[f"{ROUTING_KEY_REVERSE_INDEX_PREFIX}{idx}"] = {
            "DataType": AWS_STRING_DATA_TYPE,
            "StringValue": segments[-(idx + 1)],
        }
    return attributes


def rmq_pattern_to_aws_sns(routing_pattern: str) -> Dict[str, Any]:
    """
    Translate RabbitMQ topic patterns into SNS Message Attribute filter policies.

    Supported shapes:
    - exact: order.placed
    - single-word *: order.*.completed
    - multi-word trailing #: global.orders.#
    - multi-word leading #: #.delayed
    - combined: eu.*.truck.#
    - middle #: region.#.failed
    """
    pattern = routing_pattern.strip()
    if pattern in ("", WILDCARD_MULTI):
        return {}

    segments = split_routing(pattern)
    if WILDCARD_SINGLE not in segments and WILDCARD_MULTI not in segments:
        return {
            ROUTING_KEY_ATTR: [pattern],
            ROUTING_KEY_LENGTH_ATTR: [{"numeric": ["=", len(segments)]}],
        }

    hash_indexes = [i for i, segment in enumerate(segments) if segment == WILDCARD_MULTI]
    policy: Dict[str, Any] = {}

    if not hash_indexes:
        policy[ROUTING_KEY_LENGTH_ATTR] = [{"numeric": ["=", len(segments)]}]
        _apply_segment_filters(policy, segments, reverse=False)
        return policy

    first_hash = hash_indexes[0]
    last_hash = hash_indexes[-1]
    prefix = segments[:first_hash]
    suffix = segments[last_hash + 1 :]

    _apply_segment_filters(policy, prefix, reverse=False)
    _apply_segment_filters(policy, list(reversed(suffix)), reverse=True)
    return policy


def _apply_segment_filters(
    policy: Dict[str, Any], segments: List[str], reverse: bool
) -> None:
    prefix = ROUTING_KEY_REVERSE_INDEX_PREFIX if reverse else ROUTING_KEY_INDEX_PREFIX
    for idx, segment in enumerate(segments):
        key = f"{prefix}{idx}"
        if segment == WILDCARD_MULTI:
            continue
        if segment == WILDCARD_SINGLE:
            policy[key] = [{"exists": True}]
        else:
            policy[key] = [segment]


def rabbitmq_pattern_matches(pattern: str, routing_key: str) -> bool:
    """Reference matcher for RabbitMQ topic semantics used in tests."""
    return _match_segments(split_routing(pattern), split_routing(routing_key))


def _match_segments(pattern: List[str], key: List[str]) -> bool:
    if not pattern:
        return not key
    head, tail = pattern[0], pattern[1:]
    if head == WILDCARD_MULTI:
        for consumed in range(len(key) + 1):
            if _match_segments(tail, key[consumed:]):
                return True
        return False
    if not key:
        return False
    if head in (WILDCARD_SINGLE, key[0]):
        return _match_segments(tail, key[1:])
    return False


def sns_filter_matches(policy: Dict[str, Any], routing_key: str) -> bool:
    """Evaluate a translated SNS filter against injected routing attributes."""
    if not policy:
        return True
    attributes = inject_aws_routing_attrs(routing_key)
    for attr_name, conditions in policy.items():
        actual = attributes.get(attr_name)
        if actual is None:
            return False
        actual_value = actual["StringValue"]
        if not _condition_matches(conditions, actual_value):
            return False
    return True


def _condition_matches(conditions: List[Any], actual_value: str) -> bool:
    for condition in conditions:
        if isinstance(condition, dict):
            if "exists" in condition:
                if condition["exists"]:
                    return True
                continue
            numeric = condition.get("numeric")
            if numeric and numeric[0] == "=":
                return str(actual_value) == str(numeric[1])
            continue
        if str(condition) == str(actual_value):
            return True
    return False


def unwrap_sqs_body(raw_body: str) -> tuple[bytes, str, Dict[str, Any]]:
    """Unwrap SNS-to-SQS envelopes when present; otherwise treat as the payload."""
    attributes: Dict[str, Any] = {}
    routing_key = ""
    body = raw_body
    try:
        parsed = json.loads(raw_body)
    except (TypeError, json.JSONDecodeError):
        return encode_body(raw_body), routing_key, attributes

    if isinstance(parsed, dict) and parsed.get("Type") == SNS_NOTIFICATION_TYPE:
        body = parsed.get("Message", "")
        sns_attrs = parsed.get("MessageAttributes") or {}
        routing_key = _routing_key_from_sns_attrs(sns_attrs)
        attributes["sns"] = {key: value for key, value in parsed.items() if key != "Message"}
        return encode_body(body), routing_key, attributes

    return encode_body(raw_body), routing_key, attributes


def _routing_key_from_sns_attrs(sns_attrs: Dict[str, Any]) -> str:
    routing = sns_attrs.get(ROUTING_KEY_ATTR) or {}
    return str(routing.get("Value") or routing.get("StringValue") or "")


def routing_key_from_sqs_attributes(message: Dict[str, Any]) -> str:
    attrs = message.get("MessageAttributes") or {}
    routing = attrs.get(ROUTING_KEY_ATTR) or {}
    return str(routing.get("StringValue") or routing.get("Value") or "")


def extract_sqs_body(message: Dict[str, Any]) -> tuple[bytes, str, Dict[str, Any]]:
    raw = message.get(SQS_BODY_KEY, "")
    body, routing_key, extra = unwrap_sqs_body(raw)
    if not routing_key:
        routing_key = routing_key_from_sqs_attributes(message)
    extra["sqs"] = {
        key: value
        for key, value in message.items()
        if key not in {SQS_BODY_KEY}
    }
    return body, routing_key, extra


def optional_queue_name_from_url(queue_url: str) -> Optional[str]:
    if not queue_url:
        return None
    return queue_url.rstrip("/").split("/")[-1]
