from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

from weenspace_queue.base import Message, QueueEngine, QueueSpecification, TopicSpecification
from weenspace_queue.constants import (
    ALLOW_SNS_SEND_SID_PREFIX,
    AWS_SOURCE_ARN_CONDITION,
    DEAD_LETTER_TARGET_ARN_KEY,
    DEFAULT_AWS_REGION,
    DEFAULT_SQS_MAX_MESSAGES,
    DEFAULT_SQS_RETENTION_SECONDS,
    DEFAULT_SQS_WAIT_TIME_SECONDS,
    IAM_POLICY_VERSION,
    MAX_RECEIVE_COUNT_KEY,
    SNS_FILTER_POLICY_SCOPE_MESSAGE_ATTRIBUTES,
    SNS_PROTOCOL_SQS,
    SNS_SERVICE_PRINCIPAL,
    SQS_FIFO_SUFFIX,
    SQS_POLICY_ATTRIBUTE,
    SQS_QUEUE_ARN_ATTRIBUTE,
    SQS_RECEIPT_HANDLE_ATTR,
    SQS_REDRIVE_POLICY_ATTRIBUTE,
    SQS_RETENTION_ATTRIBUTE,
    SQS_SEND_MESSAGE_ACTION,
    SQS_VISIBILITY_ATTRIBUTE,
)
from weenspace_queue.utils import (
    decode_body,
    extract_sqs_body,
    inject_aws_routing_attrs,
    is_fifo_queue,
    is_sns_destination,
    optional_queue_name_from_url,
    rmq_pattern_to_aws_sns,
)


class AwsEngine(QueueEngine):
    def __init__(self, **kwargs: Any) -> None:
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                'AWS provider requires boto3. Install with: pip install "weenspace-mq[aws]"'
            ) from exc

        self._running = False
        self.session = boto3.Session(
            region_name=kwargs.get("region_name", DEFAULT_AWS_REGION),
            aws_access_key_id=kwargs.get("aws_access_key_id"),
            aws_secret_access_key=kwargs.get("aws_secret_access_key"),
            aws_session_token=kwargs.get("aws_session_token"),
            profile_name=kwargs.get("profile_name"),
        )
        client_kwargs = {}
        if kwargs.get("endpoint_url"):
            client_kwargs["endpoint_url"] = kwargs["endpoint_url"]
        self.sqs = self.session.client("sqs", **client_kwargs)
        self.sns = self.session.client("sns", **client_kwargs)
        self._wait_time_seconds = int(
            kwargs.get("wait_time_seconds", DEFAULT_SQS_WAIT_TIME_SECONDS)
        )
        self._max_messages = int(
            kwargs.get("max_number_of_messages", DEFAULT_SQS_MAX_MESSAGES)
        )

    def declare_queue(self, spec: QueueSpecification) -> str:
        attributes: Dict[str, str] = {}
        if spec.dead_letter_target:
            attributes[SQS_REDRIVE_POLICY_ATTRIBUTE] = json.dumps(
                {
                    DEAD_LETTER_TARGET_ARN_KEY: self._as_queue_arn(
                        spec.dead_letter_target
                    ),
                    MAX_RECEIVE_COUNT_KEY: str(spec.max_receive_count),
                }
            )
        if spec.is_durable:
            attributes[SQS_RETENTION_ATTRIBUTE] = DEFAULT_SQS_RETENTION_SECONDS
        if spec.visibility_timeout_seconds is not None:
            attributes[SQS_VISIBILITY_ATTRIBUTE] = str(spec.visibility_timeout_seconds)

        fifo = spec.name.endswith(SQS_FIFO_SUFFIX)
        create_kwargs: Dict[str, Any] = {"QueueName": spec.name}
        if attributes:
            create_kwargs["Attributes"] = attributes
        if fifo:
            create_kwargs.setdefault("Attributes", {})["FifoQueue"] = "true"
            create_kwargs["Attributes"]["ContentBasedDeduplication"] = "true"

        response = self.sqs.create_queue(**create_kwargs)
        return response["QueueUrl"]

    def declare_topic(self, spec: TopicSpecification) -> str:
        create_kwargs: Dict[str, Any] = {"Name": spec.name}
        if spec.name.endswith(SQS_FIFO_SUFFIX):
            create_kwargs["Attributes"] = {"FifoTopic": "true"}
        response = self.sns.create_topic(**create_kwargs)
        return response["TopicArn"]

    def bind_pattern(self, queue_id: str, topic_id: str, pattern: str) -> None:
        queue_url = self._as_queue_url(queue_id)
        queue_arn = self._as_queue_arn(queue_url)
        policy = rmq_pattern_to_aws_sns(pattern)
        subscribe_kwargs: Dict[str, Any] = {
            "TopicArn": topic_id,
            "Protocol": SNS_PROTOCOL_SQS,
            "Endpoint": queue_arn,
        }
        attributes: Dict[str, str] = {}
        if policy:
            attributes["FilterPolicy"] = json.dumps(policy)
            attributes["FilterPolicyScope"] = SNS_FILTER_POLICY_SCOPE_MESSAGE_ATTRIBUTES
        if attributes:
            subscribe_kwargs["Attributes"] = attributes
        self.sns.subscribe(**subscribe_kwargs)
        self._allow_sns_to_sqs(queue_url, queue_arn, topic_id)

    def publish(self, destination: str, message: Message) -> None:
        body_str = decode_body(message.body)
        msg_attrs = dict(inject_aws_routing_attrs(message.routing_key))
        extra_attrs = message.attributes or {}
        for key, value in extra_attrs.items():
            if key in msg_attrs or not isinstance(value, (str, int)):
                continue
            msg_attrs[key] = {
                "DataType": "String",
                "StringValue": str(value),
            }

        if is_sns_destination(destination):
            publish_kwargs: Dict[str, Any] = {
                "TopicArn": destination,
                "Message": body_str,
            }
            if msg_attrs:
                publish_kwargs["MessageAttributes"] = msg_attrs
            group_id = extra_attrs.get("message_group_id") or message.routing_key
            if destination.endswith(SQS_FIFO_SUFFIX) and group_id:
                publish_kwargs["MessageGroupId"] = str(group_id)
                dedup = extra_attrs.get("message_deduplication_id")
                if dedup:
                    publish_kwargs["MessageDeduplicationId"] = str(dedup)
            self.sns.publish(**publish_kwargs)
            return

        queue_url = self._as_queue_url(destination)
        send_kwargs: Dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": body_str,
        }
        if msg_attrs:
            send_kwargs["MessageAttributes"] = msg_attrs
        if is_fifo_queue(queue_url):
            send_kwargs["MessageGroupId"] = str(
                extra_attrs.get("message_group_id") or message.routing_key or "default"
            )
            dedup = extra_attrs.get("message_deduplication_id")
            if dedup:
                send_kwargs["MessageDeduplicationId"] = str(dedup)
        self.sqs.send_message(**send_kwargs)

    def consume(self, queue_id: str, handler: Callable[[Message], None]) -> None:
        queue_url = self._as_queue_url(queue_id)
        self._running = True
        while self._running:
            response = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=self._max_messages,
                WaitTimeSeconds=self._wait_time_seconds,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )
            for sqs_msg in response.get("Messages", []):
                if not self._running:
                    break
                handler(self._to_message(queue_url, sqs_msg))

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self.stop()

    def _to_message(self, queue_url: str, sqs_msg: Dict[str, Any]) -> Message:
        body, routing_key, attributes = extract_sqs_body(sqs_msg)
        receipt = sqs_msg[SQS_RECEIPT_HANDLE_ATTR]
        attributes[SQS_RECEIPT_HANDLE_ATTR] = receipt

        def accept(handle: str = receipt) -> None:
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

        def requeue(handle: str = receipt) -> None:
            self.sqs.change_message_visibility(
                QueueUrl=queue_url, ReceiptHandle=handle, VisibilityTimeout=0
            )

        def reject(handle: str = receipt) -> None:
            # Let the redrive policy / visibility timeout move the message toward the DLQ.
            self.sqs.change_message_visibility(
                QueueUrl=queue_url, ReceiptHandle=handle, VisibilityTimeout=0
            )

        return Message(
            body=body,
            routing_key=routing_key,
            attributes=attributes,
            accept=accept,
            reject=reject,
            requeue=requeue,
        )

    def _as_queue_url(self, queue_id: str) -> str:
        if queue_id.startswith("https://") or queue_id.startswith("http://"):
            return queue_id
        if SQS_ARN_MARKER in queue_id:
            name = queue_id.rsplit(":", 1)[-1]
            return self.sqs.get_queue_url(QueueName=name)["QueueUrl"]
        return self.sqs.get_queue_url(QueueName=queue_id)["QueueUrl"]

    def _as_queue_arn(self, queue_id: str) -> str:
        if SQS_ARN_MARKER in queue_id:
            return queue_id
        queue_url = self._as_queue_url(queue_id)
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=[SQS_QUEUE_ARN_ATTRIBUTE]
        )
        return attrs["Attributes"][SQS_QUEUE_ARN_ATTRIBUTE]

    def _allow_sns_to_sqs(self, queue_url: str, queue_arn: str, topic_arn: str) -> None:
        try:
            current = self.sqs.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=[SQS_POLICY_ATTRIBUTE]
            )
            policy = json.loads(current.get("Attributes", {}).get(SQS_POLICY_ATTRIBUTE) or "{}")
        except Exception:
            policy = {}
        if not policy:
            policy = {"Version": IAM_POLICY_VERSION, "Statement": []}
        statements = policy.setdefault("Statement", [])
        sid = f"{ALLOW_SNS_SEND_SID_PREFIX}{abs(hash(topic_arn))}"
        statement = {
            "Sid": sid,
            "Effect": "Allow",
            "Principal": {"Service": SNS_SERVICE_PRINCIPAL},
            "Action": SQS_SEND_MESSAGE_ACTION,
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {AWS_SOURCE_ARN_CONDITION: topic_arn}},
        }
        statements = [
            item
            for item in statements
            if not (
                item.get("Action") == SQS_SEND_MESSAGE_ACTION
                and item.get("Condition", {})
                .get("ArnEquals", {})
                .get(AWS_SOURCE_ARN_CONDITION)
                == topic_arn
            )
        ]
        statements.append(statement)
        policy["Statement"] = statements
        self.sqs.set_queue_attributes(
            QueueUrl=queue_url,
            Attributes={SQS_POLICY_ATTRIBUTE: json.dumps(policy)},
        )

    def resolve_queue_name(self, queue_id: str) -> Optional[str]:
        if queue_id.startswith("https://"):
            return optional_queue_name_from_url(queue_id)
        return queue_id
