"""Shared uppercase constants for the unified queue client."""

from enum import Enum


class Provider(str, Enum):
    AWS = "aws"
    RABBITMQ = "rabbitmq"


class QueueKind(str, Enum):
    CLASSIC = "classic"
    QUORUM = "quorum"
    STREAM = "stream"


class ExchangeKind(str, Enum):
    DIRECT = "direct"
    TOPIC = "topic"
    FANOUT = "fanout"
    HEADERS = "headers"


PROVIDER_AWS = Provider.AWS.value
PROVIDER_RABBITMQ = Provider.RABBITMQ.value

DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_RABBITMQ_URI = "amqp://guest:guest@localhost:5672/"
DEFAULT_MAX_RECEIVE_COUNT = 5
DEFAULT_SQS_WAIT_TIME_SECONDS = 20
DEFAULT_SQS_MAX_MESSAGES = 10
DEFAULT_SQS_RETENTION_SECONDS = "345600"
DEFAULT_SQS_VISIBILITY_TIMEOUT_SECONDS = 30

SNS_ARN_MARKER = "arn:aws:sns"
SQS_ARN_MARKER = "arn:aws:sqs"
SQS_URL_MARKER = "amazonaws.com"
SQS_FIFO_SUFFIX = ".fifo"

WILDCARD_SINGLE = "*"
WILDCARD_MULTI = "#"
ROUTING_SEPARATOR = "."

ROUTING_KEY_ATTR = "routing_key"
ROUTING_KEY_LENGTH_ATTR = "rk_len"
ROUTING_KEY_INDEX_PREFIX = "rk_idx_"
ROUTING_KEY_REVERSE_INDEX_PREFIX = "rk_ridx_"

AWS_STRING_DATA_TYPE = "String"
AWS_NUMBER_DATA_TYPE = "Number"
SNS_FILTER_POLICY_SCOPE_MESSAGE_ATTRIBUTES = "MessageAttributes"
SNS_PROTOCOL_SQS = "sqs"

RABBITMQ_QUEUE_PREFIX = "/queues/"
RABBITMQ_EXCHANGE_PREFIX = "/exchanges/"

SQS_RECEIPT_HANDLE_ATTR = "ReceiptHandle"
SQS_MESSAGE_ATTRIBUTES_KEY = "MessageAttributes"
SQS_BODY_KEY = "Body"
SNS_NOTIFICATION_TYPE = "Notification"
SNS_MESSAGE_KEY = "Message"

IAM_POLICY_VERSION = "2012-10-17"
SQS_SEND_MESSAGE_ACTION = "sqs:SendMessage"
SNS_SERVICE_PRINCIPAL = "sns.amazonaws.com"
SQS_POLICY_ATTRIBUTE = "Policy"
SQS_QUEUE_ARN_ATTRIBUTE = "QueueArn"
SQS_REDRIVE_POLICY_ATTRIBUTE = "RedrivePolicy"
SQS_RETENTION_ATTRIBUTE = "MessageRetentionPeriod"
SQS_VISIBILITY_ATTRIBUTE = "VisibilityTimeout"
DEAD_LETTER_TARGET_ARN_KEY = "deadLetterTargetArn"
MAX_RECEIVE_COUNT_KEY = "maxReceiveCount"

AWS_SOURCE_ARN_CONDITION = "aws:SourceArn"
ALLOW_SNS_SEND_SID_PREFIX = "AllowSnsSendMessage"
