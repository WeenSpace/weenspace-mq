# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2026-08-25

### Added
- Initial release of weenspace-queue
- Based on official rabbitmq-amqp-python-client v0.4.0
- Full AMQP 1.0 protocol support
- OAuth2 authentication support
- TLS/SSL encryption support
- Dead Letter Queue (DLQ) configuration
- Priority Queue support
- Classic, Quorum, and Stream queue types
- Async/await support via asyncio module
- Auto-reconnection with configurable backoff
- All exchange types (direct, fanout, topic, headers)

### Changed
- Renamed package from rabbitmq-amqp-python-client to weenspace-queue
- Updated module imports to use weenspace_queue namespace

### Credits
- Based on [rabbitmq-amqp-python-client](https://github.com/rabbitmq/rabbitmq-amqp-python-client) by RabbitMQ team
