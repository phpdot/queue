<?php

declare(strict_types=1);

/**
 * Message
 *
 * Immutable inbound message data transfer object wrapping an AMQP message.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Queue;

use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

final class Message
{
    /**
     * @param string $body The raw message body
     * @param string $messageId The message identifier
     * @param string $queue The queue this message was consumed from
     * @param array<string, mixed> $headers The message headers
     * @param array<string, mixed> $properties The message properties
     * @param int $priority The message priority
     */
    private function __construct(
        private readonly string $body,
        private readonly string $messageId,
        private readonly string $queue,
        private readonly array $headers,
        private readonly array $properties,
        private readonly int $priority,
    ) {}

    /**
     * Creates a Message instance from an AMQPMessage.
     *
     * @param AMQPMessage $msg The AMQP message to wrap
     * @param string $queue The queue name this message was consumed from
     */
    public static function fromAMQP(AMQPMessage $msg, string $queue): self
    {
        $headers = [];

        if ($msg->has('application_headers')) {
            /** @var AMQPTable $applicationHeaders */
            $applicationHeaders = $msg->get('application_headers');
            $headers = self::convertAmqpTableToArray($applicationHeaders);
        }

        $messageId = '';
        if ($msg->has('message_id')) {
            /** @var string $rawMessageId */
            $rawMessageId = $msg->get('message_id');
            $messageId = $rawMessageId;
        }

        $priority = 0;
        if ($msg->has('priority')) {
            /** @var int $rawPriority */
            $rawPriority = $msg->get('priority');
            $priority = $rawPriority;
        }

        $properties = [];
        if ($msg->has('content_type')) {
            /** @var string $contentType */
            $contentType = $msg->get('content_type');
            $properties['content_type'] = $contentType;
        }
        if ($msg->has('content_encoding')) {
            /** @var string $contentEncoding */
            $contentEncoding = $msg->get('content_encoding');
            $properties['content_encoding'] = $contentEncoding;
        }
        if ($msg->has('delivery_mode')) {
            /** @var int $deliveryMode */
            $deliveryMode = $msg->get('delivery_mode');
            $properties['delivery_mode'] = $deliveryMode;
        }
        if ($msg->has('timestamp')) {
            /** @var int $timestamp */
            $timestamp = $msg->get('timestamp');
            $properties['timestamp'] = $timestamp;
        }
        if ($msg->has('app_id')) {
            /** @var string $appId */
            $appId = $msg->get('app_id');
            $properties['app_id'] = $appId;
        }

        return new self(
            $msg->getBody(),
            $messageId,
            $queue,
            $headers,
            $properties,
            $priority,
        );
    }

    /**
     * Returns the raw message body.
     */
    public function body(): string
    {
        return $this->body;
    }

    /**
     * Returns the message identifier.
     */
    public function messageId(): string
    {
        return $this->messageId;
    }

    /**
     * Returns the queue this message was consumed from.
     */
    public function queue(): string
    {
        return $this->queue;
    }

    /**
     * Returns a single header value by key, or the default if not found.
     *
     * @param string $key The header key
     * @param mixed $default The default value if the header is not set
     */
    public function header(string $key, mixed $default = null): mixed
    {
        return $this->headers[$key] ?? $default;
    }

    /**
     * Returns all message headers.
     *
     * @return array<string, mixed>
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * Returns the maximum retry count from headers.
     */
    public function maxRetries(): int
    {
        $value = $this->headers['x-retries-max'] ?? 0;

        return is_int($value) ? $value : (is_numeric($value) ? intval($value) : 0);
    }

    /**
     * Returns the original exchange from headers.
     */
    public function originalExchange(): string
    {
        $value = $this->headers['x-original-exchange'] ?? '';

        return is_string($value) ? $value : '';
    }

    /**
     * Returns the original routing key from headers.
     */
    public function originalRoutingKey(): string
    {
        $value = $this->headers['x-original-routing-key'] ?? '';

        return is_string($value) ? $value : '';
    }

    /**
     * Returns the failure reason from headers.
     */
    public function failedReason(): string
    {
        $value = $this->headers['x-failed-reason'] ?? '';

        return is_string($value) ? $value : '';
    }

    /**
     * Returns all message properties.
     *
     * @return array<string, mixed>
     */
    public function properties(): array
    {
        return $this->properties;
    }

    /**
     * Returns the message priority.
     */
    public function priority(): int
    {
        return $this->priority;
    }

    /**
     * Converts an AMQPTable to a plain associative array.
     *
     * @param AMQPTable $table The AMQP table to convert
     *
     * @return array<string, mixed>
     */
    private static function convertAmqpTableToArray(AMQPTable $table): array
    {
        /** @var array<string, mixed> $data */
        $data = $table->getNativeData();

        return $data;
    }
}
