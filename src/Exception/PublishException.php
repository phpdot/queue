<?php

declare(strict_types=1);

/**
 * PublishException
 *
 * Thrown when message publishing to an exchange fails.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Queue\Exception;

final class PublishException extends QueueException
{
    /**
     * Creates an exception for a failed publish operation.
     *
     * @param string $exchange The target exchange
     * @param string $routingKey The routing key used
     * @param string $error The underlying error message
     */
    public static function publishFailed(string $exchange, string $routingKey, string $error): self
    {
        return new self(
            sprintf('Failed to publish to exchange "%s" with routing key "%s": %s', $exchange, $routingKey, $error),
        );
    }

    /**
     * Creates an exception for a missing exchange.
     *
     * @param string $exchange The exchange name that was not found
     */
    public static function exchangeNotFound(string $exchange): self
    {
        return new self(
            sprintf('Exchange "%s" is not defined in configuration.', $exchange),
        );
    }

    /**
     * Creates an exception for a failed compression operation.
     */
    public static function compressionFailed(): self
    {
        return new self('Failed to compress message body.');
    }
}
