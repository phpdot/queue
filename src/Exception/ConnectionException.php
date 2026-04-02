<?php

declare(strict_types=1);

/**
 * ConnectionException
 *
 * Thrown when RabbitMQ connection or channel operations fail.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Queue\Exception;

final class ConnectionException extends QueueException
{
    /**
     * Creates an exception for a failed connection attempt.
     *
     * @param string $host The host that was unreachable
     * @param int $attempts The number of connection attempts made
     * @param string $error The underlying error message
     */
    public static function connectionFailed(string $host, int $attempts, string $error): self
    {
        return new self(
            sprintf('Connection to "%s" failed after %d attempt(s): %s', $host, $attempts, $error),
        );
    }

    /**
     * Creates an exception for an uninitialized channel.
     */
    public static function channelNotInitialized(): self
    {
        return new self('AMQP channel is not initialized. Call connect() first.');
    }

    /**
     * Creates an exception for a failed reconnection.
     *
     * @param string $error The underlying error message
     */
    public static function reconnectFailed(string $error): self
    {
        return new self(
            sprintf('Reconnection failed: %s', $error),
        );
    }
}
