<?php

declare(strict_types=1);

/**
 * ConsumeException
 *
 * Thrown when message consumption from a queue fails.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Queue\Exception;

final class ConsumeException extends QueueException
{
    /**
     * Creates an exception for a missing queue.
     *
     * @param string $queue The queue name that was not found
     */
    public static function queueNotFound(string $queue): self
    {
        return new self(
            sprintf('Queue "%s" is not defined in configuration.', $queue),
        );
    }

    /**
     * Creates an exception for an invalid callback return type.
     */
    public static function invalidCallbackReturnType(): self
    {
        return new self('Consumer callback must return a TaskStatus enum.');
    }

    /**
     * Creates an exception for an invalid prefetch count.
     *
     * @param int $count The invalid prefetch count
     */
    public static function invalidPrefetchCount(int $count): self
    {
        return new self(
            sprintf('Prefetch count must be between 1 and 65535, got %d.', $count),
        );
    }

    /**
     * Creates an exception for a failed decompression operation.
     */
    public static function decompressFailed(): self
    {
        return new self('Failed to decompress message body.');
    }
}
