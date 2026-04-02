<?php

declare(strict_types=1);

namespace PHPdot\Queue\Tests\Config;

use PHPdot\Queue\Config\ConnectionConfig;
use PHPUnit\Framework\Attributes\Test;
use PHPUnit\Framework\TestCase;

final class ConnectionConfigTest extends TestCase
{
    #[Test]
    public function defaultHostIsLocalhost(): void
    {
        $config = new ConnectionConfig();

        self::assertSame('localhost', $config->host);
    }

    #[Test]
    public function defaultPortIs5672(): void
    {
        $config = new ConnectionConfig();

        self::assertSame(5672, $config->port);
    }

    #[Test]
    public function defaultUsernameIsGuest(): void
    {
        $config = new ConnectionConfig();

        self::assertSame('guest', $config->username);
    }

    #[Test]
    public function defaultPasswordIsGuest(): void
    {
        $config = new ConnectionConfig();

        self::assertSame('guest', $config->password);
    }

    #[Test]
    public function defaultVhostIsSlash(): void
    {
        $config = new ConnectionConfig();

        self::assertSame('/', $config->vhost);
    }

    #[Test]
    public function defaultTimeoutMsIs3000(): void
    {
        $config = new ConnectionConfig();

        self::assertSame(3000, $config->timeoutMs);
    }

    #[Test]
    public function defaultMaxRetriesIs3(): void
    {
        $config = new ConnectionConfig();

        self::assertSame(3, $config->maxRetries);
    }

    #[Test]
    public function defaultRetryDelayMsIs1000(): void
    {
        $config = new ConnectionConfig();

        self::assertSame(1000, $config->retryDelayMs);
    }

    #[Test]
    public function defaultExchangesIsEmptyArray(): void
    {
        $config = new ConnectionConfig();

        self::assertSame([], $config->exchanges);
    }

    #[Test]
    public function defaultQueuesIsEmptyArray(): void
    {
        $config = new ConnectionConfig();

        self::assertSame([], $config->queues);
    }

    #[Test]
    public function customValuesStoredCorrectly(): void
    {
        $exchanges = [
            'events' => ['type' => 'topic', 'durable' => true],
        ];
        $queues = [
            'notifications' => ['durable' => true],
        ];

        $config = new ConnectionConfig(
            host: '10.0.0.1',
            port: 5673,
            username: 'admin',
            password: 's3cret',
            vhost: '/production',
            timeoutMs: 5000,
            maxRetries: 5,
            retryDelayMs: 2000,
            exchanges: $exchanges,
            queues: $queues,
        );

        self::assertSame('10.0.0.1', $config->host);
        self::assertSame(5673, $config->port);
        self::assertSame('admin', $config->username);
        self::assertSame('s3cret', $config->password);
        self::assertSame('/production', $config->vhost);
        self::assertSame(5000, $config->timeoutMs);
        self::assertSame(5, $config->maxRetries);
        self::assertSame(2000, $config->retryDelayMs);
        self::assertSame($exchanges, $config->exchanges);
        self::assertSame($queues, $config->queues);
    }

    #[Test]
    public function allPropertiesAccessible(): void
    {
        $config = new ConnectionConfig();

        self::assertIsString($config->host);
        self::assertIsInt($config->port);
        self::assertIsString($config->username);
        self::assertIsString($config->password);
        self::assertIsString($config->vhost);
        self::assertIsInt($config->timeoutMs);
        self::assertIsInt($config->maxRetries);
        self::assertIsInt($config->retryDelayMs);
        self::assertIsArray($config->exchanges);
        self::assertIsArray($config->queues);
    }
}
