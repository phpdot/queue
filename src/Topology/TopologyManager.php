<?php

declare(strict_types=1);

/**
 * TopologyManager
 *
 * Declares exchanges, queues, bindings, and retry infrastructure on AMQP channels.
 * Caches declared state to avoid redundant declarations.
 *
 * @author Omar Hamdan <omar@phpdot.com>
 * @license MIT
 */

namespace PHPdot\Queue\Topology;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Wire\AMQPTable;
use PHPdot\Queue\Config\ConnectionConfig;
use PHPdot\Queue\Exception\ConsumeException;
use PHPdot\Queue\Exception\PublishException;

final class TopologyManager
{
    /** @var array<string, bool> */
    private array $declaredExchanges = [];

    /** @var array<string, bool> */
    private array $declaredQueues = [];

    /**
     * Creates a new topology manager.
     *
     * @param ConnectionConfig $config The connection configuration containing exchange and queue definitions
     */
    public function __construct(
        private readonly ConnectionConfig $config,
    ) {}

    /**
     * Prepares topology for publishing to an exchange.
     *
     * Declares the exchange and all queues bound to it. Skips if already declared.
     *
     * @param string $exchange The exchange name to prepare
     * @param AMQPChannel $channel The AMQP channel to declare on
     *
     *
     * @throws PublishException If the exchange is not defined in configuration
     */
    public function prepareForPublish(string $exchange, AMQPChannel $channel): void
    {
        if (isset($this->declaredExchanges[$exchange])) {
            return;
        }

        if (!isset($this->config->exchanges[$exchange])) {
            throw PublishException::exchangeNotFound($exchange);
        }

        $exchangeConfig = $this->config->exchanges[$exchange];

        $channel->exchange_declare(
            $exchange,
            $exchangeConfig['type'],
            false,
            $exchangeConfig['durable'] ?? true,
            $exchangeConfig['auto_delete'] ?? false,
        );

        $this->declaredExchanges[$exchange] = true;

        foreach ($this->config->queues as $queueName => $queueConfig) {
            $bindings = $queueConfig['bindings'] ?? [];

            foreach ($bindings as $binding) {
                if ($binding['exchange'] === $exchange) {
                    $this->declareQueue($queueName, $queueConfig, $channel);
                    $channel->queue_bind(
                        $queueName,
                        $exchange,
                        $binding['routing_key'] ?? '',
                    );
                }
            }
        }
    }

    /**
     * Prepares topology for consuming from a queue.
     *
     * Declares the queue, its bindings, and retry infrastructure if enabled.
     *
     * @param string $queue The queue name to prepare
     * @param AMQPChannel $channel The AMQP channel to declare on
     *
     *
     * @throws ConsumeException If the queue is not defined in configuration
     */
    public function prepareForConsume(string $queue, AMQPChannel $channel): void
    {
        if (isset($this->declaredQueues[$queue])) {
            return;
        }

        if (!isset($this->config->queues[$queue])) {
            throw ConsumeException::queueNotFound($queue);
        }

        $queueConfig = $this->config->queues[$queue];

        $this->declareQueue($queue, $queueConfig, $channel);

        $bindings = $queueConfig['bindings'] ?? [];

        foreach ($bindings as $binding) {
            $exchangeName = $binding['exchange'];

            if (!isset($this->declaredExchanges[$exchangeName]) && isset($this->config->exchanges[$exchangeName])) {
                $exchangeConfig = $this->config->exchanges[$exchangeName];

                $channel->exchange_declare(
                    $exchangeName,
                    $exchangeConfig['type'],
                    false,
                    $exchangeConfig['durable'] ?? true,
                    $exchangeConfig['auto_delete'] ?? false,
                );

                $this->declaredExchanges[$exchangeName] = true;
            }

            $channel->queue_bind(
                $queue,
                $exchangeName,
                $binding['routing_key'] ?? '',
            );
        }

        $retryConfig = $queueConfig['retry'] ?? [];
        $retryEnabled = $retryConfig['enabled'] ?? false;

        if ($retryEnabled) {
            $this->declareRetryInfrastructure($queue, $retryConfig, $channel);
        }

        $this->declaredQueues[$queue] = true;
    }

    /**
     * Resets all cached declaration state.
     */
    public function reset(): void
    {
        $this->declaredExchanges = [];
        $this->declaredQueues = [];
    }

    /**
     * Declares a queue on the given channel.
     *
     * @param string $name The queue name
     * @param array{durable?: bool, exclusive?: bool, auto_delete?: bool, arguments?: array<string, mixed>} $config The queue configuration
     * @param AMQPChannel $channel The AMQP channel
     */
    private function declareQueue(string $name, array $config, AMQPChannel $channel): void
    {
        $arguments = new AMQPTable($config['arguments'] ?? []);

        $channel->queue_declare(
            $name,
            false,
            $config['durable'] ?? true,
            $config['exclusive'] ?? false,
            $config['auto_delete'] ?? false,
            false,
            $arguments,
        );
    }

    /**
     * Declares retry exchange and retry queue with TTL and dead-letter routing.
     *
     * @param string $queue The original queue name
     * @param array{delay_ms?: int} $retryConfig The retry configuration
     * @param AMQPChannel $channel The AMQP channel
     */
    private function declareRetryInfrastructure(string $queue, array $retryConfig, AMQPChannel $channel): void
    {
        $retryExchange = $queue . '.retry.exchange';
        $retryQueue = $queue . '.retry';
        $delayMs = $retryConfig['delay_ms'] ?? 5000;

        $channel->exchange_declare(
            $retryExchange,
            'direct',
            false,
            true,
            false,
        );

        $this->declaredExchanges[$retryExchange] = true;

        $retryArguments = new AMQPTable([
            'x-message-ttl' => $delayMs,
            'x-dead-letter-exchange' => '',
            'x-dead-letter-routing-key' => $queue,
        ]);

        $channel->queue_declare(
            $retryQueue,
            false,
            true,
            false,
            false,
            false,
            $retryArguments,
        );

        $channel->queue_bind($retryQueue, $retryExchange, $queue);

        $this->declaredQueues[$retryQueue] = true;
    }
}
