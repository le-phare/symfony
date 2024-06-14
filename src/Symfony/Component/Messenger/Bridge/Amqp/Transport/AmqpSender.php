<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Bridge\Amqp\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * Symfony Messenger sender to send messages to AMQP brokers using PHP's AMQP extension.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class AmqpSender implements SenderInterface
{
    private SerializerInterface $serializer;
    private Connection $connection;

    public function __construct(Connection $connection, ?SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delay = $delayStamp ? $delayStamp->getDelay() : 0;

        /** @var AmqpStamp|null $amqpStamp */
        $amqpStamp = $envelope->last(AmqpStamp::class);
        if (isset($encodedMessage['headers']['Content-Type'])) {
            $contentType = $encodedMessage['headers']['Content-Type'];
            unset($encodedMessage['headers']['Content-Type']);

            if (!$amqpStamp || !isset($amqpStamp->getAttributes()['content_type'])) {
                $amqpStamp = AmqpStamp::createWithAttributes(['content_type' => $contentType], $amqpStamp);
            }
        }

        $amqpReceivedStamp = $envelope->last(AmqpReceivedStamp::class);
        if ($amqpReceivedStamp instanceof AmqpReceivedStamp) {
            $retryRoutingKey = null;
            if ($envelope->last(RedeliveryStamp::class)) {

                // dump($envelope->last(RedeliveryStamp::class));
                $retryRoutingKey = $envelope->last(RedeliveryStamp::class)->isRetryToOriginalExchange() ? $amqpReceivedStamp->getAmqpEnvelope()->getRoutingKey() : $amqpReceivedStamp->getQueueName();
            }
            $amqpStamp = AmqpStamp::createFromAmqpEnvelope(
                $amqpReceivedStamp->getAmqpEnvelope(),
                $amqpStamp,
                // TODO:
                $retryRoutingKey,
                $deadLetterExchange = $amqpReceivedStamp->getAmqpEnvelope()->getExchangeName(),
            );
        }

        try {
            $this->connection->publish(
                $encodedMessage['body'],
                $encodedMessage['headers'] ?? [],
                $delay,
                $amqpStamp
            );
        } catch (\AMQPException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        return $envelope;
    }
}
