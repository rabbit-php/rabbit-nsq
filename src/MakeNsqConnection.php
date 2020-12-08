<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Socket\Pool\SocketConfig;
use Rabbit\Socket\pool\SocketPool;

/**
 * Class MakeNsqConnection
 * @package rabbit\nsq
 */
class MakeNsqConnection
{
    /**
     * @Author Albert 63851587@qq.com
     * @DateTime 2020-11-05
     * @param string $name
     * @param string $dsn
     * @param string $dsnd
     * @param string $type
     * @param array $pool
     * @return void
     */
    public static function addConnection(
        string $name,
        string $dsn,
        string $dsnd,
        ?string $type,
        array $pool
    ): void {
        /** @var NsqManager $manager */
        $manager = getDI('nsq');
        if (!$manager->has($name)) {
            switch ($type) {
                case 'consumer':
                    $conn = [
                        $name => [
                            'consumer' => create([
                                'class' => Consumer::class,
                                'dsnd' => $dsnd,
                                'pool' => create([
                                    'class' => SocketPool::class,
                                    'client' => ConsumerClient::class,
                                    'poolConfig' => create([
                                        'class' => SocketConfig::class,
                                        'minActive' => $pool['min'],
                                        'maxActive' => $pool['max'],
                                        'maxWait' => $pool['wait'],
                                        'maxReconnect' => $pool['retry'],
                                        'uri' => [$dsn]
                                    ], [], false)
                                ], [], false)
                            ], [], false),
                            'producer' => null
                        ]
                    ];
                    break;
                case 'producer':
                    $conn = [
                        $name => [
                            'consumer' => null,
                            'producer' => create([
                                'class' => Producer::class,
                                'pool' => create([
                                    'class' => SocketPool::class,
                                    'client' => ProducerClient::class,
                                    'poolConfig' => create([
                                        'class' => SocketConfig::class,
                                        'minActive' => $pool['min'],
                                        'maxActive' => $pool['max'],
                                        'maxWait' => $pool['wait'],
                                        'maxReconnect' => $pool['retry'],
                                        'uri' => [$dsn]
                                    ], [], false)
                                ], [], false)
                            ], [], false)
                        ]
                    ];
                    break;
                default:
                    $conn = [
                        $name => [
                            'consumer' => create([
                                'class' => Consumer::class,
                                'dsnd' => $dsnd,
                                'pool' => create([
                                    'class' => SocketPool::class,
                                    'client' => ConsumerClient::class,
                                    'poolConfig' => create([
                                        'class' => SocketConfig::class,
                                        'minActive' => $pool['min'],
                                        'maxActive' => $pool['max'],
                                        'maxWait' => $pool['wait'],
                                        'maxReconnect' => $pool['retry'],
                                        'uri' => [$dsn]
                                    ], [], false)
                                ], [], false)
                            ], [], false),
                            'producer' => create([
                                'class' => Producer::class,
                                'pool' => create([
                                    'class' => SocketPool::class,
                                    'client' => ProducerClient::class,
                                    'poolConfig' => create([
                                        'class' => SocketConfig::class,
                                        'minActive' => $pool['min'],
                                        'maxActive' => $pool['max'],
                                        'maxWait' => $pool['wait'],
                                        'maxReconnect' => $pool['retry'],
                                        'uri' => [$dsn]
                                    ], [], false)
                                ], [], false)
                            ], [], false)
                        ]
                    ];
            }

            $manager->add($conn);
        }
    }
}
