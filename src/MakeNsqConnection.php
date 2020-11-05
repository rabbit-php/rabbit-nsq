<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Pool\BaseManager;
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
        string $type,
        array $pool
    ): void {
        /** @var BaseManager $manager */
        $manager = getDI('nsq');
        if (!$manager->has($name)) {
            $conn = [
                $name => create([
                    'class' => NsqClient::class,
                    'dsnd' => $dsnd,
                    'pool' => create([
                        'class' => SocketPool::class,
                        'client' => $type,
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
            ];
            $manager->add($conn);
        }
    }
}
