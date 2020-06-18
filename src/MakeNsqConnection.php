<?php
declare(strict_types=1);

namespace rabbit\nsq;

use rabbit\core\ObjectFactory;
use rabbit\socket\pool\SocketConfig;
use rabbit\socket\pool\SocketPool;

/**
 * Class MakeNsqConnection
 * @package rabbit\nsq
 */
class MakeNsqConnection
{
    /**
     * @param string $name
     * @param string $dsn
     * @param string $dsnd
     * @param string $type
     * @param array $pool
     * @param array|null $config
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(
        string $name,
        string $dsn,
        string $dsnd,
        string $type,
        array $pool,
        array $config = null
    ): void
    {
        /** @var Manager $manager */
        $manager = getDI('nsq');
        if (!$manager->has($name)) {
            $conn = [
                $name => ObjectFactory::createObject([
                    'class' => NsqClient::class,
                    'dsnd' => $dsnd,
                    'topic' => $name,
                    'pool' => ObjectFactory::createObject([
                        'class' => SocketPool::class,
                        'client' => $type,
                        'poolConfig' => ObjectFactory::createObject([
                            'class' => SocketConfig::class,
                            'minActive' => $pool['min'],
                            'maxActive' => $pool['max'],
                            'maxWait' => $pool['wait'],
                            'maxReconnect' => $pool['retry'],
                            'uri' => [$dsn]
                        ], [], false)
                    ], [], false)
                ], [], false)];
            $manager->add($conn);
        }
    }
}