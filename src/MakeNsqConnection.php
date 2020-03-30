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
     * @param array $pool
     * @param array $config
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(
        string $class,
        string $name,
        string $dsn,
        string $type,
        array $pool,
        array $config = null
    ): void
    {
        /** @var Manager $manager */
        $manager = getDI('nsq');
        if (!$manager->hasConnection($name)) {
            $conn = [
                $name => ObjectFactory::createObject([
                    'class' => $class,
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
            $manager->addConnection($conn);
        }
    }
}