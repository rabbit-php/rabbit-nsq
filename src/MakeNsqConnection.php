<?php
declare(strict_types=1);

namespace Rabbit\Nsq;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Pool\BaseManager;
use Rabbit\Socket\Pool\SocketConfig;
use Rabbit\Socket\pool\SocketPool;
use ReflectionException;
use Throwable;

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
     * @throws DependencyException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
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
        /** @var BaseManager $manager */
        $manager = getDI('nsq');
        if (!$manager->has($name)) {
            $conn = [
                $name => create([
                    'class' => NsqClient::class,
                    'dsnd' => $dsnd,
                    'topic' => $name,
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
                ], [], false)];
            $manager->add($conn);
        }
    }
}