<?php
declare(strict_types=1);

namespace rabbit\nsq;

use rabbit\pool\BaseManager;
use rabbit\socket\pool\SocketPool;

/**
 * Class Manager
 * @package rabbit\nsq
 */
class Manager extends BaseManager
{
    /**
     * @param string $name
     * @return NsqClient|null
     */
    public function getConnection(string $name = 'nsq'): ?NsqClient
    {
        if (!isset($this->connections[$name])) {
            return null;
        }
        return $this->connections[$name];
    }
}
