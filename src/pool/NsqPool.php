<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 9:22
 */

namespace rabbit\nsq\pool;


use rabbit\nsq\Tcp;
use rabbit\pool\ConnectionInterface;
use rabbit\socket\pool\SocketPool;

/**
 * Class NsqPool
 * @package rabbit\nsq\pool
 */
class NsqPool extends SocketPool
{
    /**
     * @return ConnectionInterface
     */
    public function createConnection(): ConnectionInterface
    {
        return new Tcp($this);
    }
}