<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 9:22
 */

namespace rabbit\nsq\pool;


use rabbit\pool\ConnectionInterface;
use rabbit\pool\ConnectionPool;
use rabbit\socket\TcpClient;

/**
 * Class NsqPool
 * @package rabbit\nsq\pool
 */
class NsqPool extends ConnectionPool
{
    /**
     * @return ConnectionInterface
     */
    public function createConnection(): ConnectionInterface
    {
        return new TcpClient($this);
    }
}