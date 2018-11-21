<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/14
 * Time: 3:09
 */

namespace rabbit\nsq;


use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\PoolInterface;
use rabbit\socket\TcpClient;

/**
 * Class Tcp
 * @package rabbit\nsq
 */
class Tcp extends TcpClient
{
    /**
     * Tcp constructor.
     * @param PoolInterface $connectPool
     */
    public function __construct(PoolInterface $connectPool)
    {
        parent::__construct($connectPool);
        $this->connection->send(Writer::MAGIC_V2);

        if (!$connectPool->getHeartbeat()) {
            //禁用心跳
            $this->connection->send(Writer::identify(["heartbeat_interval" => -1]));
            $result = $this->connection->recv($this->pool->getTimeout());
            $reader = (new Reader($result))->bindFrame();
            if (!$reader->isOk()) {
                throw new \RuntimeException('set identify error');
            }
        }
    }
}