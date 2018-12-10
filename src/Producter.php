<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/12/9
 * Time: 18:01
 */

namespace rabbit\nsq;

use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\PoolInterface;
use rabbit\socket\SocketClient;

/**
 * Class Producter
 * @package rabbit\nsq
 */
class Producter extends SocketClient
{
    /**
     * Tcp constructor.
     * @param PoolInterface $connectPool
     */
    public function __construct(PoolInterface $connectPool)
    {
        parent::__construct($connectPool);
        $this->send(Writer::MAGIC_V2);
        //禁用心跳
        $this->send(Writer::identify(["heartbeat_interval" => -1]));
        $reader = (new Reader(-1))->bindFrame($this);
        if (!$reader->isOk()) {
            throw new \RuntimeException('set identify error');
        }
    }
}