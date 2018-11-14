<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/14
 * Time: 3:09
 */

namespace rabbit\nsq;


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
        $this->send(Writer::MAGIC_V2);
    }
}