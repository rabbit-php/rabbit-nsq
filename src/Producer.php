<?php
declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Socket\SocketClient;
use Throwable;

/**
 * Class Producer
 * @package Rabbit\Nsq
 */
class Producer extends SocketClient
{
    /**
     * Producer constructor.
     * @param string $poolKey
     * @throws Throwable
     */
    public function __construct(string $poolKey)
    {
        parent::__construct($poolKey);
        $this->send(Writer::MAGIC_V2);
        //禁用心跳
        $this->send(Writer::identify(["heartbeat_interval" => -1]));
        $reader = (new Reader(-1))->bindFrame($this);
        if (!$reader->isOk()) {
            throw new \RuntimeException('set identify error');
        }
    }
}