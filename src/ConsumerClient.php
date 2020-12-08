<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Socket\SocketClient;

class ConsumerClient extends SocketClient
{
    /**
     * Consumer constructor.
     * @param string $poolKey
     */
    public function __construct(string $poolKey)
    {
        parent::__construct($poolKey);
        $this->send(Writer::MAGIC_V2);
    }
}
