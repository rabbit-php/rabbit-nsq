<?php
declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Nsq\Wire\Writer;
use Rabbit\Socket\SocketClient;

/**
 * Class Consumer
 * @package Rabbit\Nsq
 */
class Consumer extends SocketClient
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