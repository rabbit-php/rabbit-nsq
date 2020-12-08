<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Nsq\Consumer;
use Rabbit\Nsq\Producer;
use Rabbit\Pool\BaseManager;

class NsqManager extends BaseManager
{
    public function getProducer(string $name = 'default'): ?Producer
    {
        if (!isset($this->items[$name])) {
            return null;
        }
        return $this->items[$name]['producer'] ?? null;
    }

    public function getConsumer(string $name = 'default'): ?Consumer
    {
        if (!isset($this->items[$name])) {
            return null;
        }
        return isset($this->items[$name]['consumer']) ? clone $this->items[$name]['consumer'] : null;
    }
}
