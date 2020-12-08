<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Base\App;
use Rabbit\Base\Core\Exception;
use Throwable;

class Producer extends AbstractNsq
{
    /**
     * @param string $message
     * @return array|null
     * @throws Throwable
     */
    public function publish(string $topic, string $message): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::pub($topic, $message));
            $reader = (new Reader($connection))->bindFrame();
            $connection->release();
            return $reader->getFrame();
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
        return null;
    }

    /**
     * @param array $bodies
     * @return array|null
     * @throws Throwable
     */
    public function publishMulti(string $topic, array $bodies): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::mpub($topic, $bodies));
            $reader = (new Reader($connection))->bindFrame();
            $connection->release();
            return $reader->getFrame();
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
        return null;
    }

    /**
     * @param string $message
     * @param int $deferTime
     * @return array|null
     * @throws Throwable
     */
    public function publishDefer(string $topic, string $message, int $deferTime): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::dpub($topic, $deferTime, $message));
            $reader = (new Reader($connection))->bindFrame();
            $connection->release();
            return $reader->getFrame();
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
        return null;
    }
}
