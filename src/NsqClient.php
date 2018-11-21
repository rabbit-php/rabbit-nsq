<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 11:10
 */

namespace rabbit\nsq;

use rabbit\App;
use rabbit\nsq\pool\AsyncNsqPool;
use rabbit\nsq\pool\NsqPool;
use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\socket\tcp\AsyncTcp;

/**
 * Class NsqClient
 * @package rabbit\nsq
 */
class NsqClient
{
    /** @var NsqPool */
    private $pubPool;
    /** @var NsqPool */
    private $subPool;
    /**
     * @var string
     */
    private $module = 'nsq';

    /**
     * NsqClient constructor.
     * @param NsqPool $productPool
     * @param AsyncNsqPool $consumerPool
     */
    public function __construct(NsqPool $pubPool, NsqPool $subPool)
    {
        $this->pubPool = $pubPool;
        $this->subPool = $subPool;
    }

    /**
     * @param string $topic
     * @param string $message
     * @return NsqResult
     * @throws \Exception
     */
    public function publish(string $topic, string $message): NsqResult
    {
        try {
            $connection = $this->pubPool->getConnection();
            $result = $connection->send(Writer::pub($topic, $message));
            return new NsqResult($connection, $result);
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param mixed ...$bodies
     * @return NsqResult
     * @throws \Exception
     */
    public function publishMulti(string $topic, ...$bodies): NsqResult
    {
        try {
            $connection = $this->pubPool->getConnection();
            $result = $connection->send(Writer::mpub($topic, $bodies));
            return new NsqResult($connection, $result);
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param string $message
     * @param int $deferTime
     * @return NsqResult
     * @throws \Exception
     */
    public function publishDefer(string $topic, string $message, int $deferTime): NsqResult
    {
        try {
            $connection = $this->pubPool->getConnection();
            $result = $connection->send(Writer::dpub($topic, $deferTime, $message));
            return new NsqResult($connection, $result);
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param string $channel
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    public function subscribe(string $topic, string $channel, array $config, \Closure $callback): void
    {
        try {
            /** @var NsqPool $pool */
            $pool = $config['pool'];
            unset($config['pool']);
            /** @var Tcp $connection */
            for ($i = 0; $i < $pool->getPoolConfig()->getMinActive(); $i++) {
                $connection = $pool->getConnection();
                go(function () use ($connection, $config, $callback) {
                    while (true) {
                        $reader = (new Reader($connection->receive()))->bindFrame();
                        $this->handleMessage($connection, $reader, $config, $callback);
                    }
                });
                $connection->send(Writer::sub($topic, $channel));
                $connection->send(Writer::rdy($config['rdy'] ?? 1));
            }
        } catch (\Exception $e) {
            App::error("subscribe error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $body
     * @param \Swoole\Client $connection
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    private function handleMessage(Tcp $connection, Reader $reader, array $config, \Closure $callback): void
    {
        if ($reader->isHeartbeat()) {
            $connection->send(Writer::nop());
        } elseif ($reader->isMessage()) {
            $msg = $reader->getMessage();
            try {
                call_user_func($callback, $msg);
            } catch (\Exception $e) {
                App::error("Will be requeued: " . $e->getMessage(), $this->module);
                $connection->send(Writer::touch($msg->getId()));
                $connection->send(Writer::req(
                    $msg->getId(),
                    $config['timeout'] ? $config['timeout'] . 's' : '5s'
                ));
            }
            $connection->send(Writer::fin($msg->getId()));
            $connection->send(Writer::rdy($config['rdy'] ?? 1));
        } elseif ($reader->isOk()) {
            App::info('Ignoring "OK" frame in SUB loop', $this->module);
        } else {
            App::error("Error/unexpected frame received: =" . $reader->getMessage(), $this->module);
        }
    }
}