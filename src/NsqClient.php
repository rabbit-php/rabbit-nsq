<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 11:10
 */

namespace rabbit\nsq;

use Co\System;
use rabbit\App;
use rabbit\helper\ArrayHelper;
use rabbit\helper\VarDumper;
use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\ConnectionPool;
use rabbit\socket\SocketClient;

/**
 * Class NsqClient
 * @package rabbit\nsq
 */
class NsqClient
{
    /** @var NsqPool */
    private $pool;
    /**
     * @var string
     */
    private $module = 'nsq';
    /** @var int */
    protected $rdy = 1;
    /** @var float */
    protected $timeout = 5;

    /**
     * NsqClient constructor.
     * @param ConnectionPool $pool
     */
    public function __construct(ConnectionPool $pool)
    {
        $this->pool = $pool;
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
            $connection = $this->pool->getConnection();
            $result = $connection->send(Writer::pub($topic, $message));
            return new NsqResult($connection, $result);
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param array $bodies
     * @return NsqResult
     * @throws \Exception
     */
    public function publishMulti(string $topic, array $bodies): NsqResult
    {
        try {
            $connection = $this->pool->getConnection();
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
            $connection = $this->pool->getConnection();
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
            /** @var Consumer $connection */
            for ($i = 0; $i < $this->pool->getPoolConfig()->getMinActive(); $i++) {
                $this->doRun($topic, $channel, $config, $callback);
            }
        } catch (\Exception $e) {
            App::error("subscribe error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param string $channel
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    private function doRun(string $topic, string $channel, array $config, \Closure $callback): void
    {
        $connection = $this->pool->getConnection();
        rgo(function () use ($connection, $config, $callback, $topic, $channel) {
            $connection->send(Writer::sub($topic, $channel));
            $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
            while (true) {
                if (!$this->handleMessage($connection, $config, $callback)) {
                    break;
                }
            }
            System::sleep($this->pool->getPoolConfig()->getMaxWaitTime());
            $this->pool->setCurrentCount();
            $this->doRun($this->pool, $topic, $channel, $config, $callback);
        });
    }

    /**
     * @param string $body
     * @param \Swoole\Client $connection
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    private function handleMessage(SocketClient $connection, array $config, \Closure $callback): bool
    {
        try {
            $reader = (new Reader(-1))->bindFrame($connection);
        } catch (ConnectionException $throwable) {
            return false;
        }

        if ($reader->isHeartbeat()) {
            $connection->send(Writer::nop());
        } elseif ($reader->isMessage()) {
            $msg = $reader->getFrame();
            try {
                call_user_func($callback, $msg);
            } catch (\Exception $e) {
                App::error("Will be requeued: " . $e->getMessage(), $this->module);
                $connection->send(Writer::touch($msg['id']));
                $connection->send(Writer::req(
                    $msg['id'],
                    ArrayHelper::getValue($config, 'timeout', $this->timeout)
                ));
            }
            $connection->send(Writer::fin($msg['id']));
            $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
        } elseif ($reader->isOk()) {
            App::info('Ignoring "OK" frame in SUB loop', $this->module);
        } else {
            App::error("Error/unexpected frame received: =" . VarDumper::getDumper()->dumpAsString($reader->getFrame()), $this->module);
        }
        return true;
    }
}