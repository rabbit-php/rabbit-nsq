<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 11:10
 */

namespace rabbit\nsq;

use rabbit\App;
use rabbit\helper\CoroHelper;
use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\ConnectionPool;
use rabbit\socket\pool\SocketPool;
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
            /** @var SocketPool $pool */
            $pool = $config['pool'];
            unset($config['pool']);
            /** @var Consumer $connection */
            for ($i = 0; $i < $pool->getPoolConfig()->getMinActive(); $i++) {
                $this->doRun($pool, $topic, $channel, $config, $callback);
            }
        } catch (\Exception $e) {
            App::error("subscribe error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param SocketPool $pool
     * @param string $topic
     * @param string $channel
     * @param array $config
     * @param \Closure $callback
     */
    private function doRun(SocketPool $pool, string $topic, string $channel, array $config, \Closure $callback): void
    {
        try{
            $connection = $pool->getConnection();
        }catch (\Throwable $throwable){
            App::error($throwable->getMessage());
            CoroHelper::sleep($pool->getPoolConfig()->getMaxWaitTime());
            $pool->setCurrentCount();
            $this->doRun($pool, $topic, $channel, $config, $callback);
        }

        go(function () use ($pool, $connection, $config, $callback, $topic, $channel) {
            $connection->send(Writer::sub($topic, $channel));
            $connection->send(Writer::rdy($config['rdy'] ?? 1));
            while (true) {
                if (!$this->handleMessage($connection, $config, $callback)) {
                    break;
                }
            }
            CoroHelper::sleep($pool->getPoolConfig()->getMaxWaitTime());
            $pool->setCurrentCount();
            $this->doRun($pool, $topic, $channel, $config, $callback);
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
            $msg = $reader->getMessage($connection);
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
            App::error("Error/unexpected frame received: =" . $reader->getMessage($connection), $this->module);
        }
        return true;
    }
}