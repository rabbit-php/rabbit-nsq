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
use rabbit\socket\AsyncTcp;

/**
 * Class NsqClient
 * @package rabbit\nsq
 */
class NsqClient
{
    /** @var NsqPool */
    private $pool;

    /** @var AsyncTcp */
    private $asyncTcp;

    /**
     * @var string
     */
    private $module = 'nsq';

    /**
     * NsqClient constructor.
     * @param NsqPool $productPool
     * @param AsyncNsqPool $consumerPool
     */
    public function __construct(NsqPool $pool, AsyncTcp $asyncTcp)
    {
        $this->pool = $pool;
        $this->asyncTcp = $asyncTcp;
    }

    /**
     * @param string $message
     * @throws \Exception
     */
    public function publish(string $topic, string $message): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::pub($topic, $message));
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param mixed ...$bodies
     * @throws \Exception
     */
    public function publishMulti(string $topic, ...$bodies): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::mpub($topic, $bodies));
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $message
     * @param int $deferTime
     * @throws \Exception
     */
    public function publishDefer(string $topic, string $message, int $deferTime): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::dpub($topic, $deferTime, $message));
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $topic
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    public function subscribe(string $topic, string $channel, array $config, \Closure $callback): void
    {
        try {
            $topicChannel = implode(':', [$topic, $channel]);
            $this->asyncTcp->on('connect', function (\Swoole\Client $cli) use ($topic, $channel, $config) {
                $cli->send(Writer::MAGIC_V2);
                $cli->send(Writer::sub($topic, $channel));
                $cli->send(Writer::rdy($config['rdy'] ?? 1));
            })->on('receive', function (\Swoole\Client $cli, string $body) use ($config, $callback) {
                $this->handleMessage($cli, $body, $config, $callback);
            })->createConnection($topicChannel);
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
    private function handleMessage(\Swoole\Client $connection, string $body, array $config, \Closure $callback): void
    {
        $reader = (new Reader($body))->bindFrame();
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