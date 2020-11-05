<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\HttpClient\Client;
use Rabbit\Pool\ConnectionPool;
use Rabbit\Socket\SocketClient;
use Throwable;

/**
 * Class NsqClient
 * @package Rabbit\Nsq
 */
class NsqClient extends BaseObject
{
    private ConnectionPool $pool;
    private string $module = 'nsq';
    protected int $rdy = 1;
    protected float $timeout = 5;
    private Client $http;
    protected string $dsnd;

    protected array $created = [];

    /**
     * NsqClient constructor.
     * @param ConnectionPool $pool
     * @param string $dsnd
     */
    public function __construct(ConnectionPool $pool, string $dsnd)
    {
        $this->pool = $pool;
        $this->dsnd = $dsnd;
        $this->http = new Client();
    }

    /**
     * @throws Throwable
     */
    public function setTopicAdd(string $topic, string $channel): void
    {
        if (in_array("$topic:$channel", $this->created)) {
            return;
        }
        while (true) {
            try {
                $response = $this->http->get($this->pool->getConnectionAddress() . '/lookup', ['uri_query' => ['topic' => $topic]]);
                if ($response->getStatusCode() === 200) {
                    $data = $response->jsonArray();
                    foreach ($data['channels'] as $chl) {
                        if ($chl === $channel && $data['producers']) {
                            $product = current($data['producers']);
                            $this->pool->getPoolConfig()->setUri($product['broadcast_address'] . ':' . $product['tcp_port']);
                            $this->created[] = "$topic:$chl";
                            return;
                        }
                    }
                    break;
                } else {
                    sleep($this->pool->getPoolConfig()->getMaxWait());
                }
            } catch (Throwable $exception) {
                if ($exception->getCode() === 404) {
                    $this->createTopic($topic, $channel);
                }
            }
        }
    }

    /**
     * @return void
     * @throws Throwable
     */
    public function createTopic(string $topic, string $channel): void
    {
        $response = $this->http->post($this->dsnd . '/topic/create', ['uri_query' => ['topic' => $topic]]);
        if ($response->getStatusCode() === 200) {
            App::info("Create topic $topic success!");
        }
        $response = $this->http->post($this->dsnd . '/channel/create', ['uri_query' => ['topic' => $topic, 'channel' => $channel]]);
        if ($response->getStatusCode() === 200) {
            App::info("Create topic $topic channel $channel success!");
        }
        $this->created[] = "$topic:$channel";
    }

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
            $reader = (new Reader($this->pool->getTimeout()))->bindFrame($connection);
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
            $reader = (new Reader($this->pool->getTimeout()))->bindFrame($connection);
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
            $reader = (new Reader($this->pool->getTimeout()))->bindFrame($connection);
            $connection->release();
            return $reader->getFrame();
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
        return null;
    }

    /**
     * @param array $config
     * @param Closure $callback
     * @throws Throwable
     */
    public function subscribe(string $topic, string $channel, array $config, Closure $callback): void
    {
        try {
            /** @var Consumer $connection */
            for ($i = 0; $i < $this->pool->getPoolConfig()->getMinActive(); $i++) {
                loop(function () use ($topic, $channel, $config, $callback) {
                    $connection = $this->pool->get();
                    $this->pool->sub();
                    $connection->send(Writer::sub($topic, $channel));
                    $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
                    while (true) {
                        if (!$this->handleMessage($connection, $config, $callback)) {
                            break;
                        }
                    }
                    sleep($this->pool->getPoolConfig()->getMaxWait());
                });
            }
        } catch (\Exception $e) {
            App::error("subscribe error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param SocketClient $connection
     * @param array $config
     * @param Closure $callback
     * @return bool
     * @throws Throwable
     */
    private function handleMessage(SocketClient $connection, array $config, Closure $callback): bool
    {
        try {
            $reader = (new Reader(-1))->bindFrame($connection);
        } catch (\Throwable $throwable) {
            return false;
        }

        if ($reader->isHeartbeat()) {
            $connection->send(Writer::nop());
        } elseif ($reader->isMessage()) {
            $msg = $reader->getFrame();
            rgo(function () use ($connection, $config, $callback, &$msg) {
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
            });
        } elseif ($reader->isOk()) {
            App::info('Ignoring "OK" frame in SUB loop', $this->module);
        } else {
            App::error("Error/unexpected frame received: =" . VarDumper::getDumper()->dumpAsString($reader->getFrame()), $this->module);
        }
        return true;
    }
}
