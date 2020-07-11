<?php
declare(strict_types=1);

namespace Rabbit\Nsq;

use Closure;
use Co\System;
use Rabbit\Base\App;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\HttpClient\Client;
use Rabbit\Nsq\Wire\Reader;
use Rabbit\Nsq\Wire\Writer;
use Rabbit\Pool\ConnectionPool;
use Rabbit\Socket\SocketClient;
use Throwable;

/**
 * Class NsqClient
 * @package Rabbit\Nsq
 */
class NsqClient extends BaseObject implements InitInterface
{
    private ConnectionPool $pool;
    /**
     * @var string
     */
    private $module = 'nsq';
    /** @var int */
    protected $rdy = 1;
    /** @var float */
    protected $timeout = 5;
    /** @var Client */
    private Client $http;
    /** @var string */
    protected string $dsnd;
    /** @var string */
    protected ?string $topic;
    protected ?string $channel;

    /**
     * NsqClient constructor.
     * @param ConnectionPool $pool
     * @param string $dsnd
     */
    public function __construct(ConnectionPool $pool, string $dsnd)
    {
        $this->pool = $pool;
        $this->dsnd = $dsnd;
        $this->http = new Client([], 'saber', true);
    }

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init()
    {
        $param = explode(':', $this->topic);
        $this->topic = isset($param[0]) ? $param[0] : null;
        $this->channel = isset($param[1]) ? $param[1] : null;
        if (empty($this->topic)) {
            throw new InvalidArgumentException("topic must be set");
        }
        $this->setTopicAdd();
    }

    /**
     * @throws Throwable
     */
    public function setTopicAdd(): void
    {
        while (true) {
            try {
                $response = $this->http->get($this->pool->getConnectionAddress() . '/lookup', ['uri_query' => ['topic' => $this->topic]]);
                if ($response->getStatusCode() === 200) {
                    $data = $response->jsonArray();
                    foreach ($data['channels'] as $chl) {
                        if ($chl === $this->channel && $data['producers']) {
                            $product = current($data['producers']);
                            $this->pool->getPoolConfig()->setUri($product['broadcast_address'] . ':' . $product['tcp_port']);
                            return;
                        }
                    }
                    break;
                } else {
                    System::sleep($this->pool->getPoolConfig()->getMaxWait());
                }
            } catch (Throwable $exception) {
                if ($exception->getCode() === 404) {
                    $this->createTopic();
                }
            }
        }
    }

    /**
     * @return void
     * @throws Throwable
     */
    public function createTopic(): void
    {
        $response = $this->http->post($this->dsnd . '/topic/create', ['uri_query' => ['topic' => $this->topic]]);
        if ($response->getStatusCode() === 200) {
            App::info("Create topic $this->topic success!");
        }
        $response = $this->http->post($this->dsnd . '/channel/create', ['uri_query' => ['topic' => $this->topic, 'channel' => $this->channel]]);
        if ($response->getStatusCode() === 200) {
            App::info("Create topic $this->topic channel $this->channel success!");
        }
    }

    /**
     * @param string $message
     * @return array|null
     * @throws Throwable
     */
    public function publish(string $message): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::pub($this->topic, $message));
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
    public function publishMulti(array $bodies): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::mpub($this->topic, $bodies));
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
    public function publishDefer(string $message, int $deferTime): ?array
    {
        try {
            $connection = $this->pool->get();
            $connection->send(Writer::dpub($this->topic, $deferTime, $message));
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
    public function subscribe(array $config, Closure $callback): void
    {
        try {
            /** @var Consumer $connection */
            for ($i = 0; $i < $this->pool->getPoolConfig()->getMinActive(); $i++) {
                rgo(function () use ($config, $callback) {
                    loop:
                    $connection = $this->pool->get();
                    $connection->send(Writer::sub($this->topic, $this->channel));
                    $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
                    while (true) {
                        if (!$this->handleMessage($connection, $config, $callback)) {
                            break;
                        }
                    }
                    System::sleep($this->pool->getPoolConfig()->getMaxWait());
                    $this->pool->sub();
                    goto loop;
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