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
use rabbit\contract\InitInterface;
use rabbit\core\BaseObject;
use rabbit\exception\InvalidArgumentException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\VarDumper;
use rabbit\httpclient\Client;
use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\ConnectionPool;
use rabbit\socket\SocketClient;

/**
 * Class NsqClient
 * @package rabbit\nsq
 */
class NsqClient extends BaseObject implements InitInterface
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
    /** @var Client */
    private $http;
    /** @var string */
    protected $dsnd;
    /** @var string */
    protected $topic;
    /** @var string */
    protected $channel;

    /**
     * NsqClient constructor.
     * @param ConnectionPool $pool
     */
    public function __construct(ConnectionPool $pool, string $dsnd)
    {
        $this->pool = $pool;
        $this->dsnd = $dsnd;
        $this->http = new Client([], 'saber', true);
    }

    /**
     * @return mixed|void
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
                            $this->pool->getPoolConfig()->setUri([$product['broadcast_address'] . ':' . $product['tcp_port']]);
                            return;
                        }
                    }
                    break;
                } else {
                    System::sleep($this->pool->getPoolConfig()->getMaxWaitTime());
                }
            } catch (\Throwable $exception) {
                if ($exception->getCode() === 404) {
                    $this->createTopic();
                }
            }
        }
    }

    /**
     * @return bool
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
     * @return NsqResult
     * @throws \Exception
     */
    public function publish(string $message): NsqResult
    {
        try {
            $connection = $this->pool->getConnection();
            $result = $connection->send(Writer::pub($this->topic, $message));
            return new NsqResult($connection, $result);
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param array $bodies
     * @return NsqResult
     * @throws \Exception
     */
    public function publishMulti(array $bodies): NsqResult
    {
        try {
            $connection = $this->pool->getConnection();
            $result = $connection->send(Writer::mpub($this->topic, $bodies));
            return new NsqResult($connection, $result);
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param string $message
     * @param int $deferTime
     * @return NsqResult
     * @throws \Exception
     */
    public function publishDefer(string $message, int $deferTime): NsqResult
    {
        try {
            $connection = $this->pool->getConnection();
            $result = $connection->send(Writer::dpub($this->topic, $deferTime, $message));
            return new NsqResult($connection, $result);
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param array $config
     * @param \Closure $callback
     * @throws \Exception
     */
    public function subscribe(array $config, \Closure $callback): void
    {
        try {
            /** @var Consumer $connection */
            for ($i = 0; $i < $this->pool->getPoolConfig()->getMinActive(); $i++) {
                rgo(function () use ($config, $callback) {
                    loop:
                    $connection = $this->pool->getConnection();
                    $connection->send(Writer::sub($this->topic, $this->channel));
                    $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
                    while (true) {
                        if (!$this->handleMessage($connection, $config, $callback)) {
                            break;
                        }
                    }
                    System::sleep($this->pool->getPoolConfig()->getMaxWaitTime());
                    $this->pool->setCurrentCount($this->pool->getCurrentCount() - 1);
                    goto loop;
                });
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