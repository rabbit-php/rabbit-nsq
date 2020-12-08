<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\HttpClient\Client;
use Rabbit\Pool\ConnectionPool;
use Throwable;

abstract class AbstractNsq extends BaseObject
{
    protected ConnectionPool $pool;
    protected string $module = 'nsq';
    protected string $dsnd;
    protected static array $created = [];
    protected float $sleep = 0.5;

    public function __construct(string $dsnd, ConnectionPool $pool)
    {
        $this->pool = $pool;
        $this->dsnd = $dsnd;
    }
    /**
     * @throws Throwable
     */
    public function makeTopic(string $topic, string $channel = null): void
    {
        if (in_array("$topic:" . ($channel ?? 'null'), self::$created)) {
            return;
        }
        while (true) {
            $client = new Client();
            try {
                $response = $client->get($this->pool->getConnectionAddress() . '/lookup', ['uri_query' => ['topic' => $topic]]);
                if ($response->getStatusCode() === 200) {
                    $data = $response->jsonArray();
                    if ($channel !== null) {
                        if (count($data['channels']) === 0) {
                            $this->createTopic($topic, $channel, $client);
                            usleep((int)($this->sleep * 1000000));
                            continue;
                        }
                        foreach ($data['channels'] as $chl) {
                            if ($chl === $channel && $data['producers']) {
                                $producer = reset($data['producers']);
                                $this->pool->getPoolConfig()->setUri($producer['broadcast_address'] . ':' . $producer['tcp_port']);
                                self::$created[] = "$topic:$chl";
                                return;
                            }
                        }
                    } else {
                        $producer = reset($data['producers']);
                        $this->pool->getPoolConfig()->setUri($producer['broadcast_address'] . ':' . $producer['tcp_port']);
                        self::$created[] = "$topic:null";
                        return;
                    }

                    break;
                } else {
                    usleep((int)($this->sleep * 1000000));
                }
            } catch (Throwable $exception) {
                if ($exception->getCode() === 404) {
                    $this->createTopic($topic, $channel, $client);
                }
            }
        }
    }

    /**
     * @return void
     * @throws Throwable
     */
    public function createTopic(string $topic, ?string $channel = null, Client $client = null): void
    {
        $client = $client ?? new Client();
        $response = $client->post($this->dsnd . '/topic/create', ['uri_query' => ['topic' => $topic]]);
        if ($response->getStatusCode() === 200) {
            App::info("Create topic $topic success!");
        }
        if ($channel !== null) {
            $response = $client->post($this->dsnd . '/channel/create', ['uri_query' => ['topic' => $topic, 'channel' => $channel]]);
            if ($response->getStatusCode() === 200) {
                App::info("Create topic $topic channel $channel success!");
            }
        }
        self::$created[] = "$topic:" . ($channel ?? 'null');
    }
}
