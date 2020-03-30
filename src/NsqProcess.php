<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 13:55
 */

namespace rabbit\nsq;


use Co\System;
use rabbit\httpclient\Client;
use rabbit\nsq\message\Message;
use rabbit\process\AbstractProcess;
use rabbit\process\Process;
use Swlib\Saber\Response;

/**
 * Class NsqlProcess
 * @package rabbit\nsq
 */
class NsqProcess extends AbstractProcess
{
    /** @var NsqClient */
    private $nsq;

    /**
     * @var array
     */
    private $topics = [];
    /**
     * @var Client
     */
    private $httpClient;

    /**
     * @param Process $process
     */
    public function run(Process $process): void
    {
        foreach ($this->topics as $topicChannel => $config) {
            rgo(function () use ($topicChannel, $config) {
                [$topic, $channel] = explode(':', $topicChannel);
                $handler = $config['handler'];
                unset($config['handler']);
                $config['pool'] = clone $config['pool'];
                $config['pool']->getPoolConfig()->setName('nsq:' . $topicChannel);
                if (empty($config['pool']->getPoolConfig()->getUri())) {
                    $getTopic = true;
                    while ($getTopic) {
                        /** @var Response $response */
                        $response = $this->httpClient->get(getDI('nsq.uri') . '/lookup', ['uri_query' => ['topic' => $topic]]);
                        if ($response->success) {
                            $data = $response->getParsedJsonArray();
                            foreach ($data['channels'] as $chl) {
                                if ($chl === $channel && $data['producers']) {
                                    $product = current($data['producers']);
                                    $uri = explode(':', $product['remote_address'])[0];
                                    $port = $product['tcp_port'];
                                    $config['pool']->getPoolConfig()->setUri([$uri . ':' . $port]);
                                    $getTopic = false;
                                }
                            }
                        }
                        System::sleep($config['pool']->getPoolConfig()->getMaxWaitTime());
                    }
                }
                $this->nsq->subscribe($topic, $channel, $config, function (array $msg) use ($handler) {
                    getDI($handler)->handle($msg);
                });
            });
        }
    }
}