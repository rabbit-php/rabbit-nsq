<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 13:55
 */

namespace rabbit\nsq;


use rabbit\nsq\message\Message;
use rabbit\process\AbstractProcess;
use rabbit\process\Process;

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
     * @param Process $process
     */
    public function run(Process $process): void
    {
        foreach ($this->topics as $topicChannel => $config) {
            [$topic, $channel] = explode(':', $topicChannel);
            $handler = $config['handler'];
            unset($config['handler']);
            $this->nsq->subscribe($topic, $channel, $config, function (Message $msg) use ($handler) {
                call_user_func([$handler, 'handle'], $msg);
            });
        }
    }
}