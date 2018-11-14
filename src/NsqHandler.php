<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 14:13
 */

namespace rabbit\nsq;

use rabbit\nsq\message\Message;

/**
 * Interface NsqHandler
 * @package rabbit\nsq
 */
interface NsqHandler
{
    /**
     * @param string $msg
     */
    public function handle(Message $msg): void;
}