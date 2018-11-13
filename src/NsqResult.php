<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 9:30
 */

namespace rabbit\nsq;


use rabbit\nsq\message\Message;
use rabbit\pool\AbstractResult;

/**
 * Class NsqResult
 * @package rabbit\nsq
 */
class NsqResult extends AbstractResult
{
    /**
     * @param mixed ...$params
     * @return mixed|void
     */
    public function getResult(...$params)
    {
        $timeout = array_shift($params);
        $result = $this->parser->decode($this->recv(true, $timeout));
        return Message::fromFrame($result);
    }
}