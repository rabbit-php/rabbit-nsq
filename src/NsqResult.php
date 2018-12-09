<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/15
 * Time: 11:12
 */

namespace rabbit\nsq;


use rabbit\nsq\wire\Reader;
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
        $reader = (new Reader($timeout))->bindFrame($this->connection);
        $this->connection->release();
        return $reader;
    }
}