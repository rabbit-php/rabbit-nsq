<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/3/7
 * Time: 13:07
 */

namespace rabbit\nsq;


use rabbit\core\Exception;

class ConnectionException extends Exception
{
    public function getName(): string
    {
        return 'socket connection error';
    }
}