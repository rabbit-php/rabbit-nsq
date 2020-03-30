<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/14
 * Time: 3:09
 */

namespace rabbit\nsq;


use rabbit\nsq\wire\Writer;
use rabbit\socket\SocketClient;

/**
 * Class Tcp
 * @package rabbit\nsq
 */
class Consumer extends SocketClient
{
    /**
     * Consumer constructor.
     * @param string $poolKey
     */
    public function __construct(string $poolKey)
    {
        parent::__construct($poolKey);
        $this->send(Writer::MAGIC_V2);
    }
}