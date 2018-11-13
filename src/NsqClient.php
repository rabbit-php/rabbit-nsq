<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 11:10
 */

namespace rabbit\nsq;

use rabbit\App;
use rabbit\nsq\message\Message;
use rabbit\nsq\pool\NsqPool;
use rabbit\nsq\wire\Reader;
use rabbit\nsq\wire\Writer;
use rabbit\pool\ConnectionInterface;

/**
 * Class NsqClient
 * @package rabbit\nsq
 */
class NsqClient
{
    /** @var NsqPool */
    private $pool;

    /**
     * NsqClient constructor.
     * @param NsqPool $pool
     */
    public function __construct(NsqPool $pool)
    {
        $this->pool = $pool;
    }

    /**
     * @param string $message
     * @throws \Exception
     */
    public function publish(string $topic, string $message): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::pub($topic, $message));
        } catch (Exception $e) {
            App::error("publish error=" . (string)$e, 'nsq');
        }
    }

    /**
     * @param mixed ...$bodies
     * @throws \Exception
     */
    public function publishMulti(string $topic, ...$bodies): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::mpub($topic, $bodies));
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, 'nsq');
        }
    }

    /**
     * @param string $message
     * @param int $deferTime
     * @throws \Exception
     */
    public function publishDefer(string $topic, string $message, int $deferTime): void
    {
        try {
            $connection = $this->pool->getConnection();
            $connection->send(Writer::dpub($topic, $deferTime, $message));
        } catch (\Exception $e) {
            App::error("publish error=" . (string)$e, 'nsq');
        }
    }

    public function subscribe(array $config, Closure $callback)
    {
        try {
            /** @var ConnectionInterface $connection */
            $connection = $this->pool->getConnection();
            go(function () use ($connection, $config, $callback) {
                while (true) {
                    $this->handleMessage($connection, $config, $callback);
                    \Co::sleep(.10);
                }
            });
            $connection->send(Writer::sub($config['topic'], $config['channel']))->write(Writer::rdy($config['rdy'] ?? 1));
        } catch (\Exception $e) {
            App::error("subscribe error=" . (string)$e, 'nsq');
        }
    }

    protected function handleMessage(ConnectionInterface $connection, array $config, Closure $callback)
    {
        $reader = new Reader($connection, $config['timeout'] ?? -1);
        if ($reader->isHeartbeat()) {
            $connection->send(Writer::nop());
        } elseif ($reader->isMessage()) {
            $msg = $reader->getMessage();
            try {
                call_user_func($callback, $msg);
            } catch (\Exception $e) {
                $this->logger->error("Will be requeued: ", $e->getMessage());
                $tunnel->write(Writer::touch($msg->getId()))
                    ->write(Writer::req(
                        $msg->getId(),
                        $tunnel->getConfig()->get("defaultRequeueDelay")["default"]
                    ));
            }
            $connection->send(Writer::fin($msg->getId()));
            $connection->send(Writer::rdy($config['rdy'] ?? 1));
        } elseif ($reader->isOk()) {
            App::info('Ignoring "OK" frame in SUB loop', 'nsq');
        } else {
            App::error("Error/unexpected frame received: =" . \get_object_vars($reader), 'nsq');
        }
    }
}