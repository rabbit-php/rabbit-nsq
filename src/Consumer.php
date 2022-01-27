<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Closure;
use Rabbit\Base\App;
use Rabbit\Base\Core\LoopControl;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\Socket\SocketClient;
use Throwable;

class Consumer extends AbstractNsq
{
    protected int $rdy = 1;
    protected float $timeout = 5;

    /**
     * @param array $config
     * @param Closure $callback
     * @throws Throwable
     */
    public function subscribe(string $topic, string $channel, array $config, Closure $callback): ?LoopControl
    {
        try {
            $this->makeTopic($topic, $channel);
            return loop(function () use ($topic, $channel, $config, $callback, &$loop): void {
                /** @var ConsumerClient $connection */
                $connection = $this->pool->get();
                $this->pool->sub();
                $connection->send(Writer::sub($topic, $channel));
                $connection->send(Writer::rdy(ArrayHelper::getValue($config, 'rdy', $this->rdy)));
                while ($loop) {
                    if (!$this->handleMessage($connection, $config, $callback)) {
                        break;
                    }
                }
            }, $this->sleep);
        } catch (Throwable $e) {
            App::error("subscribe error=" . (string)$e, $this->module);
        }
    }

    /**
     * @param SocketClient $connection
     * @param array $config
     * @param Closure $callback
     * @return bool
     * @throws Throwable
     */
    private function handleMessage(SocketClient $connection, array $config, Closure $callback): bool
    {
        try {
            $reader = (new Reader($connection, -1))->bindFrame();
        } catch (Throwable $throwable) {
            return false;
        }

        if ($reader->isHeartbeat()) {
            $connection->send(Writer::nop());
        } elseif ($reader->isMessage()) {
            $msg = $reader->getFrame();
            rgo(function () use ($connection, $config, $callback, &$msg): void {
                try {
                    call_user_func($callback, $msg);
                } catch (Throwable $e) {
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
            App::debug('Ignoring "OK" frame in SUB loop', $this->module);
        } else {
            App::error("Error/unexpected frame received: =" . VarDumper::getDumper()->dumpAsString($reader->getFrame()), $this->module);
        }
        return true;
    }
}
