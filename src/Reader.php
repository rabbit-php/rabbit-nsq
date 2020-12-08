<?php

declare(strict_types=1);

namespace Rabbit\Nsq;

use Exception;
use Rabbit\Base\App;
use Rabbit\Socket\SocketClient;
use Throwable;

/**
 * Class Reader
 * @package Rabbit\Nsq\Wire
 */
class Reader
{
    const TYPE_RESPONSE = 0;
    const TYPE_ERROR = 1;
    const TYPE_MESSAGE = 2;
    const HEARTBEAT = "_heartbeat_";
    const OK = "OK";

    /** @var array */
    private array $frame = [];
    private ?float $timeout;
    private SocketClient $reader;

    /**
     * Reader constructor.
     * @param float|null $timeout
     */
    public function __construct(SocketClient $reader, float $timeout = null)
    {
        $this->reader = $reader;
        $this->timeout = $timeout;
    }

    /**
     * @return array
     */
    public function getFrame(): array
    {
        return $this->frame;
    }

    /**
     * @param SocketClient $reader
     * @return $this
     * @throws Throwable
     */
    public function bindFrame(): self
    {
        $size = 0;
        $type = 0;
        try {
            $size = $this->readInt(4);
            $type = $this->readInt(4);
        } catch (Exception $e) {
            throw new Exception("Error reading message frame [$size, $type] ({$e->getMessage()})");
        }
        $frame = [
            "size" => $size,
            "type" => $type,
        ];

        if ($size !== 0) {
            try {
                switch ($type) {
                    case self::TYPE_RESPONSE:
                        $frame['response'] = $this->readString($size - 4);
                        break;
                    case self::TYPE_ERROR:
                        $frame['error'] = $this->readString($size - 4);
                        break;
                    case self::TYPE_MESSAGE:
                        $frame['ts'] = $this->readLong();
                        $frame['attempts'] = $this->readShort();
                        $frame['id'] = $this->readString(16);
                        $frame['payload'] = $this->readString($size - 30);
                        break;
                    default:
                        throw new Exception($this->readString($size - 4));
                        break;
                }
            } catch (Exception $e) {
                App::error($e->getMessage(), 'nsq');
            }
        }

        $this->frame = $frame;
        return $this;
    }

    private function readShort(): int
    {
        list(, $res) = unpack('n', $this->reader->recv(2, $this->timeout ?? $this->reader->getPool()->getTimeout()));
        return $res;
    }

    private function readInt(): int
    {
        list(, $res) = unpack('N', $this->reader->recv(4, $this->timeout ?? $this->reader->getPool()->getTimeout()));
        if ((PHP_INT_SIZE !== 4)) {
            $res = sprintf("%u", $res);
        }
        return (int)$res;
    }

    private function readLong(): string
    {
        $timeout = $this->timeout ?? $this->reader->getPool()->getTimeout();
        $hi = unpack('N', $this->reader->recv(4, $timeout));
        $lo = unpack('N', $this->reader->recv(4, $timeout));

        // workaround signed/unsigned braindamage in php
        $hi = sprintf("%u", $hi[1]);
        $lo = sprintf("%u", $lo[1]);

        return bcadd(bcmul($hi, "4294967296"), $lo);
    }

    private function readString(int $size): string
    {
        $temp = unpack("c{$size}chars", $this->reader->recv($size, $this->timeout ?? $this->reader->getPool()->getTimeout()));
        $out = "";
        foreach ($temp as $v) {
            if ($v > 0) {
                $out .= chr($v);
            }
        }
        return $out;
    }

    /**
     * @return bool
     */
    public function isMessage(): bool
    {
        return self::TYPE_MESSAGE == $this->frame["type"];
    }

    /**
     * @return bool
     */
    public function isHeartbeat(): bool
    {
        return $this->isResponse(self::HEARTBEAT);
    }

    /**
     * @param string|null $response
     * @return bool
     */
    public function isResponse(string $response = null): bool
    {
        return isset($this->frame["response"])
            && self::TYPE_RESPONSE == $this->frame["type"]
            && (null === $response || $response === $this->frame["response"]);
    }

    /**
     * @return bool
     */
    public function isOk(): bool
    {
        return $this->isResponse(self::OK);
    }
}
