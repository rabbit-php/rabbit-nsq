<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 10:55
 */

namespace rabbit\nsq\wire;

use rabbit\App;
use rabbit\nsq\message\Message;
use rabbit\nsq\utility\IntPacker;
use rabbit\socket\SocketClient;

/**
 * Class Reader
 * @package rabbit\nsq\wire
 */
class Reader
{
    const TYPE_RESPONSE = 0;
    const TYPE_ERROR = 1;
    const TYPE_MESSAGE = 2;
    const HEARTBEAT = "_heartbeat_";
    const OK = "OK";
    /** @var string */
    private $body;
    /** @var array */
    private $frame;

    /**
     * Reader constructor.
     * @param string $body
     */
    public function __construct(float $timeout = null)
    {
        $this->timeout = $timeout;
    }

    /**
     * @return Reader
     * @throws \Exception
     */
    public function bindFrame(SocketClient $reader): self
    {
        $size = 0;
        $type = 0;
        try {
            $size = $this->readInt($reader, 4);
            $type = $this->readInt($reader, 4);
        } catch (\Exception $e) {
            throw new \Exception("Error reading message frame [$size, $type] ({$e->getMessage()})");
        }
        $frame = [
            "size" => $size,
            "type" => $type,
        ];

        if ($size !== 0) {
            try {
                if (self::TYPE_RESPONSE == $type) {
                    $frame["response"] = $this->readString($reader, $size - 4);
                } elseif (self::TYPE_ERROR == $type) {
                    $frame["error"] = $this->readString($reader, $size - 4);
                }
            } catch (\Exception $e) {
                App::error($e->getMessage(), 'nsq');
            }
        }

        $this->frame = $frame;
        return $this;
    }
    // DecodeMessage deserializes data (as []byte) and creates a new Message
    // message format:
    //  [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
    //  |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
    //  |       8-byte         ||    ||                 16-byte                      || N-byte
    //  ------------------------------------------------------------------------------------------...
    //    nanosecond timestamp    ^^                   message ID                       message body
    //                         (uint16)
    //                          2-byte
    //                         attempts

    /**
     * @param $size
     * @return string
     */
    private function readInt(SocketClient $reader, int $size): int
    {
        list(, $res) = unpack('N', $this->read($reader, $size));
        if ((PHP_INT_SIZE !== 4)) {
            $res = sprintf("%u", $res);
        }
        return (int)$res;
    }

    /**
     * @param int $size
     * @return string
     */
    private function read(SocketClient $reader, int $size): string
    {
        return $reader->recv($size, $this->timeout ?? $reader->getPool()->getTimeout());
    }

    /**
     * @param $size
     * @return string
     */
    private function readString(SocketClient $reader, int $size): string
    {
        $bytes = unpack("c{$size}chars", $this->read($reader, $size));
        return implode(array_map("chr", $bytes));
    }

    public function getMessage(SocketClient $reader): ?Message
    {
        if (null !== $this->frame && self::TYPE_MESSAGE == $this->frame["type"]) {
            return (new Message())->setTimestamp($this->readInt64($reader, 8))
                ->setAttempts($this->readUInt16($reader, 2))
                ->setId($this->readString($reader, 16))
                ->setBody($this->readString($reader, $this->frame["size"] - 30))
                ->setDecoded();
        }
        return null;
    }

    /**
     * @param $size
     * @return int
     */
    private function readInt64(SocketClient $reader, int $size): int
    {
        return IntPacker::int64($this->read($reader, $size));
    }

    /**
     * @param $size
     * @return int
     */
    private function readUInt16(SocketClient $reader, int $size): int
    {
        return IntPacker::uInt16($this->read($reader, $size));
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