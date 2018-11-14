<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 10:55
 */

namespace rabbit\nsq\wire;

use rabbit\nsq\message\Message;
use rabbit\nsq\utility\IntPacker;

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
    public function __construct(string $body)
    {
        $this->body = $body;
    }

    /**
     * @return Reader
     * @throws \Exception
     */
    public function bindFrame(): self
    {
        $size = 0;
        $type = 0;
        try {
            $size = $this->readInt(4);
            $type = $this->readInt(4);
        } catch (\Exception $e) {
            throw new \Exception("Error reading message frame [$size, $type] ({$e->getMessage()})");
        }
        $frame = [
            "size" => $size,
            "type" => $type,
        ];

        try {
            if (self::TYPE_RESPONSE == $type) {
                $frame["response"] = $this->readString($size - 4);
            } elseif (self::TYPE_ERROR == $type) {
                $frame["error"] = $this->readString($size - 4);
            }
        } catch (\Exception $e) {
            throw new \Exception("Error reading frame details [$size, $type] ({$e->getMessage()})");
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
    public function getMessage(): ?Message
    {
        if (null !== $this->frame && self::TYPE_MESSAGE == $this->frame["type"]) {
            return (new Message())->setTimestamp($this->readInt64(8))
                ->setAttempts($this->readUInt16(2))
                ->setId($this->readString(16))
                ->setBody($this->readString($this->frame["size"] - 30))
                ->setDecoded();
        }
        return null;
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
     * @return bool
     */
    public function isOk(): bool
    {
        return $this->isResponse(self::OK);
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
     * @param $size
     * @return string
     */
    private function readInt(int $size): int
    {
        list(, $res) = unpack('N', $this->read($size));
        if ((PHP_INT_SIZE !== 4)) {
            $res = sprintf("%u", $res);
        }
        return (int)$res;
    }

    /**
     * @param $size
     * @return int
     */
    private function readInt64(int $size): int
    {
        return IntPacker::int64($this->read($size));
    }

    /**
     * @param $size
     * @return int
     */
    private function readUInt16(int $size): int
    {
        return IntPacker::uInt16($this->read($size));
    }

    /**
     * @param $size
     * @return string
     */
    private function readString(int $size): string
    {
        $bytes = unpack("c{$size}chars", $this->read($size));
        return implode(array_map("chr", $bytes));
    }

    /**
     * @param int $size
     * @return string
     */
    private function read(int $size): string
    {
        $sub = substr($this->body, 0, $size);
        $this->body = substr($this->body, $size);
        return $sub;
    }
}