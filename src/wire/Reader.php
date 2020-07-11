<?php
declare(strict_types=1);

namespace Rabbit\Nsq\Wire;

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

    /**
     * Reader constructor.
     * @param float|null $timeout
     */
    public function __construct(float $timeout = null)
    {
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
    public function bindFrame(SocketClient $reader): self
    {
        $size = 0;
        $type = 0;
        try {
            $size = $this->readInt($reader, 4);
            $type = $this->readInt($reader, 4);
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
                        $frame['response'] = $this->readString($reader, $size - 4);
                        break;
                    case self::TYPE_ERROR:
                        $frame['error'] = $this->readString($reader, $size - 4);
                        break;
                    case self::TYPE_MESSAGE:
                        $frame['ts'] = $this->readLong($reader);
                        $frame['attempts'] = $this->readShort($reader);
                        $frame['id'] = $this->readString($reader, 16);
                        $frame['payload'] = $this->readString($reader, $size - 30);
                        break;
                    default:
                        throw new Exception($this->readString($reader, $size - 4));
                        break;
                }
            } catch (Exception $e) {
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
     * @param SocketClient $reader
     * @param int $size
     * @return string
     * @throws Exception
     */
    private function read(SocketClient $reader, int $size): string
    {
        $data = $reader->recv($size, $this->timeout ?? $reader->getPool()->getTimeout());
        if (empty($data)) {
            throw new Exception("recv empty data!");
        }
        return $data;
    }

    /**
     * @param SocketClient $connection
     * @return int
     */
    private function readShort(SocketClient $connection): int
    {
        list(, $res) = unpack('n', $connection->recv(2));
        return $res;
    }

    /**
     * @param SocketClient $connection
     * @return int
     */
    private function readInt(SocketClient $connection): int
    {
        list(, $res) = unpack('N', $connection->recv(4));
        if ((PHP_INT_SIZE !== 4)) {
            $res = sprintf("%u", $res);
        }
        return (int)$res;
    }

    /**
     * @param SocketClient $connection
     * @return string
     */
    private function readLong(SocketClient $connection): string
    {
        $hi = unpack('N', $connection->recv(4));
        $lo = unpack('N', $connection->recv(4));

        // workaround signed/unsigned braindamage in php
        $hi = sprintf("%u", $hi[1]);
        $lo = sprintf("%u", $lo[1]);

        return bcadd(bcmul($hi, "4294967296"), $lo);
    }

    /**
     * @param SocketClient $connection
     * @param int $size
     * @return string
     */
    private function readString(SocketClient $connection, int $size): string
    {
        $temp = unpack("c{$size}chars", $connection->recv($size));
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