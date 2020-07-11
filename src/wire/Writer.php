<?php
declare(strict_types=1);

namespace Rabbit\Nsq\Wire;

use Rabbit\Base\Helper\JsonHelper;
use Rabbit\Nsq\IntPacker;

/**
 * Class Writer
 * @package rabbit\nsq\wire
 */
class Writer
{
    const MAGIC_V2 = "  V2";

    /**
     * @return string
     */
    public static function magic(): string
    {
        return self::MAGIC_V2;
    }

    /**
     * @param array $json
     * @return string
     */
    public static function identify(array $json): string
    {
        $json = JsonHelper::encode($json);
        $cmd = self::command("IDENTIFY");
        $size = IntPacker::uInt32(strlen($json), true);
        return $cmd . $size . $json;
    }

    /**
     * @param $action
     * @param mixed ...$params
     * @return string
     */
    private static function command($action, ...$params): string
    {
        return sprintf("%s %s%s", $action, implode(' ', $params), "\n");
    }

    /**
     * @param string $topic
     * @param string $body
     * @return string
     */
    public static function pub(string $topic, string $body): string
    {
        $cmd = self::command("PUB", $topic);
        $size = IntPacker::uInt32(strlen($body), true);
        return $cmd . $size . $body;
    }

    /**
     * @param string $topic
     * @param array $bodies
     * @return string
     */
    public static function mpub(string $topic, array $bodies): string
    {
        $cmd = self::command("MPUB", $topic);
        $num = IntPacker::uInt32(count($bodies), true);
        $mb = implode(array_map(function ($body) {
            return IntPacker::uint32(strlen($body), true) . $body;
        }, $bodies));
        $size = IntPacker::uInt32(strlen($num . $mb), true);
        return $cmd . $size . $num . $mb;
    }

    /**
     * @param string $topic
     * @param int $deferTime
     * @param string $body
     * @return string
     */
    public static function dpub(string $topic, int $deferTime, string $body): string
    {
        $cmd = self::command("DPUB", $topic, $deferTime);
        $size = IntPacker::uInt32(strlen($body), true);
        return $cmd . $size . $body;
    }

    /**
     * @param string $topic
     * @param string $channel
     * @return string
     */
    public static function sub(string $topic, string $channel): string
    {
        return self::command("SUB", $topic, $channel);
    }

    /**
     * @param int $count
     * @return string
     */
    public static function rdy(int $count): string
    {
        return self::command("RDY", $count);
    }

    /**
     * @param string $id
     * @return string
     */
    public static function fin(string $id): string
    {
        return self::command("FIN", $id);
    }

    /**
     * @param string $id
     * @param int $timeout
     * @return string
     */
    public static function req(string $id, int $timeout): string
    {
        return self::command("REQ", $id, $timeout);
    }

    public static function touch(string $id): string
    {
        return self::command("TOUCH", $id);
    }

    /**
     * @return string
     */
    public static function cls(): string
    {
        return self::command("CLS");
    }

    /**
     * @return string
     */
    public static function nop(): string
    {
        return self::command("NOP");
    }

    /**
     * @param array $json
     * @return string
     */
    public static function auth(array $json): string
    {
        $json = JsonHelper::encode($json);
        $cmd = self::command("AUTH");
        $size = IntPacker::uInt32(strlen($json), true);
        return $cmd . $size . $json;
    }
}