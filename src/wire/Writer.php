<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/11/13
 * Time: 10:37
 */

namespace rabbit\nsq\wire;

use rabbit\nsq\utility\IntPacker;

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
     * @return string
     */
    public static function identify(): string
    {
        return self::command("IDENTIFY");
    }

    /**
     * @param $topic
     * @param $body
     * @return string
     */
    public static function pub(string $topic, string $body): string
    {
        $cmd = self::command("PUB", $topic);
        $size = IntPacker::uInt32(strlen($body), true);
        return $cmd . $size . $body;
    }

    /**
     * @param $topic
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
     * @param $topic
     * @param $deferTime
     * @param $body
     * @return string
     */
    public static function dpub(string $topic, int $deferTime, string $body): string
    {
        $cmd = self::command("DPUB", $topic, $deferTime);
        $size = IntPacker::uInt32(strlen($body), true);
        return $cmd . $size . $body;
    }

    /**
     * @param $topic
     * @param $channel
     * @return string
     */
    public static function sub($topic, $channel): string
    {
        return self::command("SUB", $topic, $channel);
    }

    /**
     * @param $count
     * @return string
     */
    public static function rdy(int $count): string
    {
        return self::command("RDY", $count);
    }

    /**
     * @param $id
     * @return string
     */
    public static function fin($id): string
    {
        return self::command("FIN", $id);
    }

    /**
     * @param $id
     * @param $timeout
     * @return string
     */
    public static function req($id, int $timeout): string
    {
        return self::command("REQ", $id, $timeout);
    }

    public static function touch($id): string
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
     * @param $secret
     * @return string
     */
    public static function auth($secret): string
    {
        $cmd = self::command("AUTH");
        $size = IntPacker::uInt32(strlen($secret), true);
        return $cmd . $size . $secret;
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
}