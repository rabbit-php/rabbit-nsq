<?php

namespace rabbit\nsq\message;
/**
 * Class Message
 * @package rabbit\message
 */
class Message
{
    /** @var bool */
    private $decoded = false;
    /** @var string */
    private $id;
    /** @var string */
    private $body;
    /** @var float */
    private $timestamp;
    /** @var int */
    private $attempts;
    /** @var string */
    private $nsqdAddr;
    /** @var string */
    private $delegate;

    /**
     * Message constructor.
     */
    public function __construct()
    {
        $this->timestamp = microtime(true);
    }

    /**
     * @return bool
     */
    public function isDecoded(): bool
    {
        return $this->decoded;
    }

    /**
     * @return $this
     */
    public function setDecoded(): self
    {
        $this->decoded = true;
        return $this;
    }

    /**
     * @return string
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * @param string $id
     * @return Message
     */
    public function setId(string $id): self
    {
        $this->id = $id;
        return $this;
    }

    /**
     * @return string
     */
    public function getBody(): string
    {
        return $this->body;
    }

    /**
     * @param string $body
     * @return Message
     */
    public function setBody(string $body): self
    {
        $this->body = $body;
        return $this;
    }

    /**
     * @return float
     */
    public function getTimestamp(): float
    {
        return $this->timestamp;
    }

    /**
     * @param float|null $timestamp
     * @return Message
     */
    public function setTimestamp(float $timestamp = null): self
    {
        if (null === $timestamp) {
            $this->timestamp = floor(microtime(true) * 1000);
        }
        $this->timestamp = floor($timestamp * 1000);
        return $this;
    }

    /**
     * @return int
     */
    public function getAttempts(): int
    {
        return $this->attempts;
    }

    /**
     * @param int $attempts
     * @return $this
     */
    public function setAttempts(int $attempts)
    {
        $this->attempts = $attempts;
        return $this;
    }

    /**
     * @return string
     */
    public function getNsqdAddr(): string
    {
        return $this->nsqdAddr;
    }

    /**
     * @param string $nsqdAddr
     * @return Message
     */
    public function setNsqdAddr(string $nsqdAddr): self
    {
        $this->nsqdAddr = $nsqdAddr;
        return $this;
    }

    /**
     * @return string
     */
    public function getDelegate():string
    {
        return $this->delegate;
    }

    /**
     * @param string $delegate
     * @return Message
     */
    public function setDelegate(string $delegate): self
    {
        $this->delegate = $delegate;
        return $this;
    }
}