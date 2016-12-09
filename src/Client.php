<?php

/*
 * This file is part of KoolKode Async MySQL.
 *
 * (c) Martin Schröder <m.schroeder2007@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types = 1);

namespace KoolKode\Async\MySQL;

use KoolKode\Async\Awaitable;
use KoolKode\Async\Socket\SocketStream;
use KoolKode\Async\Util\Executor;

class Client
{
    const CLIENT_LONG_FLAG = 0x00000004;

    const CLIENT_CONNECT_WITH_DB = 0x00000008;

    const CLIENT_COMPRESS = 0x00000020;

    const CLIENT_PROTOCOL_41 = 0x00000200;

    const CLIENT_SSL = 0x00000800;

    const CLIENT_TRANSACTIONS = 0x00002000;

    const CLIENT_SECURE_CONNECTION = 0x00008000;

    const CLIENT_MULTI_STATEMENTS = 0x00010000;

    const CLIENT_MULTI_RESULTS = 0x00020000;

    const CLIENT_PS_MULTI_RESULTS = 0x00040000;

    const CLIENT_PLUGIN_AUTH = 0x00080000;

    const CLIENT_CONNECT_ATTRS = 0x00100000;

    const CLIENT_SESSION_TRACK = 0x00800000;

    const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    const CLIENT_DEPRECATE_EOF = 0x01000000;

    const MYSQL_TYPE_DECIMAL = 0x00;

    const MYSQL_TYPE_TINY = 0x01;

    const MYSQL_TYPE_SHORT = 0x02;

    const MYSQL_TYPE_LONG = 0x03;

    const MYSQL_TYPE_FLOAT = 0x04;

    const MYSQL_TYPE_DOUBLE = 0x05;

    const MYSQL_TYPE_NULL = 0x06;

    const MYSQL_TYPE_TIMESTAMP = 0x07;

    const MYSQL_TYPE_LONGLONG = 0x08;

    const MYSQL_TYPE_INT24 = 0x09;

    const MYSQL_TYPE_DATE = 0x0A;

    const MYSQL_TYPE_TIME = 0x0B;

    const MYSQL_TYPE_DATETIME = 0x0C;

    const MYSQL_TYPE_YEAR = 0x0D;

    const MYSQL_TYPE_NEWDATE = 0x0E;

    const MYSQL_TYPE_VARCHAR = 0x0F;

    const MYSQL_TYPE_BIT = 0x10;

    const MYSQL_TYPE_TIMESTAMP2 = 0x11;

    const MYSQL_TYPE_DATETIME2 = 0x12;

    const MYSQL_TYPE_TIME2 = 0x13;

    const MYSQL_TYPE_NEWDECIMAL = 0xF6;

    const MYSQL_TYPE_ENUM = 0xF7;

    const MYSQL_TYPE_SET = 0xF8;

    const MYSQL_TYPE_TINY_BLOB = 0xF9;

    const MYSQL_TYPE_MEDIUM_BLOB = 0xFA;

    const MYSQL_TYPE_LONG_BLOB = 0xFB;

    const MYSQL_TYPE_BLOB = 0xFC;

    const MYSQL_TYPE_VAR_STRING = 0xFD;

    const MYSQL_TYPE_STRING = 0xFE;

    const MYSQL_TYPE_GEOMETRY = 0xFF;

    protected $socket;

    /**
     * Connection ID.
     * 
     * @var int
     */
    protected $id;

    protected $sequence = -1;

    protected $info;

    protected $capabilities = 0;

    protected $clientCaps = 0;

    protected $serverCaps = 0;

    protected $statusFlags = 0;

    /**
     * Executor being used to queue commands and execute them one by one in the correct order.
     * 
     * @var Executor
     */
    protected $executor;

    public function __construct(SocketStream $socket)
    {
        $this->socket = $socket;
        $this->executor = new Executor();
        
        $this->clientCaps |= self::CLIENT_SESSION_TRACK;
        $this->clientCaps |= self::CLIENT_TRANSACTIONS;
        $this->clientCaps |= self::CLIENT_PROTOCOL_41;
        $this->clientCaps |= self::CLIENT_DEPRECATE_EOF;
        $this->clientCaps |= self::CLIENT_SECURE_CONNECTION;
        $this->clientCaps |= self::CLIENT_MULTI_RESULTS;
        $this->clientCaps |= self::CLIENT_MULTI_STATEMENTS;
        $this->clientCaps |= self::CLIENT_PLUGIN_AUTH;
        $this->clientCaps |= self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }

    public function shutdown(\Throwable $e = null): Awaitable
    {
        if ($e) {
            $this->executor->cancel($e);
            
            return $this->socket->close();
        }
        
        return $this->executor->execute(function () {
            return $this->socket->close();
        });
    }

    public function handshake(string $username, string $password): \Generator
    {
        try {
            list ($auth, $authPlugin) = yield from $this->readAuthChallenge();
            
            $packet = $this->createAuthPacket($username, $password, $auth, $authPlugin);
            
            yield from $this->sendPacket($packet);
            
            $packet = yield from $this->readRawPacket();
            
            if ($packet->readInt8() !== 0x00) {
                throw new \RuntimeException('Authentication failed');
            }
        } finally {
            $this->sequence = -1;
        }
    }

    protected function readAuthChallenge(): \Generator
    {
        $packet = yield from $this->readRawPacket();
        
        if (0x0A !== ($version = $packet->readInt8())) {
            throw new \RuntimeException(\sprintf('Unsupported protocol version: "0x%02X"', $version));
        }
        
        $this->info['server'] = $packet->readNullString();
        $this->id = $packet->readInt32();
        
        $auth = $packet->readFixedLengthString(8);
        $packet->discardByte(0x00);
        
        $this->serverCaps = $packet->readInt16();
        
        if (!$packet->isConsumed()) {
            $this->info['charset'] = $packet->readInt8();
            $this->statusFlags = $packet->readInt16();
            $this->serverCaps |= ($packet->readInt16() << 16);
            
            if ($this->serverCaps & self::CLIENT_PLUGIN_AUTH) {
                $len2 = $packet->readInt8();
            } else {
                $packet->discardByte(0x00);
                $len2 = 0;
            }
            
            if ($this->serverCaps & self::CLIENT_SECURE_CONNECTION) {
                for ($i = 0; $i < 10; $i++) {
                    $packet->discardByte(0x00);
                }
                
                $auth .= $packet->readFixedLengthString(\max(13, $len2 - 8));
                
                if ($this->serverCaps & self::CLIENT_PLUGIN_AUTH) {
                    $authPlugin = \trim($packet->readNullString());
                }
            }
        }
        
        return [
            $auth,
            $authPlugin ?? ''
        ];
    }

    protected function createAuthPacket(string $username, string $password, string $auth, string $authPlugin): string
    {
        $this->capabilities = $this->clientCaps & $this->serverCaps;
        
        // Charset 45 = utf8mb4_general_ci
        $packet = $this->encodeInt32($this->capabilities);
        $packet .= $this->encodeInt32(1 << 24 - 1);
        $packet .= $this->encodeInt8(45);
        $packet .= \str_repeat("\x00", 23);
        $packet .= $username . "\x00";
        
        if ($password === '') {
            $credentials = '';
        } else {
            $credentials = $this->secureAuth($password, $auth);
        }
        
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            $packet .= $this->encodeInt(\strlen($credentials)) . $credentials;
        } elseif ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
            $packet .= $this->encodeInt8(\strlen($credentials)) . $credentials;
        } else {
            $packet .= $credentials . "\x00";
        }
        
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            $packet .= ($authPlugin ?? '') . "\x00";
        }
        
        return $packet;
    }

    protected function secureAuth(string $password, string $scramble): string
    {
        $hash = \sha1($password, true);
        
        return $hash ^ \sha1(\substr($scramble, 0, 20) . \sha1($hash, true), true);
    }

    public function isEofDeprecated(): bool
    {
        return ($this->capabilities & self::CLIENT_DEPRECATE_EOF) !== 0;
    }

    public function readRawPacket(bool $object = true): \Generator
    {
        $header = yield $this->socket->readBuffer(4, true);
        
        $len = \unpack('V', \substr($header, 0, 3) . "\x00")[1];
        $this->sequence = \ord($header[3]);
        
        if ($object) {
            return new Packet($len ? yield $this->socket->readBuffer($len, true) : '');
        }
        
        return $len ? yield $this->socket->readBuffer($len, true) : '';
    }

    public function readPacket(int ...$expected): \Generator
    {
        $header = yield $this->socket->readBuffer(4, true);
        
        $len = \unpack('V', \substr($header, 0, 3) . "\x00")[1];
        $this->sequence = \ord($header[3]);
        
        $payload = $len ? yield $this->socket->readBuffer($len, true) : '';
        $packet = new Packet(\substr($payload, 1), \ord($payload[0]));
        
        if ($expected && !\in_array($packet->type, $expected, true)) {
            $expected = \implode(', ', \array_map(function (int $type) {
                return \sprintf('0x%02X', $type);
            }, $expected));
            
            throw new \RuntimeException(\sprintf('Received 0x%02X packet, expecting one of %s', $packet->type, $expected));
        }
        
        return $packet;
    }

    public function sendCommand(callable $callback): Awaitable
    {
        return $this->executor->execute(function () use ($callback) {
            try {
                $result = $callback($this);
                
                if ($result instanceof \Generator) {
                    $result = yield from $result;
                }
                
                return $result;
            } finally {
                $this->sequence = -1;
            }
        });
    }

    public function sendPacket(string $packet): \Generator
    {
        $packet = $this->encodeInt24(\strlen($packet)) . \chr(++$this->sequence % 256) . $packet;
        
        return yield $this->socket->write($packet);
    }

    public function encodeInt(int $val): string
    {
        if ($val < 0xFB) {
            return \chr($val);
        }
        
        if ($val < (1 << 16)) {
            return "\xFC" . $this->encodeInt16($val);
        }
        
        if ($val < (1 << 24)) {
            return "\xFD" . $this->encodeInt24($val);
        }
        
        if ($val < (1 << 62) * 4) {
            return "\xFE" . $this->encodeInt64($val);
        }
        
        throw new \RuntimEexception("Cannot encode integer bigger than 2^64 - 1 (current: $val)");
    }

    public function encodeInt8(int $val): string
    {
        return \chr($val);
    }

    public function encodeInt16(int $val): string
    {
        return \pack('v', $val);
    }

    public function encodeInt24(int $val): string
    {
        return \substr(\pack('V', $val), 0, 3);
    }

    public function encodeInt32(int $val): string
    {
        return \pack('V', $val);
    }
}
