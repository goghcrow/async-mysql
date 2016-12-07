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

use KoolKode\Async\Socket\SocketStream;

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
    
    public function __construct(SocketStream $socket)
    {
        $this->socket = $socket;
        
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
    
    public function close()
    {
        $this->sequence = -1;
        $this->stream->close();
    }
    
    public function flush()
    {
        $this->sequence = -1;
    }

    public function handshake(string $username, string $password): \Generator
    {
        list ($auth, $authPlugin) = yield from $this->readAuthChallenge();
        
        $packet = $this->createAuthPacket($username, $password, $auth, $authPlugin);
        
        yield from $this->sendPacket($packet);
        
        $packet = yield from $this->readNextPacket();
        
        if (\ord($packet[0]) !== 0x00) {
            throw new \RuntimeException('Authentication failed');
        }
    }
    
    protected function readAuthChallenge(): \Generator
    {
        $packet = yield from $this->readNextPacket();
        $len = \strlen($packet);
        $offset = 0;
        
        if ($this->readInt8($packet, $offset) !== 0x0A) {
            throw new \RuntimeException(\sprintf('Unsupported protocol version: "0x%02X"', \ord($packet[0])));
        }
        
        $this->info['server'] = $this->readNullString($packet, $offset);
        $this->id = $this->readInt32($packet, $offset);
        
        $auth = $this->readFixedLengthString($packet, 8, $offset);
        $this->discardByte($packet, $offset, 0x00);
        
        $this->serverCaps = $this->readInt16($packet, $offset);
        
        if ($len > $offset) {
            $this->info['charset'] = $this->readInt8($packet, $offset);
            $this->statusFlags = $this->readInt16($packet, $offset);
            $this->serverCaps |= ($this->readInt16($packet, $offset) << 16);
            
            if ($this->serverCaps & self::CLIENT_PLUGIN_AUTH) {
                $len2 = $this->readInt8($packet, $offset);
            } else {
                $this->discardByte($packet, $offset, 0x00);
                $len2 = 0;
            }
            
            if ($this->serverCaps & self::CLIENT_SECURE_CONNECTION) {
                for ($i = 0; $i < 10; $i++) {
                    $this->discardByte($packet, $offset, 0x00);
                }
                
                $auth .= $this->readFixedLengthString($packet, \max(13, $len2 - 8), $offset);
                
                if ($this->serverCaps & self::CLIENT_PLUGIN_AUTH) {
                    $authPlugin = \trim($this->readNullString($packet, $offset));
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

    public function sendCommand(string $packet): \Generator
    {
        if ($this->sequence >= 0) {
            throw new \RuntimeException('Cannot send new command while processing another command');
        }
        
        return yield from $this->sendPacket($packet);
    }

    public function sendPacket(string $packet): \Generator
    {
        $packet = $this->encodeInt24(\strlen($packet)) . \chr(++$this->sequence % 256) . $packet;
        
        return yield $this->socket->write($packet);
    }

    public function readNextPacket(): \Generator
    {
        $header = yield $this->socket->readBuffer(4, true);
        $offset = 0;
        
        $len = $this->readInt24($header, $offset);
        $this->sequence = $this->readInt8($header, $offset);
        
        return $len ? yield $this->socket->readBuffer($len, true) : '';
    }

    public function discardByte(string $data, int & $offset, int $expected = null)
    {
        if ($expected !== null) {
            if (\ord($data[$offset]) !== $expected) {
                throw new \RuntimeException(\sprintf('Expected byte 0x%02X, received 0x%02X', $expected, \ord($data[$offset - 1])));
            }
        }
        
        $offset++;
    }

    public function readInt8(string $data, int & $offset): int
    {
        try {
            return \ord(\substr($data, $offset, 1));
        } finally {
            $offset += 1;
        }
    }

    public function readInt16(string $data, int & $offset): int
    {
        try {
            return \unpack('v', \substr($data, $offset, 2))[1];
        } finally {
            $offset += 2;
        }
    }

    public function readInt24(string $data, int & $offset): int
    {
        try {
            return \unpack('V', \substr($data, $offset, 3) . "\x00")[1];
        } finally {
            $offset += 3;
        }
    }

    public function readInt32(string $data, int & $offset): int
    {
        try {
            return \unpack('V', \substr($data, $offset, 4))[1];
        } finally {
            $offset += 4;
        }
    }

    public function readLengthEncodedInt(string $data, int & $offset)
    {
        $int = \ord(\substr($data, $offset, 1));
        $offset++;
        
        if ($int <= 0xFB) {
            return $int;
        }
        
        switch ($int) {
            case 0xFC:
                return $this->readInt16($data, $offset);
            case 0xFD:
                return $this->readInt24($data, $offset);
            case 0xFE:
                return $this->readInt64($data, $offset);
        }
        
        throw new \RuntimeException("$int is not in ranges [0x00, 0xFA] or [0xFC, 0xFE]");
    }

    public function readFixedLengthString(string $data, int $len, int & $offset): string
    {
        $str = \substr($data, $offset, $len);
        $offset += \strlen($str);
        
        return $str;
    }

    public function readNullString(string $data, int & $offset): string
    {
        $str = \substr($data, $offset, \strpos($data, "\0", $offset) - 1);
        $offset += \strlen($str) + 1;
        
        return $str;
    }

    public function readLengthEncodedString(string $data, int & $offset)
    {
        $len = $this->readLengthEncodedInt($data, $offset);
        
        if ($len < 1) {
            return '';
        }
        
        if ($len === 0xFB) {
            return NULL;
        }
        
        $str = \substr($data, $offset, $len);
        $offset += \strlen($str);
        
        return $str;
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
