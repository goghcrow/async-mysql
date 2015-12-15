<?php

/*
 * This file is part of KoolKode Async MySQL.
 *
 * (c) Martin Schröder <m.schroeder2007@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace KoolKode\Async\MySQL;

use KoolKode\Async\Stream\DuplexStreamInterface;
use KoolKode\Async\Stream\SocketStream;

use function KoolKode\Async\readBuffer;

class Connection
{
    const DEFAULT_PORT = 3306;

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
    
    const MAX_PACKET_SIZE = 1 << 24 - 1;

    /**
     * Socket stream to the MySQL / MariaDB server.
     * 
     * @var DuplexStreamInterface
     */
    protected $stream;

    /**
     * Packet sequence number.
     * 
     * @var int
     */
    protected $sequence = -1;
    
    /**
     * Server-assigned connection ID.
     * 
     * @var int
     */
    protected $id = 0;
    
    /**
     * Connection charset.
     * 
     * @var int
     */
    protected $charset = 45; // utf8mb4_general_ci
    
    protected $capabilities = 0;
    
    protected $serverCapabilities = 0;
    
    protected $authPluginName = '';
    
    protected $authPluginData = '';
    
    protected $info = [];
    
    public function __construct(DuplexStreamInterface $stream)
    {
        $this->stream = $stream;
        
        $this->capabilities |= self::CLIENT_SESSION_TRACK;
        $this->capabilities |= self::CLIENT_TRANSACTIONS;
        $this->capabilities |= self::CLIENT_PROTOCOL_41;
        $this->capabilities |= self::CLIENT_SECURE_CONNECTION;
        $this->capabilities |= self::CLIENT_MULTI_RESULTS;
        $this->capabilities |= self::CLIENT_PS_MULTI_RESULTS;
        $this->capabilities |= self::CLIENT_MULTI_STATEMENTS;
        $this->capabilities |= self::CLIENT_PLUGIN_AUTH;
        $this->capabilities |= self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }
    
    public static function connect(string $host, string $username, string $password, int $port = self::DEFAULT_PORT): \Generator
    {
        $conn = new static(yield from SocketStream::connect($host, $port));
        
        yield from $conn->handleHandshake($username, $password);
        
        return $conn;
    }
    
    public function close(): \Generator
    {
        try {
            yield from $this->sendPacket($this->encodeInt8(0x01));
        } finally {
            $this->stream->close();
        }
    }
    
    protected function handleHandshake(string $username, string $password): \Generator
    {
        $packet = yield from $this->readNextPacket();
        $off = 0;
        
        if ($this->readInt8($packet, $off) !== 0x0A) {
            throw new \RuntimeException(sprintf('Unsupported protocol version: %02X', ord($packet)));
        }
        
        $this->info['server'] = $this->readNullString($packet, $off);
        $this->id = $this->readInt32($packet, $off);
        
        $this->authPluginData = substr($packet, $off, 8);
        $off += 8;
        
        $off++;
        
        $this->serverCapabilities = $this->readInt16($packet, $off);
        
        if (strlen($packet) > $off) {
            $this->info['charset'] = $this->readInt8($packet, $off);
            $this->info['statusFlags'] = $this->readInt16($packet, $off);
            
            $this->serverCapabilities += ($this->readInt16($packet, $off) << 16);
            
            $alen = ($this->serverCapabilities & self::CLIENT_PLUGIN_AUTH) ? $this->readInt8($packet, $off) : 0;
            
            if ($this->serverCapabilities & self::CLIENT_SECURE_CONNECTION) {
                $off += 10;
                
                $this->authPluginData .= $this->readFixedLengthString($packet, max(13, $alen - 8), $off);
                
                if ($this->serverCapabilities & self::CLIENT_PLUGIN_AUTH) {
                    $this->authPluginName = trim($this->readNullString($packet, $off));
                }
            }
        }
        
        // Prepare and send handshake response packet:
        $this->capabilities &= $this->serverCapabilities;
        
        $packet = $this->encodeInt32($this->capabilities);
        $packet .= $this->encodeInt32(self::MAX_PACKET_SIZE);
        $packet .= $this->encodeInt8($this->charset);
        $packet .= str_repeat("\0", 23);
        
        $packet .= $username . "\0";
        
        if ($password === '') {
            $auth = '';
        } elseif ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            switch ($this->authPluginName) {
                case 'mysql_native_password':
                    $auth = $this->secureAuth($password, $this->authPluginData);
                    break;
                default:
                    throw new \RuntimeException(sprintf('Unsupported auth scheme: "%s"', $this->authPluginName));
            }
        } else {
            $auth = $this->secureAuth($password, $this->authPluginData);
        }
        
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            $packet .= $this->encodeInt(strlen($auth)) . $auth;
        } elseif ($this->capabilities & self::CLIENT_SECURE_CONNECTION) {
            $packet .= $this->encodeInt8(strlen($auth)) . $auth;
        } else {
            $packet .= $auth . "\0";
        }
        
        if ($this->capabilities & self::CLIENT_PLUGIN_AUTH) {
            $packet .= $this->authPluginName . "\0";
        }
        
        yield from $this->sendPacket($packet);
        
        $packet = yield from $this->readNextPacket();
        
        if (ord($packet) !== 0) {
            var_dump($packet);
        }
    }
    
    protected function readNextPacket(): \Generator
    {
        $header = yield readBuffer($this->stream, 4);
        $off = 0;
        
        $len = $this->readInt24($header, $off);
        $this->sequence = $this->readInt8($header, $off);
        
        if ($len > 0) {
            $payload = yield readBuffer($this->stream, $len);
        } else {
            $payload = '';
        }
        
        switch (ord($payload)) {
            case 0xFF:
                $off = 1;
                $code = $this->readInt16($payload, $off);
                
                if ($this->capabilities & self::CLIENT_PROTOCOL_41) {
                    // Marker...
                    $this->readFixedLengthString($payload, 1, $off);
                    
                    $sqlState = $this->readFixedLengthString($payload, 5, $off);
                } else {
                    $sqlState = '';
                }
                
                $error = substr($payload, $off);
                
                if ($sqlState !== '') {
                    $error = sprintf('SQLSTATE[%s]: %s', $sqlState, $error);
                }
                
                $this->sequence = -1;
                
                throw new ConnectionException($error, $code, $sqlState);
        }
        
        return $payload;   
    }
    
    protected function sendPacket(string $packet): \Generator
    {
        $packet = $this->encodeInt24(strlen($packet)) . chr(++$this->sequence) . $packet;
        
        return yield from $this->stream->write($packet);
    }
    
    protected function secureAuth(string $password, string $scramble): string
    {
        $hash = sha1($password, true);
        
        return $hash ^ sha1(substr($scramble, 0, 20) . sha1($hash, true), true);
    }
    
    protected function readInt8(string $data, int & $off): int
    {
        try {
            return ord(substr($data, $off));
        } finally {
            $off += 1;
        }
    }
    
    protected function readInt16(string $data, int & $off): int
    {
        try {
            return unpack('v', substr($data, $off))[1];
        } finally {
            $off += 2;
        }
    }
    
    protected function readInt24(string $data, int & $off): int
    {
        try {
            return unpack('V', substr($data, $off, 3) . "\x00")[1];
        } finally {
            $off += 3;
        }
    }
    
    protected function readInt32(string $data, int & $off): int
    {
        try {
            return unpack('V', substr($data, $off))[1];
        } finally {
            $off += 4;
        }
    }
    
    protected function readFixedLengthString(string $data, int $len, int & $off): string
    {
        $str = substr($data, $off, $len);
        $off += strlen($str);
        
        return $str;
    }
    
    protected function readNullString(string $data, int & $off): string
    {
        $str = substr($data, $off, strpos($data, "\0", $off) - 1);
        $off += strlen($str) + 1;
        
        return $str;
    }
    
    protected function encodeInt(int $val): string
    {
        if ($val < 0xFB) {
            return chr($val);
        }
        
        if ($val < (1 << 16)) {
            return "\xFC" . $this->encodeInt16($val);
        }
        
        if ($val < (1 << 24)) {
            return "\xFD" . $this->encodeInt24($val);
        }
        
        if ($val < (1 << 62) * 4) {
            return "\xFE" . self::encode_int64($val);
        }
        
        throw new \OutOfRangeException("encodeInt doesn't allow integers bigger than 2^64 - 1 (current: $val)");
    }
    
    protected function encodeInt8(int $val): string
    {
        return chr($val);
    }
    
    protected function encodeInt16(int $val): string
    {
        return pack('v', $val);
    }

    protected function encodeInt24(int $val): string
    {
        return substr(pack('V', $val), 0, 3);
    }

    protected function encodeInt32(int $val): string
    {
        return pack('V', $val);
    }

    protected function encodeInt64(int $val): string
    {
        return pack('VV', $val & 0XFFFFFFFF, $val >> 32);
    }
}
