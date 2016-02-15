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
use KoolKode\Async\Stream\Stream;

class Client
{
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
    
    const MYSQL_TYPE_DATE = 0x0a;
    
    const MYSQL_TYPE_TIME = 0x0b;
    
    const MYSQL_TYPE_DATETIME = 0x0c;
    
    const MYSQL_TYPE_YEAR = 0x0d;
    
    const MYSQL_TYPE_NEWDATE = 0x0e;
    
    const MYSQL_TYPE_VARCHAR = 0x0f;
    
    const MYSQL_TYPE_BIT = 0x10;
    
    const MYSQL_TYPE_TIMESTAMP2 = 0x11;
    
    const MYSQL_TYPE_DATETIME2 = 0x12;
    
    const MYSQL_TYPE_TIME2 = 0x13;
    
    const MYSQL_TYPE_NEWDECIMAL = 0xf6;
    
    const MYSQL_TYPE_ENUM = 0xf7;
    
    const MYSQL_TYPE_SET = 0xf8;
    
    const MYSQL_TYPE_TINY_BLOB = 0xf9;
    
    const MYSQL_TYPE_MEDIUM_BLOB = 0xfa;
    
    const MYSQL_TYPE_LONG_BLOB = 0xfb;
    
    const MYSQL_TYPE_BLOB = 0xfc;
    
    const MYSQL_TYPE_VAR_STRING = 0xfd;
    
    const MYSQL_TYPE_STRING = 0xfe;
    
    const MYSQL_TYPE_GEOMETRY = 0xff;
    
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
    
    const SERVER_STATUS_IN_TRANS = 0x0001;
    
    const SERVER_STATUS_AUTOCOMMIT = 0x0002;
    
    const SERVER_MORE_RESULTS_EXISTS = 0x0008;
    
    const SERVER_STATUS_NO_GOOD_INDEX_USED = 0x0010;
    
    const SERVER_STATUS_NO_INDEX_USED = 0x0020;
    
    const SERVER_STATUS_CURSOR_EXISTS = 0x0040;
    
    const SERVER_STATUS_LAST_ROW_SENT = 0x0080;
    
    const SERVER_STATUS_DB_DROPPED = 0x0100;
    
    const SERVER_STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
    
    const SERVER_STATUS_METADATA_CHANGED = 0x0400;
    
    const SERVER_QUERY_WAS_SLOW = 0x0800;
    
    const SERVER_PS_OUT_PARAMS = 0x1000;
    
    const SERVER_STATUS_IN_TRANS_READONLY = 0x2000;
    
    const SERVER_SESSION_STATE_CHANGED = 0x4000;
    
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
    
    protected $connected = false;
    
    protected $lastInsertId;
    
    protected $info = [];
    
    public function __construct(DuplexStreamInterface $stream)
    {
        $this->stream = $stream;
        
        $this->capabilities |= self::CLIENT_SESSION_TRACK;
        $this->capabilities |= self::CLIENT_TRANSACTIONS;
        $this->capabilities |= self::CLIENT_PROTOCOL_41;
        $this->capabilities |= self::CLIENT_DEPRECATE_EOF;
        $this->capabilities |= self::CLIENT_SECURE_CONNECTION;
        $this->capabilities |= self::CLIENT_MULTI_RESULTS;
        $this->capabilities |= self::CLIENT_PS_MULTI_RESULTS;
        $this->capabilities |= self::CLIENT_MULTI_STATEMENTS;
        $this->capabilities |= self::CLIENT_PLUGIN_AUTH;
        $this->capabilities |= self::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
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
    
    public function getLastInsertId()
    {
        return $this->lastInsertId;
    }
    
    public function hasCapabilty(int $caps): bool
    {
        return ($this->capabilities & $caps) === $caps;
    }
    
    public function handleHandshake(string $username, string $password): \Generator
    {
        $this->connected = true;
        
        try {
            $packet = yield from $this->readNextPacket();
            $off = 0;
    
            if ($this->readInt8($packet, $off) !== 0x0A) {
                throw new ProtocolError(sprintf('Unsupported protocol version: %02X', ord($packet)));
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
                        throw new ProtocolError(sprintf('Unsupported auth scheme: "%s"', $this->authPluginName));
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
                throw new ProtocolError(sprintf('Handshake failed, response packet is %02X', ord($packet)));
            }
        } catch (\Throwable $e) {
            $this->connected = false;
            
            throw $e;
        } finally {
            $this->sequence = -1;
        }
    }
    
    public function readNextPacket(bool $processOK = true): \Generator
    {
        if (!$this->connected) {
            throw new ConnectionException('Cannot read packges when connection has not been established');
        }
        
        $header = yield from $this->readWithLength(4);
        $off = 0;
        
        $len = $this->readInt24($header, $off);
        $this->sequence = $this->readInt8($header, $off);
        
        if ($len > 0) {
            $payload = yield from $this->readWithLength($len);
        } else {
            $payload = '';
        }
    
        switch (ord($payload)) {
            case 0x00:
            case 0xFE:
                if ($processOK) {
                    $off = 1;               
                    $this->readLengthEncodedInt($payload, $off);
                    
                    $this->lastInsertId = $this->readLengthEncodedInt($payload, $off) ?  : NULL;
                }
                break;
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
    
    public function readWithLength(int $length): \Generator
    {
        $chunk = yield from Stream::readBuffer($this->stream, $length);
        
        if (strlen($chunk) !== $length) {
            throw new ConnectionException(sprintf('Only %u bytes read but %u bytes were requested', strlen($chunk), $length));
        }
        
        return $chunk;
    }
    
    public function canSendCommand(): bool
    {
        return $this->connected && $this->sequence < 0;
    }
    
    public function sendCommand(string $packet): \Generator
    {
        if (!$this->connected) {
            throw new ConnectionException('Cannot send commands before a connection has been established');
        }
        
        if ($this->sequence >= 0) {
            throw new ConnectionException('Cannot send command while the connection is busy processing another command');
        }
        
        $packet = $this->encodeInt24(strlen($packet)) . chr(++$this->sequence) . $packet;
        
        return yield from $this->stream->write($packet);
    }

    public function sendPacket(string $packet): \Generator
    {
        if (!$this->connected) {
            throw new ConnectionException('Cannot send packet before a connection has been established');
        }
        
        if ($this->sequence < 0) {
            throw new ConnectionException('Cannot send additional package when the connection is not processing a command');
        }
        
        $packet = $this->encodeInt24(strlen($packet)) . chr(++$this->sequence % 256) . $packet;
        
        return yield from $this->stream->write($packet);
    }
    
    public function secureAuth(string $password, string $scramble): string
    {
        $hash = sha1($password, true);
    
        return $hash ^ sha1(substr($scramble, 0, 20) . sha1($hash, true), true);
    }
    
    public function readLengthEncodedInt(string $data, int & $off = 0)
    {
        $int = ord(substr($data, $off));
        $off++;
    
        if ($int <= 0xfb) {
            return $int;
        }
    
        if ($int == 0xfc) {
            return $this->readInt16($data, $off);
        }
    
        if ($int == 0xfd) {
            return $this->readInt24($data, $off);
        }
    
        if ($int == 0xfe) {
            return $this->readInt64($data, $off);
        }
    
        throw new ProtocolError("$int is not in ranges [0x00, 0xfa] or [0xfc, 0xfe]");
    }
    
    public function readInt8(string $data, int & $off = 0): int
    {
        try {
            return ord(substr($data, $off));
        } finally {
            $off += 1;
        }
    }
    
    public function readInt16(string $data, int & $off = 0): int
    {
        try {
            return unpack('v', substr($data, $off))[1];
        } finally {
            $off += 2;
        }
    }
    
    public function readInt24(string $data, int & $off = 0): int
    {
        try {
            return unpack('V', substr($data, $off, 3) . "\x00")[1];
        } finally {
            $off += 3;
        }
    }
    
    public function readInt32(string $data, int & $off = 0): int
    {
        try {
            return unpack('V', substr($data, $off))[1];
        } finally {
            $off += 4;
        }
    }
    
    public function readUnsigned32(string $data, int & $off = 0): int
    {
        try {
            if (PHP_INT_MAX >> 31) {
                return unpack('V', substr($data, $off))[1];
            }
    
            $int = unpack('v', substr($data, $off));
    
            return $int[1] + ($int[2] * (1 << 16));
        } finally {
            $off += 4;
        }
    }
    
    public function readInt64(string $data, int & $off = 0): float
    {
        try {
            if (PHP_INT_MAX >> 31) {
                $int = unpack('Va/Vb', substr($data, $off));
    
                return $int['a'] + ($int['b'] << 32);
            }
    
            $int = unpack('va/vb/Vc', substr($data, $off));
            
            return $int['a'] + ($int['b'] * (1 << 16)) + $int['c'] * (1 << 16) * (1 << 16);
        } finally {
            $off += 8;
        }
    }
    
    public function readUnsigned64(string $data, int & $off = 0): float
    {
        try {
            if (PHP_INT_MAX >> 31) {
                $int = unpack('Va/Vb', substr($data, $off));
    
                return $int['a'] + $int['b'] * (1 << 32);
            }
    
            $int = unpack('va/vb/vc/vd', substr($data, $off));
    
            return $int['a'] + ($int['b'] * (1 << 16)) + ($int['c'] + ($int['d'] * (1 << 16))) * (1 << 16) * (1 << 16);
        } finally {
            $off += 8;
        }
    }
    
    public function readFixedLengthString(string $data, int $len, int & $off = 0): string
    {
        $str = substr($data, $off, $len);
        $off += strlen($str);
    
        return $str;
    }
    
    public function readNullString(string $data, int & $off = 0): string
    {
        $str = substr($data, $off, strpos($data, "\0", $off) - 1);
        $off += strlen($str) + 1;
    
        return $str;
    }
    
    public function readLengthEncodedString(string $data, int & $off = 0)
    {
        $len = $this->readLengthEncodedInt($data, $off);
    
        if ($len < 1) {
            return '';
        }
    
        if ($len === 0xfb) {
            return NULL;
        }
    
        $str = substr($data, $off, $len);
        $off += strlen($str);
    
        return $str;
    }
    
    public function encodeInt(int $val): string
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
    
        throw new ProtocolError("encodeInt doesn't allow integers bigger than 2^64 - 1 (current: $val)");
    }
    
    public function encodeInt8(int $val): string
    {
        return chr($val);
    }
    
    public function encodeInt16(int $val): string
    {
        return pack('v', $val);
    }
    
    public function encodeInt24(int $val): string
    {
        return substr(pack('V', $val), 0, 3);
    }
    
    public function encodeInt32(int $val): string
    {
        return pack('V', $val);
    }
    
    public function encodeInt64(int $val): string
    {
        return pack('VV', $val & 0XFFFFFFFF, $val >> 32);
    }
    
    public function isLittleEndian()
    {
        static $result = NULL;
        
        if ($result === NULL) {
            return $result = unpack('S', "\x01\x00")[1] === 1;
        }
        
        return $result;
    }
    
    public function encodeBinary($val): array
    {
        $unsigned = false;
        
        switch (gettype($val)) {
            case 'boolean':
                $type = self::MYSQL_TYPE_TINY;
                $value = $val ? "\x01" : "\0";
                break;
            case 'integer':
                if ($val >= 0) {
                    $unsigned = true;
                }
                
                if ($val >= 0 && $val < (1 << 15)) {
                    $type = self::MYSQL_TYPE_SHORT;
                    $value = $this->encodeInt16($val);
                } else {
                    $type = self::MYSQL_TYPE_LONGLONG;
                    $value = $this->encodeInt64($val);
                }
                break;
            case 'double':
                $type = self::MYSQL_TYPE_DOUBLE;
                $value = pack('d', $val);
                
                if ($this->isLittleEndian()) {
                    $value = strrev($value);
                }
                break;
            case 'string':
                $type = self::MYSQL_TYPE_LONG_BLOB;
                $value = $this->encodeInt(strlen($val)) . $val;
                break;
            case 'NULL':
                $type = self::MYSQL_TYPE_NULL;
                $value = '';
                break;
            default:
                throw new ProtocolError("Unexpected type for binding parameter: " . gettype($val));
        }
        
        return [
            $unsigned,
            $type,
            $value
        ];
    }
    
    public function readBinary(int $type, string $data, int & $off)
    {
        // TODO: Implement more data types...
        
        $unsigned = $type & 0x80;
        
        switch ($type) {
            case self::MYSQL_TYPE_STRING:
            case self::MYSQL_TYPE_VARCHAR:
            case self::MYSQL_TYPE_VAR_STRING:
            case self::MYSQL_TYPE_ENUM:
            case self::MYSQL_TYPE_SET:
            case self::MYSQL_TYPE_LONG_BLOB:
            case self::MYSQL_TYPE_MEDIUM_BLOB:
            case self::MYSQL_TYPE_BLOB:
            case self::MYSQL_TYPE_TINY_BLOB:
            case self::MYSQL_TYPE_GEOMETRY:
            case self::MYSQL_TYPE_BIT:
            case self::MYSQL_TYPE_DECIMAL:
            case self::MYSQL_TYPE_NEWDECIMAL:
                return $this->readLengthEncodedString($data, $off);
            case self::MYSQL_TYPE_LONGLONG:
            case self::MYSQL_TYPE_LONGLONG | 0x80:
                return $unsigned && ($data[$off + 7] & "\x80") ? $this->readUnsigned64($data, $off) : $this->readInt64($data, $off);
            case self::MYSQL_TYPE_LONG:
            case self::MYSQL_TYPE_LONG | 0x80:
            case self::MYSQL_TYPE_INT24:
            case self::MYSQL_TYPE_INT24 | 0x80:
                $shift = PHP_INT_MAX >> 31 ? 32 : 0;
                
                return $unsigned && ($data[$off + 3] & "\x80") ? $this->readUnsigned32($data, $off) : (($this->readInt32($data, $off) << $shift) >> $shift);
            case self::MYSQL_TYPE_TINY:
            case self::MYSQL_TYPE_TINY | 0x80:
                $shift = PHP_INT_MAX >> 31 ? 56 : 24;
                
                return $unsigned ? $this->readInt8($data, $off) : (($this->readInt8($data, $off) << $shift) >> $shift);
            case self::MYSQL_TYPE_DOUBLE:
                try {
                    return unpack("d", substr($data, $off))[1];
                } finally {
                    $off += 8;
                }
            case self::MYSQL_TYPE_FLOAT:
                try {
                    return unpack("f", substr($data, $off))[1];
                } finally {
                    $off += 4;
                }
            case self::MYSQL_TYPE_NULL:
                return NULL;
            default:
                throw new ProtocolError(sprintf('Unsupported column type: 0x%02X', $type));
        }
    }
}
