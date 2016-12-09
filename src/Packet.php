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

class Packet
{
    /**
     * Numeric packet type (will be 0x00 when reading a raw packet).
     * 
     * @var int
     */
    public $type;
    
    protected $data;
    
    protected $length;
    
    protected $offset = 0;
    
    public function __construct(string $data, int $type = 0x00)
    {
        $this->data = $data;
        $this->length = \strlen($data);
        $this->type = $type;
    }

    public function __toString(): string
    {
        return $this->data;
    }

    public function getData(): string
    {
        return $this->data;
    }
    
    public function getLength(): int
    {
        return $this->length;
    }

    public function getOffset(): int
    {
        return $this->offset;
    }
    
    public function isConsumed(): bool
    {
        return $this->offset >= $this->length;
    }
    
    public function reset()
    {
        $this->offset = 0;
    }

    public function discardByte(int $expected = null)
    {
        if ($expected !== null) {
            if (\ord($this->data[$this->offset]) !== $expected) {
                throw new \RuntimeException(\sprintf('Expected byte 0x%02X, received 0x%02X', $expected, \ord($this->data[$this->offset])));
            }
        }
        
        $this->offset++;
    }

    public function readInt8(): int
    {
        return \ord(\substr($this->data, $this->offset++, 1));
    }

    public function readInt16(): int
    {
        try {
            return \unpack('v', \substr($this->data, $this->offset, 2))[1];
        } finally {
            $this->offset += 2;
        }
    }

    public function readInt24(): int
    {
        try {
            return \unpack('V', \substr($this->data, $this->offset, 3) . "\x00")[1];
        } finally {
            $this->offset += 3;
        }
    }

    public function readInt32(): int
    {
        try {
            return \unpack('V', \substr($this->data, $this->offset, 4))[1];
        } finally {
            $this->offset += 4;
        }
    }

    public function readInt64()
    {
        try {
            if (\PHP_INT_MAX >> 31) {
                $int = \unpack('Va/Vb', \substr($this->data, $this->offset, 8));
                
                return $int['a'] + ($int['b'] << 32);
            }
            
            $int = \unpack('va/vb/Vc', \substr($this->data, $this->offset, 8));
            
            return $int['a'] + ($int['b'] * (1 << 16)) + $int['c'] * (1 << 16) * (1 << 16);
        } finally {
            $this->offset += 8;
        }
    }

    public function readUnsigned32(): int
    {
        try {
            if (\PHP_INT_MAX >> 31) {
                return \unpack('V', \substr($this->data, $this->offset, 4))[1];
            }
            
            $int = \unpack('v', \substr($this->data, $this->offset, 4));
            
            return $int[1] + ($int[2] * (1 << 16));
        } finally {
            $this->offset += 4;
        }
    }

    public function readUnsigned64()
    {
        try {
            if (\PHP_INT_MAX >> 31) {
                $int = \unpack('Va/Vb', \substr($this->data, $this->offset, 8));
                
                return $int['a'] + $int['b'] * (1 << 32);
            }
            
            $int = \unpack('va/vb/vc/vd', \substr($this->data, $this->offset));
            
            return $int['a'] + ($int['b'] * (1 << 16)) + ($int['c'] + ($int['d'] * (1 << 16))) * (1 << 16) * (1 << 16);
        } finally {
            $this->offset += 8;
        }
    }

    public function readLengthEncodedInt()
    {
        $int = \ord(\substr($this->data, $this->offset++, 1));
        
        if ($int <= 0xFB) {
            return $int;
        }
        
        switch ($int) {
            case 0xFC:
                return $this->readInt16();
            case 0xFD:
                return $this->readInt24();
            case 0xFE:
                return $this->readInt64();
        }
        
        throw new \RuntimeException("$int is not in ranges [0x00, 0xFA] or [0xFC, 0xFE]");
    }

    public function readFixedLengthString(int $len): string
    {
        $str = \substr($this->data, $this->offset, $len);
        $this->offset += \strlen($str);
        
        return $str;
    }

    public function readNullString(): string
    {
        $str = \substr($this->data, $this->offset, \strpos($this->data, "\x00", $this->offset) - 1);
        $this->offset += \strlen($str) + 1;
        
        return $str;
    }

    public function readEofString(): string
    {
        try {
            return \substr($this->data, $this->offset);
        } finally {
            $this->offset = $this->length;
        }
    }

    public function readLengthEncodedString()
    {
        $len = $this->readLengthEncodedInt();
        
        if ($len < 1) {
            return '';
        }
        
        if ($len === 0xFB) {
            return null;
        }
        
        $str = \substr($this->data, $this->offset, $len);
        $this->offset += \strlen($str);
        
        return $str;
    }
    
    public function readNullBitmap(int $columnCount): array
    {
        $map = [];
        
        for ($i = 0; $i < $columnCount; $i++) {
            if (\ord($this->data[$this->offset + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                $map[$i] = null;
            }
        }
        
        $this->offset += ($columnCount + 9) >> 3;
        
        return $map;
    }

    public function readValue(int $type)
    {
        // TODO: Implement more data types...
        $unsigned = $type & 0x80;
        
        switch ($type) {
            case Client::MYSQL_TYPE_STRING:
            case Client::MYSQL_TYPE_VARCHAR:
            case Client::MYSQL_TYPE_VAR_STRING:
            case Client::MYSQL_TYPE_ENUM:
            case Client::MYSQL_TYPE_SET:
            case Client::MYSQL_TYPE_LONG_BLOB:
            case Client::MYSQL_TYPE_MEDIUM_BLOB:
            case Client::MYSQL_TYPE_BLOB:
            case Client::MYSQL_TYPE_TINY_BLOB:
            case Client::MYSQL_TYPE_GEOMETRY:
            case Client::MYSQL_TYPE_BIT:
            case Client::MYSQL_TYPE_DECIMAL:
            case Client::MYSQL_TYPE_NEWDECIMAL:
                return $this->readLengthEncodedString();
            case Client::MYSQL_TYPE_LONGLONG:
            case Client::MYSQL_TYPE_LONGLONG | 0x80:
                return ($unsigned && ($this->data[$this->offset + 7] & "\x80")) ? $this->readUnsigned64() : $this->readInt64();
            case Client::MYSQL_TYPE_LONG:
            case Client::MYSQL_TYPE_LONG | 0x80:
            case Client::MYSQL_TYPE_INT24:
            case Client::MYSQL_TYPE_INT24 | 0x80:
                $shift = PHP_INT_MAX >> 31 ? 32 : 0;
                
                return ($unsigned && ($this->data[$this->offset + 3] & "\x80")) ? $this->readUnsigned32() : (($this->readInt32() << $shift) >> $shift);
            case Client::MYSQL_TYPE_TINY:
            case Client::MYSQL_TYPE_TINY | 0x80:
                $shift = PHP_INT_MAX >> 31 ? 56 : 24;
                
                return $unsigned ? $this->readInt8() : (($this->readInt8() << $shift) >> $shift);
            case Client::MYSQL_TYPE_DOUBLE:
                try {
                    return \unpack('d', \substr($this->data, $this->offset))[1];
                } finally {
                    $this->offset += 8;
                }
            case Client::MYSQL_TYPE_FLOAT:
                try {
                    return \unpack('f', \substr($this->data, $this->offset))[1];
                } finally {
                    $this->offset += 4;
                }
            case Client::MYSQL_TYPE_NULL:
                return null;
        }
        
        throw new \InvalidArgumentException(\sprintf('Unsupported column type: 0x%02X', $type));
    }
}
