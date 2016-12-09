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

/**
 * Assembles data of a MySQL packet using convenient mutator methods.
 * 
 * @author Martin Schröder
 */
class PacketBuilder
{
    /**
     * Assembled encoded packet data.
     * 
     * @var string
     */
    protected $data;
    
    public function __construct(string $data = '')
    {
        $this->data = $data;
    }

    public function __toString(): string
    {
        return $this->data;
    }

    public function build(): string
    {
        return $this->data;
    }
    
    public function isLittleEndian()
    {
        static $result = null;
        
        return $result ?? ($result = (\unpack('S', "\x01\x00")[1] === 1));
    }
    
    public function write(string $bytes): PacketBuilder
    {
        $this->data .= $bytes;
        
        return $this;
    }

    public function writeInt(int $val): PacketBuilder
    {
        if ($val < 0xFB) {
            return $this->writeInt8($val);
        }
        
        if ($val < (1 << 16)) {
            $this->data .= "\xFC";
            
            return $this->writeInt16($val);
        }
        
        if ($val < (1 << 24)) {
            $this->data .= "\xFD";
            
            return $this->writeInt24($val);
        }
        
        if ($val < (1 << 62) * 4) {
            $this->data .= "\xFE";
            
            return $this->writeInt64($val);
        }
        
        throw new \RuntimEexception("Cannot encode integer bigger than 2^64 - 1 (current: $val)");
    }

    public function writeInt8(int $val): PacketBuilder
    {
        $this->data .= \chr($val);
        
        return $this;
    }

    public function writeInt16(int $val): PacketBuilder
    {
        $this->data .= \pack('v', $val);
        
        return $this;
    }

    public function writeInt24(int $val): PacketBuilder
    {
        $this->data .= \substr(\pack('V', $val), 0, 3);
        
        return $this;
    }

    public function writeInt32(int $val): PacketBuilder
    {
        $this->data .= \pack('V', $val);
        
        return $this;
    }
    
    public function writeNullString(string $string): PacketBuilder
    {
        $this->data .= $string . "\x00";
        
        return $this;
    }

    public function writeInt64(int $val): string
    {
        $this->data .= \pack('VV', $val & 0xFFFFFFFF, $val >> 32);
        
        return $this;
    }

    public function writeLengthEncodedString(string $string): PacketBuilder
    {
        $this->writeInt(\strlen($string));
        $this->data .= $string;
        
        return $this;
    }
}
