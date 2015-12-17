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

class Statement
{
    const CURSOR_TYPE_NO_CURSOR = 0x00;
    
    const CURSOR_TYPE_READ_ONLY = 0x01;
    
    const CURSOR_TYPE_FOR_UPDATE = 0x02;
    
    const CURSOR_TYPE_SCROLLABLE = 0x04;
    
    protected $conn;
    
    protected $client;
    
    protected $id;
    
    protected $columns;
    
    protected $params;
    
    protected $bound = [];
    
    public function __construct(Connection $conn, int $id, array $columns, array $params)
    {
        $this->conn = $conn;
        $this->client = $conn->getClient();
        $this->id = $id;
        $this->columns = $columns;
        $this->params = $params;
    }
    
    public function bindValue(int $pos, $val)
    {
        $this->bound[$pos] = $val;
    }
    
    public function execute(): \Generator
    {
        if (count($this->params) !== count($this->bound)) {
            throw new ConnectionException(sprintf('Statement contains %u placeholders, given %u values', count($this->params), count($this->bound)));
        }
        
        try {
            $packet = $this->client->encodeInt8(0x17);
            $packet .= $this->client->encodeInt32($this->id);
            
            $flags = self::CURSOR_TYPE_NO_CURSOR;
            
            $packet .= $this->client->encodeInt8($flags);
            $packet .= $this->client->encodeInt32(1);
            
            $bound = !empty($this->bound);
            
            if (!empty($this->params)) {
                $args = $this->bound;
                $types = '';
                $values = '';
                
                ksort($args, SORT_NUMERIC);
                
                // Append NULL-bitmap with all bits set to 0:
                $nullOffset = strlen($packet);
                $packet .= str_repeat("\0", (count($this->bound) + 7) >> 3);
                
                foreach ($args as $i => $val) {
                    if ($val === NULL) {
                        // Set NULL bit at param position to 1:
                        $off = $nullOffset + ($i >> 3);
                        $packet[$off] = $packet[$off] | chr(1 << ($i % 8));
                    } else {
                        $bound = true;
                    }
                    
                    list ($unsigned, $type, $val) = $this->client->encodeBinary($val);
                    
                    $types .= $this->client->encodeInt8($type);
                    $types .= $unsigned ? "\x80" : "\0";
                    $values .= $val;
                }
                
                $packet .= $this->client->encodeInt8((int) $bound);
                
                if ($bound) {
                    $packet .= $types;
                    $packet .= $values;
                }
            }
            
            yield from $this->client->sendPacket($packet);
            
            $packet = yield from $this->client->readNextPacket();
            $off = 0;
            if (ord($packet) === 0x00 || ord($packet) === 0xFE) {
                $off = 1;
                
                $affected = $this->client->readLengthEncodedInt($packet, $off);
                // $lastInsertId = $this->client->readLengthEncodedInt($packet, $off);

                return $affected;
            }
            
            $columns = [];
            $cc = $this->client->readLengthEncodedInt($packet, $off);
            
            for ($i = 0; $i < $cc; $i++) {
                $columns[] = $this->conn->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($cc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->conn->assert(ord(yield from $this->client->readNextPacket()) === 0xFE, 'Missing EOF after column definitions');
            }
            
            $rows = [];
            $names = array_map(function (array $col) {
                return $col['name'];
            }, $columns);
            
            while (true) {
                $packet = yield from $this->client->readNextPacket();
                
                if (ord($packet) === 0xFE) {
                    break;
                }
                
                $off = 0;
                $this->conn->assert($this->client->readInt8($packet, $off) === 0x00, 'Missing packet header in result row');
                
                $fields = [];
                for ($i = 0; $i < $cc; $i++) {
                    if (ord($packet[$off + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                        $fields[$i] = NULL;
                    }
                }
                $off += ($cc + 9) >> 3;
                
                for ($i = 0; $off < \strlen($packet); $i++) {
                    while (array_key_exists($i, $fields)) {
                        $i++;
                    }
                    
                    $fields[$i] = $this->client->readBinary($columns[$i]['type'], $packet, $off);
                }
                
                $rows[] = array_combine($names, $fields);
            }
            
            return $rows;
        } finally {
            $this->client->flush();
        }
    }
}
