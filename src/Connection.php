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

use KoolKode\Async\Stream\SocketStream;

class Connection
{
    const DEFAULT_PORT = 3306;
    
    protected $client;
    
    public function __construct(Client $client)
    {
        $this->client = $client;
    }
    
    public function getClient(): Client
    {
        return $this->client;
    }
    
    public static function connect(string $dsn, string $username, string $password): \Generator
    {
        if ('mysql:' !== substr($dsn, 0, 6)) {
            throw new \InvalidArgumentException(sprintf('Invalid MySQL DSN: "%s"', $dsn));
        }
        
        $settings = [];
        
        foreach (explode(';', substr($dsn, 6)) as $part) {
            list ($k, $v) = array_map('trim', explode('=', $part));
            
            switch ($k) {
                case 'host':
                case 'dbname':
                    $settings[$k] = $v;
                    break;
                case 'port':
                    $settings[$k] = (int)$v;
                    break;
                default:
                    throw new \InvalidArgumentException(sprintf('Unknown MySQL DSN param: "%s"', $k));
            }
        }
        
        if (empty($settings['host'])) {
            throw new \InvalidArgumentException('Missing MySQL host in DSN');
        }
        
        $host = $settings['host'];
        $port = $settings['port'] ?? self::DEFAULT_PORT;
        
        $client = new Client(yield from SocketStream::connect($host, $port));

        yield from $client->handleHandshake($username, $password);
        
        $conn = new static($client);
        
        if (!empty($settings['dbname'])) {
            yield from $conn->changeDefaultSchema($settings['dbname']);
        }
        
        return $conn;
    }
    
    public function lastInsertId()
    {
        return $this->client->getLastInsertId();
    }
    
    public function close(): \Generator
    {
        try {
            if ($this->client->canSendCommand()) {
                yield from $this->client->sendCommand($this->client->encodeInt8(0x01));
            }
        } finally {
            $this->client->close();
        }
    }
    
    public function changeDefaultSchema(string $schema): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x02) . $schema);
            yield from $this->client->readNextPacket();
        } finally {
            $this->client->flush();
        }
    }
    
    public function ping(): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x0E));
            
            $response = yield from $this->client->readNextPacket();
            
            return ord($response) === 0x00;
        } finally {
            $this->client->flush();
        }
    }
    
    public function query(string $query): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x03) . $query);
            
            $response = yield from $this->client->readNextPacket();
            $colCount = $this->client->readLengthEncodedInt($response);
            
            if ($colCount < 0) {
                return;
            }
            
            $columns = [];
            for ($i = 0; $i < $colCount; $i++) {
                $columns[] = $this->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if (!$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after column definitions');
            }
            
            $rows = [];
            
            while (true) {
                $packet = yield from $this->client->readNextPacket();
                
                switch (ord($packet)) {
                    case 0x00:
                    case 0xFE:
                        break 2;
                }
                
                $row = [];
                
                for ($off = 0, $i = 0; $i < $colCount; $i++) {
                    if (0xFB === ord($packet[$off])) {
                        $row[$columns[$i]['name']] = NULL;
                    } else {
                        $row[$columns[$i]['name']] = $this->readTextCol($columns[$i]['type'], $packet, $off);
                    }
                }
                
                $rows[] = $row;
            }
            
            $off = 1;
            
            $affected = $this->client->readLengthEncodedInt($packet, $off);
            $lastInsertId = $this->client->readLengthEncodedInt($packet, $off);
//             $statusFlags = 0;
//             $numWarnings = 0;
            
//             if ($this->capabilities & Client::CLIENT_PROTOCOL_41) {
//                 $statusFlags = $this->client->readInt16($packet, $off);
//                 $numWarnings = $this->client->readInt16($packet, $off);
//             } elseif ($this->capabilities & Client::CLIENT_TRANSACTIONS) {
//                 $statusFlags = $this->client->readInt16($packet, $off);
//             }
            
            if ($this->client->hasCapabilty(Client::CLIENT_SESSION_TRACK)) {
                $info = $this->client->readLengthEncodedString($packet, $off);
                
                if ($statusFlags & Client::SERVER_SESSION_STATE_CHANGED) {
                    $changes = $this->client->readLengthEncodedString($packet, $off);
                }
            } else {
                $info = substr($packet, $off);
            }
            
            return $rows;
        } finally {
            $this->client->flush();
        }
    }
    
    public function prepare(string $sql): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x16) . $sql);
            
            $packet = yield from $this->client->readNextPacket();
            $off = 0;
            
            $this->assert($this->client->readInt8($packet, $off) === 0x00, 'Status is not OK');
            
            $id = $this->client->readInt32($packet, $off);
            
            $cc = $this->client->readInt16($packet, $off);
            $pc = $this->client->readInt16($packet, $off);
            
            $this->assert($this->client->readInt8($packet, $off) === 0x00, 'Missing filler');
            
            // Warning count:
            $this->client->readInt16($packet, $off);
            
            $cols = [];
            $params = [];
            
            // Params:
            for ($i = 0; $i < $pc; $i++) {
                $params[] = $this->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($pc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after param definitions');
            }
            
            // Columns:
            for ($i = 0; $i < $cc; $i++) {
                $cols[] = $this->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($cc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after column definitions');
            }
            
            return new Statement($this, $id, $cols, $params);
        } finally {
            $this->client->flush();
        }
    }
    
    public function assert(bool $condition, string $message)
    {
        if (!$condition) {
            throw new ProtocolError($message);
        }
    }
    
    public function parseColumnDefinition(string $packet): array
    {
        $off = 0;
        
        $this->assert('def' === $this->client->readLengthEncodedString($packet, $off), 'Missing column def');
        
        $col = [
            'schema' => $this->client->readLengthEncodedString($packet, $off),
            'table' => $this->client->readLengthEncodedString($packet, $off),
            'org_table' => $this->client->readLengthEncodedString($packet, $off),
            'name' => $this->client->readLengthEncodedString($packet, $off),
            'org_name' => $this->client->readLengthEncodedString($packet, $off)
        ];
        
        $this->assert(0 < $this->client->readLengthEncodedInt($packet, $off), 'Missing fixed column values');
        
        $col['charset'] = $this->client->readInt16($packet, $off);
        $col['length'] = $this->client->readInt32($packet, $off);
        $col['type'] = $this->client->readInt8($packet, $off);
        $col['flags'] = $this->client->readInt16($packet, $off);
        $col['decimals'] = $this->client->readInt8($packet, $off);
        
        $this->assert("\0\0" === $this->client->readFixedLengthString($packet, 2, $off), 'Missing column definition filler');
        
        return $col;
    }
    
    protected function readTextCol(int $type, string $data, int & $off)
    {
        $unsigned = $type & 0x80;
        $str = $this->client->readLengthEncodedString($data, $off);
        
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
                return $str;
            case Client::MYSQL_TYPE_LONGLONG:
            case Client::MYSQL_TYPE_LONGLONG | 0x80:
                return ($unsigned && ($str[7] & "\x80")) ? $this->client->readUnsigned64(str_pad($str, 8, "\0", STR_PAD_RIGHT)) : $this->client->readInt64(str_pad($str, 8, "\0", STR_PAD_RIGHT));
            case Client::MYSQL_TYPE_LONG:
            case Client::MYSQL_TYPE_LONG | 0x80:
            case Client::MYSQL_TYPE_INT24:
            case Client::MYSQL_TYPE_INT24 | 0x80:
            case Client::MYSQL_TYPE_TINY:
            case Client::MYSQL_TYPE_TINY | 0x80:
                return (int) $str;
            case Client::MYSQL_TYPE_DOUBLE:
            case Client::MYSQL_TYPE_FLOAT:
                return (float) $str;
            case Client::MYSQL_TYPE_NULL:
                return NULL;
        }
    }
}
