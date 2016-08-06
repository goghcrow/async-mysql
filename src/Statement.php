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

use KoolKode\Async\Database\DatabaseException;
use KoolKode\Async\Database\StatementInterface;
use Psr\Log\LoggerInterface;

class Statement implements StatementInterface
{
    const CURSOR_TYPE_NO_CURSOR = 0x00;
    
    const CURSOR_TYPE_READ_ONLY = 0x01;
    
    const CURSOR_TYPE_FOR_UPDATE = 0x02;
    
    const CURSOR_TYPE_SCROLLABLE = 0x04;
        
    protected $id;

    protected $sql;
    
    protected $params = [];
    
    protected $limit = 0;

    protected $offset = 0;

    protected $first = true;

    protected $recompile = true;

    protected $closed = false;
    
    protected $columnCount = 0;
    
    protected $columnNames = [];
    
    protected $columnIndex = [];
    
    protected $columnDefinitions = [];
    
    protected $paramDefinitions = [];
    
    protected $conn;
    
    protected $client;
    
    protected $logger;
    
    public function __construct(string $sql, Connection $conn, Client $client, LoggerInterface $logger = NULL)
    {
        $this->sql = $sql;
        $this->conn = $conn;
        $this->client = $client;
        $this->logger = $logger;
    }
    
    public function __destruct()
    {
        $this->close();
    }
    
    public function close()
    {
        if ($this->id !== NULL && !$this->closed) {
            $this->conn->queueWork(function () {
                $this->conn->releaseStatement($this->id);
            });
        }
    }
    
    public function closeCursor()
    {
        if ($this->id !== NULL && !$this->closed) {
        
        }
    }

    /**
     * {@inheritdoc}
     */
    public function setLimit(int $limit)
    {
        if ($limit < 0) {
            throw new \InvalidArgumentException('Limit must not be negative');
        }
        
        if ($this->limit !== $limit) {
            $this->recompile = true;
        }
        
        $this->limit = $limit;
    }

    /**
     * {@inheritdoc}
     */
    public function setOffset(int $offset)
    {
        if ($offset < 0) {
            throw new \InvalidArgumentException('Offset must not be negative');
        }
        
        if ($this->offset !== $offset) {
            $this->recompile = true;
        }
        
        $this->offset = $offset;
    }

    /**
     * {@inheritdoc}
     */
    public function bindParam($value)
    {
        $this->params[] = $value;
    }

    /**
     * {@inheritdoc}
     */
    public function bindParams(array $params)
    {
        foreach ($params as $v) {
            $this->bindParam($v);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function updateParam(int $index, $value)
    {
        if (!array_key_exists($index, $this->params)) {
            throw new \OutOfBoundsException(sprintf('Param %u not defined in DB statement', $index));
        }
        
        $this->params[$index] = $value;
    }

    /**
     * {@inheritdoc}
     */
    public function updateParams(array $params)
    {
        foreach ($params as $k => $v) {
            $this->updateParam($k, $v);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function resetParams()
    {
        $this->params = [];
    }
    
    public function free()
    {
        if (!$this->closed) {
            $this->closed = true;
            $this->conn->releaseStatement($this->id);
        }
    }
    
    public function execute(): \Generator
    {
        try {
            yield from $this->conn->awaitPendingTasks();
            
            if ($this->recompile) {
                $this->close();
                
                yield from $this->conn->awaitPendingTasks();
                
                $sql = $this->sql;
                
                if ($this->limit > 0) {
                    $sql .= ' LIMIT ' . $this->limit;
                    
                    if ($this->offset > 0) {
                        $sql .= ' OFFSET ' . $this->offset;
                    }
                }
                
                yield from $this->client->sendCommand($this->client->encodeInt8(0x16) . $sql);
                
                $packet = yield from $this->client->readNextPacket(false);
                $off = 0;
                
                $this->conn->assert($this->client->readInt8($packet, $off) === 0x00, 'Status is not OK');
                
                $this->id = $this->client->readInt32($packet, $off);
                
                $cc = $this->client->readInt16($packet, $off);
                $pc = $this->client->readInt16($packet, $off);
                
                $this->conn->assert($this->client->readInt8($packet, $off) === 0x00, 'Missing filler');
                
                // Warning count:
                $this->client->readInt16($packet, $off);
                
                $this->paramDefinitions = [];
                
                // Params:
                for ($i = 0; $i < $pc; $i++) {
                    $this->paramDefinitions[] = $this->conn->parseColumnDefinition(yield from $this->client->readNextPacket());
                }
                
                if ($pc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                    $this->conn->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after param definitions');
                }
                
                // Columns:
                for ($i = 0; $i < $cc; $i++) {
                    $this->conn->parseColumnDefinition(yield from $this->client->readNextPacket());
                }
                
                if ($cc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                    $this->conn->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after column definitions');
                }
                
                $this->recompile = false;
                
                $this->client->flush();
            }
            
            if (count($this->paramDefinitions) !== count($this->params)) {
                throw new ConnectionException(sprintf('Statement contains %u placeholders, given %u values', count($this->paramDefinitions), count($this->params)));
            }
            
            yield from $this->conn->awaitPendingTasks();
            
            $packet = $this->client->encodeInt8(0x17);
            $packet .= $this->client->encodeInt32($this->id);
            
            $flags = self::CURSOR_TYPE_NO_CURSOR;
            
            $packet .= $this->client->encodeInt8($flags);
            $packet .= $this->client->encodeInt32(1);
            
            $bound = !empty($this->params);
            
            if (!empty($this->params)) {
                $types = '';
                $values = '';
                
                // Append NULL-bitmap with all bits set to 0:
                $nullOffset = \strlen($packet);
                $packet .= str_repeat("\0", (count($this->params) + 7) >> 3);
                
                foreach ($this->params as $i => $val) {
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
            
            yield from $this->client->sendCommand($packet);
            
            $this->processing = true;
            
            $packet = yield from $this->client->readNextPacket();
            $off = 0;
            
            if (ord($packet) === 0x00 || ord($packet) === 0xFE) {
                $off = 1;
                $affected = $this->client->readLengthEncodedInt($packet, $off);
                
                $this->client->flush();
                $this->closed = true;
                
                return $affected;
            }
            
            $this->columnDefinitions = [];
            $this->columnCount = $this->client->readLengthEncodedInt($packet, $off);
            
            for ($i = 0; $i < $this->columnCount; $i++) {
                $this->columnDefinitions[] = $this->conn->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($this->columnCount > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->conn->assert(ord(yield from $this->client->readNextPacket()) === 0xFE, 'Missing EOF after column definitions');
            }
            
            $this->columnNames = array_map(function (array $def) {
                return $def['name'];
            }, $this->columnDefinitions);
            $this->columnIndex = array_flip($this->columnNames);
            
            $this->first = true;
            
            return 0;
        } catch (\Throwable $e) {
            throw new DatabaseException('Failed to prepare / execute statement', 0, $e);
        }
    }

    public function nextRow(): \Generator
    {
        $this->first = false;
        
        if ($this->closed) {
            return false;
        }
        
        try {
            $packet = yield from $this->client->readNextPacket(false);
            
            if ($packet === '' || ord($packet) === 0xFE) {
                $this->closed = true;
                
                $this->client->flush();
                
                return false;
            }
            
            $off = 0;
            $this->conn->assert($this->client->readInt8($packet, $off) === 0x00, 'Missing packet header in result row');
            
            $row = [];
            for ($i = 0; $i < $this->columnCount; $i++) {
                if (ord($packet[$off + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                    $row[$i] = NULL;
                }
            }
            $off += ($this->columnCount + 9) >> 3;
            
            for ($i = 0; $off < \strlen($packet); $i++) {
                while (array_key_exists($i, $row)) {
                    $i++;
                }
                
                $row[$i] = $this->client->readBinary($this->columnDefinitions[$i]['type'], $packet, $off);
            }
            
            ksort($row, SORT_NUMERIC);
            
            $this->row = array_combine($this->columnNames, $row);
            
            return true;
        } catch (\Throwable $e) {
            throw new DatabaseException('Failed to fetch next row', 0, $e);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function row(): array
    {
        if ($this->first) {
            throw new DatabaseException('Cannot read a row before it has been fetched using nextRow()');
        }
        
        if ($this->closed) {
            throw new DatabaseException('Cannot read from closed statement');
        }
        
        return $this->row;
    }

    /**
     * {@inheritdoc}
     */
    public function column(string $name, $default = NULL)
    {
        if ($this->first) {
            throw new DatabaseException('Cannot read a row before it has been fetched using nextRow()');
        }
        
        if ($this->closed) {
            throw new DatabaseException('Cannot read from closed statement');
        }
        
        if (!isset($this->columnIndex[$name])) {
            throw new \OutOfBoundsException(sprintf('Result column not found: "%s"', $name));
        }
        
        return $this->row[$name] ?? $default;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchRows(): \Generator
    {
        if ($this->closed) {
            throw new DatabaseException('Cannot read from closed statement');
        }
        
        $result = [];
        
        while (yield from $this->nextRow()) {
            $result[] = $this->row;
        }
        
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn(string $name): \Generator
    {
        if ($this->closed) {
            throw new DatabaseException('Cannot read from closed statement');
        }
        
        if (!isset($this->columnIndex[$name])) {
            throw new \OutOfBoundsException(sprintf('Result column not found: "%s"', $name));
        }
        
        $result = [];
        
        while (yield from $this->nextRow()) {
            $result[] = $this->row[$name];
        }
        
        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchMap(string $key, string $value): \Generator
    {
        if ($this->closed) {
            throw new DatabaseException('Cannot read from closed statement');
        }
        
        if (!isset($this->columnIndex[$key])) {
            throw new \OutOfBoundsException(sprintf('Key column not found: "%s"', $key));
        }
        
        if (!isset($this->columnIndex[$value])) {
            throw new \OutOfBoundsException(sprintf('Value column not found: "%s"', $value));
        }
        
        $result = [];
        
        while (yield from $this->nextRow()) {
            $result[$this->row[$key]] = $this->row[$value];
        }
        
        return $result;
    }
}
