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

class ResultSet
{
    protected $conn;
    
    protected $client;
    
    protected $names;
    
    protected $columns;
    
    protected $columnCount;
    
    protected $affectedRows;
    
    protected $closed = false;
    
    public function __construct(Connection $conn, array $columns, int $affectedRows)
    {
        $this->conn = $conn;
        $this->client = $conn->getClient();
        $this->columns = $columns;
        $this->columnCount = count($this->columns);
        $this->affectedRows = $affectedRows;
        
        $this->names = array_map(function (array $col) {
            return $col['name'];
        }, $columns);
    }
    
    public function __debugInfo(): array
    {
        return [
            'affectedRows' => $this->affectedRows,
            'columns' => $this->names
        ];
    }
    
    public function rowCount(): int
    {
        return $this->affectedRows;
    }
    
    public function fetchRow(): \Generator
    {
        if ($this->closed) {
            return;
        }
        
        $packet = yield from $this->client->readNextPacket();
        
        if (ord($packet) === 0xFE) {
            $this->closed = true;
            
            $this->client->flush();
            
            return;
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
        
        for ($i = 0; $off < strlen($packet); $i++) {
            while (array_key_exists($i, $row)) {
                $i++;
            }
            
            $row[$i] = $this->client->readBinary($this->columns[$i]['type'], $packet, $off);
        }
        
        return array_combine($this->names, $row);
    }
    
    public function fetchRowsArray(): \Generator
    {
        if ($this->closed) {
            return [];
        }
        
        $rows = [];
        while (NULL !== ($row = yield from $this->fetchRow())) {
            $rows[] = $row;
        }
        
        return $rows;
    }
    
    public function fetchColumnArray(string $column): \Generator
    {
        $values = [];
        while (NULL !== ($row = yield from $this->fetchRow())) {
            $values[] = $row[$column];
        }
        
        return $values;
    }
}
