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

use Psr\Log\LoggerInterface;
use KoolKode\Async\Awaitable;
use KoolKode\Async\Coroutine;
use KoolKode\Async\Deferred;
use KoolKode\Async\Success;
use KoolKode\Async\Util\Channel;

/**
 * Prepared statement that encapsulates an SQL query.
 * 
 * @author Martin Schröder
 */
class Statement
{
    const CURSOR_TYPE_NO_CURSOR = 0x00;

    const CURSOR_TYPE_READ_ONLY = 0x01;

    const CURSOR_TYPE_FOR_UPDATE = 0x02;

    const CURSOR_TYPE_SCROLLABLE = 0x04;

    /**
     * Original SQL query string.
     * 
     * @var string
     */
    protected $sql;

    /**
     * Client to be used for sending commands.
     * 
     * @var Client
     */
    protected $client;

    /**
     * Optional PSR logger instance.
     * 
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * ID of the statement if it has been prepared.
     * 
     * @var int
     */
    protected $id;
    
    /**
     * Recompile / prepare the statement before next execution?
     * 
     * @var bool
     */
    protected $recompile = false;
    
    /**
     * Limit to be applied to the query.
     * 
     * @var int
     */
    protected $limit = 0;
    
    /**
     * Offset to be applied to the query.
     * 
     * @var int
     */
    protected $offset = 0;
    
    /**
     * Bound param values.
     * 
     * @var array
     */
    protected $params = [];
    
    /**
     * Definitions of all params as required by the DB (filled as statement is prepared).
     * 
     * @var array
     */
    protected $paramDefinitions = [];

    public function __construct(string $sql, Client $client, LoggerInterface $logger = null)
    {
        $this->sql = $sql;
        $this->client = $client;
        $this->logger = $logger;
    }

    public function __destruct()
    {
        $this->dispose();
    }

    /**
     * Dispose of the prepared statement.
     */
    public function dispose(): Awaitable
    {        
        if ($this->id) {
            try {
                $id = $this->id;
                
                return $this->client->sendCommand(function (Client $client) use ($id) {
                    $this->paramDefinitions = [];
                    
                    $builder = new PacketBuilder();
                    $builder->writeInt8(0x19);
                    $builder->writeInt32($id);
                    
                    yield from $client->sendPacket($builder->build());
                });
            } finally {
                $this->id = null;
            }
        }
        
        return new Success(null);
    }

    /**
     * Apply a limit to the number of rows returned by the query.
     * 
     * @param int $limit
     * @return Statement
     * 
     * @throws \InvalidArgumentException When a limit value less than 1 is given.
     */
    public function limit(int $limit): Statement
    {
        if ($limit < 1) {
            throw new \InvalidArgumentException('Limit must not be less than 1');
        }
        
        if ($limit !== $this->limit) {
            $this->limit = $limit;
            $this->recompile = true;
        }
        
        return $this;
    }

    /**
     * Skip the given number of result rows.
     * 
     * @param int $offset
     * @return Statement
     * 
     * @throws \InvalidArgumentException When a negative offset is given.
     */
    public function offset(int $offset): Statement
    {
        if ($offset < 0) {
            throw new \InvalidArgumentException('Offset must not be negative');
        }
        
        if ($offset !== $this->offset) {
            $this->offset = $offset;
            $this->recompile = true;
        }
        
        return $this;
    }
    
    /**
     * Bind a value to a param placeholder.
     * 
     * @param int $pos 0-indexed parameter position.
     * @param mixed $value
     * @return Statement
     * 
     * @throws \InvalidArgumentException When the param index is negative.
     */
    public function bind(int $pos, $value): Statement
    {
        if ($pos < 0) {
            throw new \InvalidArgumentException('Param index must not be negative');
        }
        
        $this->params[$pos] = $value;
        
        return $this;
    }

    /**
     * Execute the prepared statement.
     * 
     * @return ResultSet
     */
    public function execute(): Awaitable
    {
        $coroutine = null;
        
        $defer = new Deferred(function ($defer, \Throwable $e) use (& $coroutine) {
            $coroutine->cancel($e);
        });
        
        $coroutine = new Coroutine(function () use ($defer) {
            try {
                if ($this->id === null || $this->recompile) {
                    if ($this->id !== null) {
                        yield $this->dispose();
                    }
                    
                    yield $this->client->sendCommand(function (Client $client) {
                        return $this->prepareQuery($client);
                    });
                }
                
                return yield $this->client->sendCommand(function (Client $client) use ($defer) {
                    return $this->executeQuery($client, $defer);
                }, false);
            } catch (\Throwable $e) {
                $defer->cancel($e);
            }
        });
        
        return $defer;
    }

    protected function throwError(Packet $packet)
    {
        $code = $packet->readInt16();
        $marker = $packet->readFixedLengthString(1);
        $state = $packet->readFixedLengthString(5);
        $message = $packet->readEofString();
        
        throw new \RuntimeException(\sprintf('Failed to prepare SQL query: SQLSTATE [%s]: "%s"', $state, $message), $code);
    }

    protected function prepareQuery(Client $client): \Generator
    {
        $sql = $this->sql;
        
        if ($this->limit) {
            $sql .= ' LIMIT ' . $this->limit;
            
            if ($this->offset) {
                $sql .= ' OFFSET ' . $this->offset;
            }
        }
        
        $builder = new PacketBuilder();
        $builder->writeInt8(0x16);
        $builder->write($sql);
        
        yield from $client->sendPacket($builder->build());
        
        $packet = yield from $client->readPacket(0x00, 0xFF);
        
        if ($packet->type === 0xFF) {
            return $this->throwError($packet);
        }
        
        $this->id = $packet->readInt32();
        $columnCount = $packet->readInt16();
        $paramCount = $packet->readInt16();
        
        // Discard filler:
        $packet->discardByte(0x00);
        
        $warningCount = $packet->readInt16();
        
        for ($i = 0; $i < $paramCount; $i++) {
            $this->paramDefinitions[] = $this->parseColumnDefinition($client, yield from $client->readRawPacket());
        }
        
        if ($paramCount && !$client->isEofDeprecated()) {
            yield from $client->readPacket(0xFE);
        }
        
        for ($i = 0; $i < $columnCount; $i++) {
            $this->parseColumnDefinition($client, yield from $client->readRawPacket());
        }
        
        if ($columnCount && !$client->isEofDeprecated()) {
            yield from $client->readPacket(0xFE);
        }
    }

    protected function parseColumnDefinition(Client $client, Packet $packet): array
    {
        $col = [
            'catalog' => $packet->readLengthEncodedString(),
            'schema' => $packet->readLengthEncodedString(),
            'tableAlias' => $packet->readLengthEncodedString(),
            'table' => $packet->readLengthEncodedString(),
            'columnAlias' => $packet->readLengthEncodedString(),
            'column' => $packet->readLengthEncodedString()
        ];
        
        if (0x0C !== $packet->readLengthEncodedInt()) {
            throw new \RuntimeException('Invalid length of colum description fields');
        }
        
        $col['charset'] = $packet->readInt16();
        $col['length'] = $packet->readInt32();
        $col['type'] = $packet->readInt8();
        $col['flags'] = $packet->readInt16();
        $col['decimals'] = $packet->readInt8();
        
        $packet->discardByte(0x00);
        $packet->discardByte(0x00);
        
        return $col;
    }

    protected function executeQuery(Client $client, Deferred $defer, int $prefetch = 4): \Generator
    {
        if (\count($this->params) !== \count($this->paramDefinitions)) {
            throw new \RuntimeException(\sprintf('Query requires %u params, %u params bound', \count($this->paramDefinitions), \count($this->params)));
        }
        
        $builder = new PacketBuilder();
        $builder->writeInt8(0x17);
        $builder->writeInt32($this->id);
        $builder->writeInt8(self::CURSOR_TYPE_NO_CURSOR);
        $builder->writeInt32(1);
        
        if (!empty($this->params)) {
            $builder->write($this->encodeParams());
        }
        
        yield from $client->sendPacket($builder->build());
        
        $packet = yield from $client->readRawPacket();
        
        switch (\ord($packet->getData()[0])) {
            case 0x00:
            case 0xFE:
                if ($packet->getLength() < 9) {
                    $affected = $packet->readLengthEncodedInt();
                    $insertId = $packet->readLengthEncodedInt();
                    
                    return $defer->resolve(new ResultSet($affected, $insertId));
                }
                
                break;
            case 0xFF:
                return $this->throwError($packet);
        }
        
        $columnCount = $packet->readLengthEncodedInt();
        $defs = [];
        
        for ($i = 0; $i < $columnCount; $i++) {
            $defs[] = $this->parseColumnDefinition($client, yield from $client->readRawPacket());
        }
        
        $names = \array_map(function (array $def) {
            return $def['columnAlias'];
        }, $defs);
        
        if ($columnCount && !$client->isEofDeprecated()) {
            yield from $client->readPacket(0xFE);
        }
        
        $defer->resolve(new ResultSet(0, 0, $channel = new Channel($prefetch)));
        
        try {
            while (true) {
                $packet = yield from $client->readPacket(0x00, 0xFE);
                
                switch ($packet->type) {
                    case 0xFE:
                        break 2;
                }
                
                yield $channel->send($this->parseRow($client, $packet, $columnCount, $names, $defs));
            }
            
            $channel->close();
        } catch (\Throwable $e) {
            $channel->close($e);
        }
    }

    protected function encodeParams(): string
    {
        $types = new PacketBuilder();
        $values = '';
        
        // Append NULL-bitmap with all bits set to 0:
        $mask = \str_repeat("\0", (\count($this->params) + 7) >> 3);
        
        for ($count = \count($this->paramDefinitions), $i = 0; $i < $count; $i++) {
            if (!\array_key_exists($i, $this->params)) {
                throw new \RuntimeException(\sprintf('Param %u is not bound'));
            }
            
            $val = $this->params[$i];
            
            if ($val === null) {
                // Set NULL bit at param position to 1:
                $off = ($i >> 3);
                $mask[$off] |= \chr(1 << ($i % 8));
            } else {
                $bound = true;
            }
            
            list ($unsigned, $type, $val) = $this->encodeValue($val, $this->paramDefinitions[$i]);
            
            $types->writeInt8($type);
            $types->writeInt8($unsigned ? 0x80 : 0x00);
            
            $values .= $val;
        }
        
        $builder = new PacketBuilder();
        $builder->write($mask);
        $builder->writeInt8((int) $bound);
        
        if ($bound) {
            $builder->write($types->build());
            $builder->write($values);
        }
        
        return $builder->build();
    }

    protected function encodeValue($val, array $def): array
    {
        // TODO: Convert value into the correct param type...
        
        $builder = new PacketBuilder();
        $unsigned = false;
        
        switch (\gettype($val)) {
            case 'boolean':
                $type = Client::MYSQL_TYPE_TINY;
                $builder->write($val ? "\x01" : "\x00");
                break;
            case 'integer':
                if ($val >= 0) {
                    $unsigned = true;
                }
                
                if ($val >= 0 && $val < (1 << 15)) {
                    $type = Client::MYSQL_TYPE_SHORT;
                    $builder->writeInt16($val);
                } else {
                    $type = Client::MYSQL_TYPE_LONGLONG;
                    $builder->writeInt64($val);
                }
                break;
            case 'double':
                $type = Client::MYSQL_TYPE_DOUBLE;
                $value = \pack('d', $val);
                
                if ($this->isLittleEndian()) {
                    $value = \strrev($value);
                }
                
                $builder->write($value);
                break;
            case 'string':
                $type = Client::MYSQL_TYPE_LONG_BLOB;
                $builder->writeLengthEncodedString($val);
                break;
            case 'NULL':
                $type = Client::MYSQL_TYPE_NULL;
                break;
            default:
                throw new ProtocolError("Unexpected type for binding parameter: " . \gettype($val));
        }
        
        return [
            $unsigned,
            $type,
            $builder->build()
        ];
    }

    protected function parseRow(Client $client, Packet $packet, int $columnCount, array $columnNames, array $defs): array
    {
        $row = $packet->readNullBitmap($columnCount);
        $i = 0;
        
        while (!$packet->isConsumed()) {
            while (\array_key_exists($i, $row)) {
                $i++;
            }
            
            $row[$i] = $packet->readValue($defs[$i]['type']);
        }
        
        \ksort($row, \SORT_NUMERIC);
        
        return \array_combine($columnNames, $row);
    }
}
