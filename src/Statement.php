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

class Statement
{
    const CURSOR_TYPE_NO_CURSOR = 0x00;

    const CURSOR_TYPE_READ_ONLY = 0x01;

    const CURSOR_TYPE_FOR_UPDATE = 0x02;

    const CURSOR_TYPE_SCROLLABLE = 0x04;

    protected $sql;

    protected $client;

    protected $logger;

    protected $id;
    
    protected $recompile = false;
    
    protected $limit = 0;
    
    protected $offset = 0;

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

    public function dispose(): Awaitable
    {
        if ($this->id) {
            try {
                $id = $this->id;
                
                return $this->client->sendCommand(function (Client $client) use ($id) {
                    $packet = $client->encodeInt8(0x19);
                    $packet .= $client->encodeInt32($id);
                    
                    yield from $client->sendPacket($packet);
                });
            } finally {
                $this->id = null;
            }
        }
        
        return new Success(null);
    }

    public function limit(int $limit): Statement
    {
        $this->limit = $limit;
        
        return $this;
    }
    
    public function offset(int $offset): Statement
    {
        $this->offset = $offset;
        
        return $this;
    }
    
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

    protected function throwError(Client $clinet, string $packet)
    {
        $offset = 0;
        
        $code = $client->readInt16($packet, $offset);
        $marker = $client->readFixedLengthString($packet, 1, $offset);
        $state = $client->readFixedLengthString($packet, 5, $offset);
        $message = \substr($packet, $offset);
        
        throw new \RuntimeException(\sprintf('Failed to prepare SQL query: SQLSTATE [%s]: "%s"', $state, $message), $code);
    }

    protected function prepareQuery(Client $client): \Generator
    {
        $sql = $this->sql;
        
        yield from $client->sendPacket($client->encodeInt8(0x16) . $sql);
        
        list ($type, $packet) = yield from $client->readPacket(0x00, 0xFF);
        
        if ($type === 0xFF) {
            return $this->throwError($clinet, $packet);
        }
        
        $offset = 0;
        
        $this->id = $client->readInt32($packet, $offset);
        $columnCount = $client->readInt16($packet, $offset);
        $paramCount = $client->readInt16($packet, $offset);
        
        // Discard filler:
        $client->discardByte($packet, $offset, 0x00);
        
        $warningCount = $client->readInt16($packet, $offset);
        
        // TODO: Params...
        

        for ($i = 0; $i < $columnCount; $i++) {
            $this->parseColumnDefinition($client, yield from $client->readRawPacket());
        }
        
        if ($columnCount && !$client->isEofDeprecated()) {
            yield from $client->readPacket(0xFE);
        }
    }

    protected function parseColumnDefinition(Client $client, string $packet): array
    {
        $offset = 0;
        
        $col = [
            'catalog' => $client->readLengthEncodedString($packet, $offset),
            'schema' => $client->readLengthEncodedString($packet, $offset),
            'tableAlias' => $client->readLengthEncodedString($packet, $offset),
            'table' => $client->readLengthEncodedString($packet, $offset),
            'columnAlias' => $client->readLengthEncodedString($packet, $offset),
            'column' => $client->readLengthEncodedString($packet, $offset)
        ];
        
        if (0x0C !== $client->readLengthEncodedInt($packet, $offset)) {
            throw new \RuntimeException('Invalid length of colum description fields');
        }
        
        $col['charset'] = $client->readInt16($packet, $offset);
        $col['length'] = $client->readInt32($packet, $offset);
        $col['type'] = $client->readInt8($packet, $offset);
        $col['flags'] = $client->readInt16($packet, $offset);
        $col['decimals'] = $client->readInt8($packet, $offset);
        
        $client->discardByte($packet, $offset, 0x00);
        $client->discardByte($packet, $offset, 0x00);
        
        return $col;
    }

    protected function executeQuery(Client $client, Deferred $defer, int $prefetch = 4): \Generator
    {
        $packet = $client->encodeInt8(0x17);
        $packet .= $client->encodeInt32($this->id);
        $packet .= $client->encodeInt8(self::CURSOR_TYPE_NO_CURSOR);
        $packet .= $client->encodeInt32(1);
        
        yield from $client->sendPacket($packet);
        
        $packet = yield from $client->readRawPacket();
        $offset = 0;
        
        switch (\ord($packet[0])) {
            case 0x00:
            case 0xFE:
                if (\strlen($packet) < 9) {
                    $affected = $client->readLengthEncodedInt($packet, $offset);
                    $insertId = $client->readLengthEncodedInt($packet, $offset);
                    
                    return $defer->resolve(new ResultSet($affected, $insertId));
                }
                
                break;
            case 0xFF:
                return $this->throwError($clinet, $packet);
        }
        
        $columnCount = $client->readLengthEncodedInt($packet, $offset);
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
                list ($type, $packet) = yield from $client->readPacket(0x00, 0xFE);
                
                switch ($type) {
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

    protected function parseRow(Client $client, string $packet, int $columnCount, array $columnNames, array $defs): array
    {
        $row = [];
        $offset = 0;
        
        for ($i = 0; $i < $columnCount; $i++) {
            if (\ord($packet[$offset + (($i + 2) >> 3)]) & (1 << (($i + 2) % 8))) {
                $row[$i] = null;
            }
        }
        
        $offset += ($columnCount + 9) >> 3;
        
        for ($i = 0; $offset < \strlen($packet); $i++) {
            while (\array_key_exists($i, $row)) {
                $i++;
            }
            
            $row[$i] = $client->readBinary($defs[$i]['type'], $packet, $offset);
        }
        
        \ksort($row, \SORT_NUMERIC);
        
        return \array_combine($columnNames, $row);
    }
}
