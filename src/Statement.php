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
        
        yield from $client->sendPacket($client->encodeInt8(0x16) . $sql);
        
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
        
        // TODO: Params...
        

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
        $packet = $client->encodeInt8(0x17);
        $packet .= $client->encodeInt32($this->id);
        $packet .= $client->encodeInt8(self::CURSOR_TYPE_NO_CURSOR);
        $packet .= $client->encodeInt32(1);
        
        yield from $client->sendPacket($packet);
        
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
