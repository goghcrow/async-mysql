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

use KoolKode\Async\Event\EventEmitter;

class Pool implements ConnectionInterface
{
    protected $dsn;
    
    protected $username;
    
    protected $password;
    
    protected $size;
    
    protected $events;
    
    protected $conns = [];
    
    protected $available = [];
    
    public function __construct(EventEmitter $events, string $dsn, string $username, string $password, int $size = 1)
    {
        $this->events = $events;
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
        $this->size = $size;
    }
    
    public function __debugInfo(): array
    {
        return [
            'dsn' => $this->dsn,
            'size' => $this->size,
            'connections' => count($this->conns),
            'available' => count($this->available)
        ];
    }
    
    public function close(): \Generator
    {
        yield;
    }
    
    public function releaseConnection(Connection $conn)
    {
        $this->available[] = $conn;
        
        $this->events->emit(new ConnectionReleasedEvent($conn));
    }
    
    public function prepare(string $sql): \Generator
    {
        $conn = yield from $this->aquireConnection();
        $stmt = yield from $conn->prepare($sql);
        
        return $stmt;
    }
    
    public function getClient(): \Generator
    {
        $conn = yield from $this->aquireConnection();
        $client = yield from $conn->getClient();
        
        return $client;
    }
    
    protected function aquireConnection(): \Generator
    {
        if (empty($this->available) && count($this->conns) < $this->size) {
            for ($id = 0; $id < $this->size; $id++) {
                if (!array_key_exists($id, $this->conns)) {
                    break;
                }
            }
            
            $this->conns[$id] = NULL;
            
            try {
                $conn = $this->conns[$id] = yield from Connection::connect($this->dsn, $this->username, $this->password);
            } catch (\Throwable $e) {
                unset($this->conns[$id]);
            }
        } else {
            while (empty($this->available)) {
                yield from $this->events->await(ConnectionReleasedEvent::class);
            }
            
            $conn = array_pop($this->available);
        }
        
        $conn->setPool($this);
        
        return $conn;
    }
}
