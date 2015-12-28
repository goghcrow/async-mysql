<?php

namespace KoolKode\Async\MySQL;

use function KoolKode\Async\noop;

class Pool implements ConnectionInterface
{
    protected $dsn;
    
    protected $username;
    
    protected $password;
    
    protected $conns = [];
    
    protected $available = [];
    
    public function __construct(string $dsn, string $username, string $password)
    {
        $this->dsn = $dsn;
        $this->username = $username;
        $this->password = $password;
    }
    
    public function __debugInfo(): array
    {
        return [
            'dsn' => $this->dsn,
            'connections' => count($this->conns),
            'available' => count($this->available)
        ];
    }
    
    public function close(): \Generator
    {
        yield noop();
    }
    
    public function releaseConnection(Connection $conn)
    {
        $this->available[] = $conn;
    }
    
    public function prepare(string $sql): \Generator
    {
        if (empty($this->available)) {
            $conn = $this->conns[] = yield from Connection::connect($this->dsn, $this->username, $this->password);
        } else {
            $conn = array_pop($this->available);
        }
        
        $conn->setPool($this);
        
        return yield from $conn->prepare($sql);
    }
    
    public function getClient(): \Generator
    {
        if (empty($this->available)) {
            $conn = $this->conns[] = yield from Connection::connect($this->dsn, $this->username, $this->password);
        } else {
            $conn = array_pop($this->available);
        }
        
        $conn->setPool($this);
        
        return yield from $conn->getClient();
    }
}
