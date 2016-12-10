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

use KoolKode\Async\Awaitable;
use KoolKode\Async\Failure;
use KoolKode\Async\Success;
use Psr\Log\LoggerInterface;

/**
 * MySQL DB connection that can be used to execute SQL queries.
 * 
 * @author Martin Schröder
 */
class Connection
{
    /**
     * Client object being used to communicate with the DB server.
     * 
     * @var Client
     */
    protected $client;
    
    protected $disposed = false;
    
    /**
     * PSR logger instance.
     * 
     * @var LoggerInterface
     */
    protected $logger;
    
    /**
     * Create a new MySQL connection using the given DB client.
     * 
     * @param Client $client
     */
    public function __construct(Client $client, LoggerInterface $logger = null)
    {
        $this->client = $client;
        $this->logger = $logger;
    }
    
    /**
     * Shut the DB connection down.
     * 
     * @param \Throwable $e Optional cause of shutdown.
     */
    public function shutdown(\Throwable $e = null): Awaitable
    {
        if (!$this->disposed) {
            $this->disposed = true;
            
            return $this->client->shutdown($e);
        }
        
        return new Success(null);
    }

    /**
     * Ping the DB server.
     * 
     * @return int Number of milliseconds needed to send ping packets back and forth.
     */
    public function ping(): Awaitable
    {
        if ($this->disposed) {
            return new Failure(new \RuntimeException('Cannot ping a disposed connection'));
        }
        
        return $this->client->sendCommand(function (Client $client) {
            $builder = new PacketBuilder();
            $builder->writeInt8(0x0E);
            
            $time = \microtime(true) * 1000;
            
            yield from $client->sendPacket($builder->build());
            yield from $client->readPacket(0x00);
            
            return (int) \ceil((\microtime(true) * 1000 - $time) + .5);
        });
    }
    
    /**
     * Create a prepared statement from the given SQL.
     * 
     * @param string $sql
     * @return Statement
     */
    public function prepare(string $sql): Statement
    {
        if ($this->disposed) {
            return new Failure(new \RuntimeException('Cannot prepare a statement using a disposed connection'));
        }
        
        return new Statement($sql, $this->client, $this->logger);
    }
}
