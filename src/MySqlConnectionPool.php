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
use KoolKode\Async\AwaitPending;
use KoolKode\Async\Coroutine;
use KoolKode\Async\Database\Connection;
use KoolKode\Async\Database\ConnectionPool;
use KoolKode\Async\Database\Statement;
use KoolKode\Async\Deferred;
use KoolKode\Async\Failure;
use KoolKode\Async\MultiReasonException;
use KoolKode\Async\Success;
use KoolKode\Async\Transform;
use KoolKode\Async\Util\Channel;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;

/**
 * MySQL connection pool.
 * 
 * @author Martin Schröder
 */
class MySqlConnectionPool implements ConnectionPool, LoggerAwareInterface
{
    use LoggerAwareTrait;

    /**
     * Maximum number of pooled connections.
     * 
     * @var int
     */
    protected $size;

    /**
     * Number of currently pooled connections.
     * 
     * @var int
     */
    protected $active = 0;

    /**
     * Has the pool been shut down yet?
     * 
     * @var bool
     */
    protected $disposed = false;

    /**
     * Holds all available MySQL connections.
     * 
     * @var Channel
     */
    protected $clients;

    /**
     * MySQL connection factory.
     *
     * @var ConnectionFactory
     */
    protected $factory;

    /**
     * Create a new MySQL connection pool.
     * 
     * @param ConnectionFactory $factory
     * @param int $size Maximum pool size.
     */
    public function __construct(ConnectionFactory $factory, int $size = 10)
    {
        $this->factory = $factory;
        $this->size = $size;
        
        $this->clients = new Channel($this->size);
    }

    public function __debugInfo(): array
    {
        return [
            'size' => $this->size,
            'active' => $this->active,
            'idle' => $this->clients->count(),
            'disposed' => $this->disposed
        ];
    }

    public function shutdown(\Throwable $e = null): Awaitable
    {
        if ($this->disposed) {
            return new Success(null);
        }
        
        if ($e === null) {
            $e = new \RuntimeException('Connection pool shut down');
        }
        
        $this->disposed = true;
        $this->clients->close($e);
        
        return new Coroutine(function () use ($e) {
            $promises = [];
            
            while (null !== ($client = yield $this->clients->receive())) {
                $promises[] = $client->shutdown($e);
            }
            
            yield new AwaitPending($promises);
        });
    }

    public function initialize(int $size): Awaitable
    {
        $defer = new Deferred();
        $promises = [];
        
        for ($size = \min($size, $this->size), $i = $this->active; $i < $size; $i++) {
            $this->active++;
            
            $promise = $promises[] = $this->factory->connectClient();
            
            $promise->when(function (\Throwable $e = null, Client $client = null) {
                if ($client && !$client->isDisposed()) {
                    $this->clients->send($client);
                } else {
                    $this->active--;
                    
                    if ($this->logger) {
                        $this->logger->error('Failed to create pooled MySQL connection', [
                            'exception' => $e
                        ]);
                    }
                    
                    $client->shutdown($e);
                }
            });
        }
        
        $await = new AwaitPending($promises);
        
        $await->when(function ($e, array $result) use ($defer) {
            if (empty($result[1])) {
                $defer->resolve(null);
            } else {
                $defer->fail(new MultiReasonException($result[1], 'Failed to MySQL pool'));
            }
        });
        
        return $defer;
    }

    public function checkout(): Awaitable
    {
        if ($this->disposed) {
            return new Failure(new \RuntimeException('Cannot checkout connection from disposed pool'));
        }
        
        if ($this->active < $this->size && !$this->clients->count()) {
            $this->active++;
            
            return new Transform($this->factory->connectClient(), function (Client $client) {
                return $this->createConnection($client);
            });
        }
        
        return new Transform($this->clients->receive(), function (Client $client) {
            return $this->createConnection($client);
        });
    }

    protected function createConnection(Client $client): PooledConnection
    {
        return new PooledConnection($client, function (Client $client) {
            if ($this->disposed) {
                $this->active--;
                $client->shutdown();
            } elseif ($client->isDisposed()) {
                $this->active--;
            } elseif ($client->isWithinTransaction()) {
                $this->handleUnterminatedTransaction($client);
            } else {
                $this->clients->send($client);
            }
        }, $this->logger);
    }

    /**
     * Create a prepared statement from the given SQL.
     * 
     * @param string $sql
     * @return PooledStatement
     */
    public function prepare(string $sql): Statement
    {
        if ($this->disposed) {
            throw new \RuntimeException('Cannot prepare statement using a disposed connection pool');
        }
        
        return new PooledStatement($sql, function () {
            if ($this->disposed) {
                return new Failure(new \RuntimeException('Cannot execute statement due to disposed connection pool'));
            }
            
            if ($this->active < $this->size && !$this->clients->count()) {
                $this->active++;
                
                return $this->factory->connectClient();
            }
            
            return $this->clients->receive();
        }, function (Client $client, \Throwable $e = null) {
            if ($e || $this->disposed) {
                $this->active--;
                $client->shutdown($e);
            } elseif ($client->isDisposed()) {
                $this->active--;
            } elseif ($client->isWithinTransaction()) {
                $this->handleUnterminatedTransaction($client);
            } else {
                $this->clients->send($client);
            }
        }, $this->logger);
    }

    protected function handleUnterminatedTransaction(Client $client)
    {
        $client->sendCommand(function () {})->when(function ($e) use ($client) {
            if ($e || $client->isWithinTransaction()) {
                $this->active--;
                $client->shutdown();
            } else {
                $this->clients->send($client);
            }
        });
    }
}
