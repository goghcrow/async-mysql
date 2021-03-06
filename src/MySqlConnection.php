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

use AsyncInterop\Promise;
use KoolKode\Async\Database\Connection;
use KoolKode\Async\Database\Statement;
use KoolKode\Async\Coroutine;
use KoolKode\Async\Failure;
use KoolKode\Async\Success;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;

/**
 * MySQL DB connection that can be used to execute SQL queries.
 * 
 * @author Martin Schröder
 */
class MySqlConnection implements Connection, LoggerAwareInterface
{
    use LoggerAwareTrait;
    
    /**
     * Client object being used to communicate with the DB server.
     * 
     * @var Client
     */
    protected $client;
    
    protected $prefix;
    
    protected $disposed = false;
    
    /**
     * Create a new MySQL connection using the given DB client.
     * 
     * @param Client $client
     */
    public function __construct(Client $client, string $prefix = '')
    {
        $this->client = $client;
        $this->prefix = $prefix;
        
        $this->logger = new Logger(static::class);
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown(\Throwable $e = null): Promise
    {
        if (!$this->disposed) {
            $this->disposed = true;
            
            return $this->client->shutdown($e);
        }
        
        return new Success(null);
    }
    
    public function applySchemaObjectPrefix(string $name): string
    {
        return \str_replace('#__', $this->prefix, $name);
    }
    
    /**
     * {@inheritdoc}
     */
    public function quoteIdentifier(string $identifier): string
    {
        return '`' . \str_replace('`', '``', $identifier) . '`';
    }
    
    /**
     * {@inheritdoc}
     */
    public function insert(string $table, array $values): Promise
    {
        return new Coroutine(function () use ($table, $values) {
            $sql = 'INSERT INTO ' . $this->quoteIdentifier($this->applySchemaObjectPrefix($table)) . ' (';
            $params = [];
            
            foreach ($values as $k => $v) {
                if ($params) {
                    $sql . ', ';
                }
                
                $sql .= $this->quoteIdentifier($k);
                $params[] = $v;
            }
            
            $sql .= ') VALUES (' . \implode(', ', \array_fill(0, \count($params), '?')) . ')';
            
            $stmt = $this->prepare($sql);
            
            try {
                return (yield $stmt->bindAll($params)->execute())->lastInsertId();
            } finally {
                $stmt->dispose();
            }
        });
    }
    
    /**
     * {@inheritdoc}
     */
    public function update(string $table, array $identity, array $values): Promise
    {
        return new Coroutine(function () use ($table, $identity, $values) {
            $sql = 'UPDATE ' . $this->quoteIdentifier($this->applySchemaObjectPrefix($table)) . ' SET ';
            $params = [];
            
            foreach ($values as $k => $v) {
                if ($params) {
                    $sql .= ', ';
                }
                
                $sql .= $this->quoteIdentifier($k) . ' = ?';
                $params[] = $v;
            }
            
            $sql .= ' WHERE ';
            $i = 0;
            
            foreach ($identity as $k => $v) {
                if ($i++) {
                    $sql .= ', ';
                }
                
                $sql .= $this->quoteIdentifier($k) . ' = ?';
                $params[] = $v;
            }
            
            $stmt = $this->prepare($sql);
            
            try {
                return (yield $stmt->bindAll($params)->execute())->affectedRows();
            } finally {
                $stmt->dispose();
            }
        });
    }
    
    /**
     * {@inheritdoc}
     */
    public function delete(string $table, array $identity): Promise
    {
        return new Coroutine(function () use ($table, $identity) {
            $sql = 'DELETE FROM ' . $this->quoteIdentifier($this->applySchemaObjectPrefix($table)) . ' WHERE ';
            $params = [];
            
            foreach ($identity as $k => $v) {
                if ($params) {
                    $sql .= ', ';
                }
                
                $sql .= $this->quoteIdentifier($k) . ' = ?';
                $params[] = $v;
            }
            
            $stmt = $this->prepare($sql);
            
            try {
                return (yield $stmt->bindAll($params)->execute())->affectedRows();
            } finally {
                $stmt->dispose();
            }
        });
    }

    /**
     * Ping the DB server.
     * 
     * @return int Number of milliseconds needed to send ping packets back and forth.
     */
    public function ping(): Promise
    {
        if ($this->disposed) {
            return new Failure(new \RuntimeException('Cannot ping a disposed connection'));
        }
        
        return $this->client->sendCommand(function (Client $client) {
            $builder = new PacketBuilder();
            $builder->writeInt8(0x0E);
            
            $time = \microtime(true) * 1000;
            
            try {
                yield from $client->sendPacket($builder->build());
                yield from $client->readPacket(0x00);
            } catch (\Throwable $e) {
                $this->shutdown($e);
                
                throw $e;
            }
            
            return (int) \ceil((\microtime(true) * 1000 - $time) + .5);
        });
    }

    /**
     * {@inheritdoc}
     */
    public function prepare(string $sql): Statement
    {
        if ($this->disposed) {
            return new Failure(new \RuntimeException('Cannot prepare a statement using a disposed connection'));
        }
        
        $sql = \preg_replace_callback("'`#__([^`]+)`'", function (array $m) {
            return '`' . \str_replace('`', '``', $this->prefix . $m[1]) . '`';
        }, $sql);
        
        return new MySqlStatement($sql, $this->client, $this->prefix);
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction(bool $readOnly = false): Promise
    {
        return $this->client->beginTransaction($readOnly);
    }

    /**
     * {@inheritdoc}
     */
    public function commit(): Promise
    {
        return $this->client->commit();
    }

    /**
     * {@inheritdoc}
     */
    public function rollBack(): Promise
    {
        return $this->client->rollBack();
    }
}
