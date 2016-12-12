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
use KoolKode\Async\Coroutine;
use KoolKode\Async\Database\ResultSet;
use KoolKode\Async\Success;
use KoolKode\Async\Transform;
use KoolKode\Async\Util\Channel;

/**
 * Provides access to the result of an SQL query.
 * 
 * @author Martin Schröder
 */
class MySqlResultSet implements ResultSet
{
    /**
     * Number of rows affected by the query.
     * 
     * @var int
     */
    protected $affectedRows;

    /**
     * ID of the last inserted item (if the table contained an auto-increment field).
     * 
     * @var int
     */
    protected $lastInsertId;
    
    /**
     * Channel that provides result rows returned by a SELECT query.
     * 
     * This field will be NULL after the cursor has been closed!
     * 
     * @var Channel
     */
    protected $channel;

    public function __construct(int $affectedRows, int $lastInsertId, Channel $channel = null)
    {
        $this->affectedRows = $affectedRows;
        $this->lastInsertId = $lastInsertId;
        $this->channel = $channel;
    }

    /**
     * Ensure the cursor is closed when the result is not needed anymore.
     */
    public function __destruct()
    {
        $this->closeCursor();
    }

    /**
     * Get the number of rows affected by the query (will allways be 0 for SELECT queries).
     * 
     * @return int
     */
    public function affectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * Get the last inserted ID.
     * 
     * @return int
     */
    public function lastInsertId(): int
    {
        return $this->lastInsertId;
    }

    /**
     * Access all result rows using the underlying channel.
     * 
     * @return Channel
     */
    public function channel(): Channel
    {
        return $this->channel ?? Channel::fromArray([]);
    }

    /**
     * Close the cursor of this result.
     * 
     * Allways call this method to free the DB connection if you do not fetch all rows from the result.
     */
    public function closeCursor()
    {
        if ($this->channel !== null) {
            try {
                if (!$this->channel->isClosed()) {
                    $channel = $this->channel;
                    $channel->close();
                    
                    new Coroutine(function () use ($channel) {
                        while (null !== yield $channel->receive());
                    });
                }
            } finally {
                $this->channel = null;
            }
        }
    }

    /**
     * Fetch the next row from the DB.
     * 
     * @return array or NULL when no more rows are available.
     */
    public function fetch(): Awaitable
    {
        if ($this->channel === null) {
            return new Success(null);
        }
        
        return $this->channel->receive();
    }

    /**
     * Fetch all rows from the DB into an array.
     * 
     * @return array
     */
    public function fetchArray(): Awaitable
    {
        if ($this->channel === null) {
            return new Success([]);
        }
        
        return new Coroutine(function () {
            $result = [];
            
            try {
                while (null !== ($row = yield $this->channel->receive())) {
                    $result[] = $row;
                }
            } finally {
                $this->closeCursor();
            }
            
            return $result;
        });
    }
    
    /**
     * Fetch the next value of the given column.
     * 
     * @param string $alias
     * @return mixed
     */
    public function fetchColumn(string $alias): Awaitable
    {
        if ($this->channel === null) {
            return new Success(null);
        }
        
        return new Transform($this->channel->receive(), function (array $row = null) use ($alias) {
            return ($row === null) ? null : $row[$alias];
        });
    }

    /**
     * Fetch all values for the given column into an array.
     * 
     * @param string $alias
     * @return array
     */
    public function fetchColumnArray(string $alias): Awaitable
    {
        if ($this->channel === null) {
            return new Success([]);
        }
        
        return new Coroutine(function () use ($alias) {
            $result = [];
            
            try {
                while (null !== ($row = yield $this->channel->receive())) {
                    $result[] = $row[$alias];
                }
            } finally {
                $this->closeCursor();
            }
            
            return $result;
        });
    }
}
