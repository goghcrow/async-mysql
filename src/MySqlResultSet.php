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
     * {@inheritdoc}
     */
    public function affectedRows(): int
    {
        return $this->affectedRows;
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId(): int
    {
        return $this->lastInsertId;
    }
    
    /**
     * {@inheritdoc}
     */
    public function closeCursor(): Promise
    {
        if ($this->channel !== null) {
            try {
                if (!$this->channel->isClosed()) {
                    $channel = $this->channel;
                    $channel->close();
                    
                    return new Coroutine(function () use ($channel) {
                        while (null !== yield $channel->receive());
                    });
                }
            } finally {
                $this->channel = null;
            }
        }
        
        return new Success(null);
    }

    /**
     * {@inheritdoc}
     */
    public function fetch(): Promise
    {
        if ($this->channel === null) {
            return new Success(null);
        }
        
        return $this->channel->receive();
    }

    /**
     * {@inheritdoc}
     */
    public function fetchArray(): Promise
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
     * {@inheritdoc}
     */
    public function fetchColumn(string $alias, bool & $eof): Promise
    {
        if ($this->channel === null) {
            $eof = true;
            
            return new Success(null);
        }
        
        return new Transform($this->channel->receive(), function (array $row = null) use ($alias, & $eof) {
            if ($row === null) {
                $eof = true;
            } else {
                return $row[$alias];
            }
        });
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumnArray(string $alias): Promise
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
