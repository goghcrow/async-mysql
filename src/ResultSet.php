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
use KoolKode\Async\Success;
use KoolKode\Async\Util\Channel;

class ResultSet
{
    protected $affectedRows;

    protected $lastInsertId;
    
    protected $channel;

    public function __construct($affectedRows, $lastInsertId, Channel $channel = null)
    {
        $this->affectedRows = $affectedRows;
        $this->lastInsertId = $lastInsertId;
        $this->channel = $channel;
    }

    public function __destruct()
    {
        $this->closeCursor();
    }

    public function affectedRows()
    {
        return $this->affectedRows;
    }

    public function lastInsertId()
    {
        return $this->lastInsertId;
    }

    public function channel(): Channel
    {
        return $this->channel ?? Channel::fromArray([]);
    }

    public function closeCursor()
    {
        if ($this->channel !== null) {
            try {
                if (!$this->channel->isClosed()) {
                    $channel = $this->channel;
                    
                    new Coroutine(function () use ($channel) {
                        while (null !== yield $channel->receive());
                    });
                }
            } finally {
                $this->channel = null;
            }
        }
    }

    public function fetch(): Awaitable
    {
        if ($this->channel === null) {
            return new Success(null);
        }
        
        return $this->channel->receive();
    }

    public function fetchAll(): Awaitable
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

    public function fetchColumn(string $alias): Awaitable
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
