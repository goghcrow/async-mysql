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

class PooledStatement extends Statement
{
    protected $connector;
    
    protected $disposer;
    
    public function __construct(string $sql, callable $connector, callable $disposer, LoggerInterface $logger = null)
    {
        $this->sql = $sql;
        $this->connector = $connector;
        $this->disposer = $disposer;
        $this->logger = $logger;
    }
    
    public function dispose(): Awaitable
    {
        if ($this->disposed) {
            return parent::dispose();
        }
        
        $promise = parent::dispose();
        
        if ($this->client) {
            $promise->when(function ($e) {
                ($this->disposer)($this->client, $e);
            });
        }
        
        return $promise;
    }

    public function execute(): Awaitable
    {
        return new Coroutine(function () {
            if ($this->client === null) {
                $this->client = yield ($this->connector)();
            }
            
            return yield parent::execute();
        });
    }
}
