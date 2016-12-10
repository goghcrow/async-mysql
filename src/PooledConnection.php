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
use Psr\Log\LoggerInterface;

class PooledConnection extends Connection
{
    protected $disposer;
    
    public function __construct(Client $client, callable $disposer, LoggerInterface $logger = null)
    {
        parent::__construct($client, $logger);
        
        $this->disposer = $disposer;
    }

    public function shutdown(\Throwable $e = null): Awaitable
    {
        if ($this->disposed) {
            return parent::shutdown($e);
        }
        
        $promise = parent::shutdown($e);
        
        $promise->when(function () {
            ($this->disposer)($this->client);
        });
        
        return $promise;
    }
}
