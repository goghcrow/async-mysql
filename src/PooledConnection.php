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

use Interop\Async\Promise;
use KoolKode\Async\Success;
use Psr\Log\LoggerInterface;

/**
 * Connection backed by a pooled MySQL client.
 * 
 * @author Martin Schröder
 */
class PooledConnection extends MySqlConnection
{
    protected $disposer;
    
    public function __construct(Client $client, callable $disposer, LoggerInterface $logger = null)
    {
        parent::__construct($client, $logger);
        
        $this->disposer = $disposer;
    }

    public function shutdown(\Throwable $e = null): Promise
    {
        if (!$this->disposed) {
            $this->disposed = true;
            
            ($this->disposer)($this->client);
        }
        
        return new Success(null);
    }
}
