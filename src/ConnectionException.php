<?php

/*
 * This file is part of KoolKode Async MySQL.
 *
 * (c) Martin Schröder <m.schroeder2007@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace KoolKode\Async\MySQL;

class ConnectionException extends \RuntimeException
{
    protected $sqlState;
    
    public function __construct(string $message, int $code = 0, string $sqlState = '', \Throwable $cause = NULL)
    {
        parent::__construct($message, $code, $cause);
        
        $this->sqlState = trim($sqlState);
    }
    
    public function hasSqlState(): bool
    {
        return $this->sqlState !== '';
    }
    
    public function getSqlState(): string
    {
        return $this->sqlState;
    }
}
