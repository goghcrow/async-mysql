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

use KoolKode\Async\Event\Event;

class ConnectionReleasedEvent extends Event
{
    public $conn;

    public function __construct(ConnectionInterface $conn)
    {
        $this->conn = $conn;
    }
}
