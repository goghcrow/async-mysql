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

/**
 * Contract for a MySQL connection.
 * 
 * @author Martin Schröder
 */
interface ConnectionInterface
{
    /**
     * Prepare the given SQL statement.
     * 
     * @param string $sql
     * @return Statement
     */
    public function prepare(string $sql): \Generator;
}
