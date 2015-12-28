<?php

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
