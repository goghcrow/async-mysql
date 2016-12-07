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

use KoolKode\Async\Test\AsyncTestCase;

class SetupTest extends AsyncTestCase
{
    public function testBasicConnection()
    {
        $dsn = $this->getEnvParam('DB_DSN');
        $username = $this->getEnvParam('DB_USERNAME', '');
        $password = $this->getEnvParam('DB_PASSWORD', '');
        
        $pdo = new \PDO($dsn, $username, $password);
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        
        $ddl = file_get_contents(__DIR__ . '/test.sql');
        
        foreach (array_map('trim', explode(';', $ddl)) as $cmd) {
            if ($cmd === '') {
                continue;
            }
            
            $pdo->exec($cmd);
        }
        
        $pdo->exec("INSERT INTO customer (name) VALUES ('KoolKode'), ('Async'), ('MySQL'), ('Git')");
        
        $conn = yield Connection::connect($dsn, $username, $password);
        
        print_r($conn);
    }
}
