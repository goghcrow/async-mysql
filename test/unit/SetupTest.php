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
        
        $factory = new ConnectionFactory();
        
        $conn = yield $factory->connect($dsn, $username, $password);
        $this->assertInstanceOf(Connection::class, $conn);
        
        try {
            $this->assertGreaterThan(0, yield $conn->ping());
            
            $stmt = $conn->prepare("SELECT * FROM customer WHERE id IN (?, ?) ORDER BY id");
            $this->assertInstanceOf(Statement::class, $stmt);
            
            $stmt->bind(0, 2);
            $stmt->bind(1, 1);
            
            $result = yield $stmt->execute();
            $this->assertInstanceOf(ResultSet::class, $result);
            
            $rows1 = [];
            
            try {
                while ($row = yield $result->fetch()) {
                    $rows1[] = $row;
                }
            } finally {
                $result->closeCursor();
            }
            
            $rows2 = yield (yield $stmt->execute())->fetchAll();
            $this->assertEquals($rows1, $rows2);
            
            $stmt->bind(1, 3);
            $stmt->limit(2);
            
            $result = yield $stmt->execute();
            $this->assertEquals('Async MySQL', implode(' ', yield $result->fetchColumn('name')));
        } finally {
            $conn->shutdown();
        }
    }
}
