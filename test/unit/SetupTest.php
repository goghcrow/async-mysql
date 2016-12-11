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
        
        $factory = new ConnectionFactory($dsn, $username, $password);
        
        $pool = new ConnectionPool($factory);
        
//         $conn = yield $factory->connect();
//         $this->assertInstanceOf(Connection::class, $conn);
        
        try {
//             $this->assertGreaterThan(0, yield $conn->ping());
            
            $stmt = $pool->prepare("SELECT * FROM customer WHERE id > ? ORDER BY id");
            $this->assertInstanceOf(Statement::class, $stmt);
            
            try {
                $stmt->bind(0, 1);
                
                $result = yield $stmt->execute();
                $this->assertInstanceOf(ResultSet::class, $result);
                
                $rows1 = [];
                
                try {
                    $rows1[] = yield $result->fetch();
                    $rows1[] = yield $result->fetch();
                } finally {
                    $result->closeCursor();
                }
                
                $rows2 = yield (yield $stmt->limit(2)->execute())->fetchArray();
                $this->assertEquals($rows1, $rows2);
            } finally {
                $stmt->dispose();
            }
            
            $conn = yield $pool->checkout();
            
            try {
                $this->assertEquals(5, yield $conn->insert('customer', [
                    'name' => 'Hub'
                ]));
            } finally {
                $conn->shutdown();
            }
            
            $stmt = $pool->prepare("SELECT name FROM customer ORDER BY id");
            
            try {
                $result = yield $stmt->limit(2)->offset(1)->execute();
                $this->assertEquals('Async MySQL', implode(' ', yield $result->fetchColumnArray('name')));
            } finally {
                yield $stmt->dispose();
            }
        } finally {
            $pool->shutdown();
        }
    }
}
