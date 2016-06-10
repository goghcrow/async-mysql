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

use KoolKode\Async\Util\AsyncTestTrait;

use function KoolKode\Async\awaitAll;
use function KoolKode\Async\runTask;

class SetupTest extends \PHPUnit_Framework_TestCase
{
    use AsyncTestTrait;
    
    /**
     * Get an ENV param.
     *
     * @param string $name Name of the env variable.
     * @param mixed $default Default value to be used.
     * @return mixed
     *
     * @throws \OutOfBoundsException When env param is not set and no default value was given.
     */
    protected function getEnvParam(string $name)
    {
        if (array_key_exists($name, $GLOBALS)) {
            return $GLOBALS[$name];
        }
        
        if (array_key_exists($name, $_ENV)) {
            return $_ENV[$name];
        }
        
        if (array_key_exists($name, $_SERVER)) {
            return $_SERVER[$name];
        }
        
        if (func_num_args() > 1) {
            return func_get_arg(1);
        }
        
        throw new \OutOfBoundsException(sprintf('ENV param not found: "%s"', $name));
    }

    public function testConnection()
    {
        $pdo = new \PDO($this->getEnvParam('DB_DSN'), $this->getEnvParam('DB_USERNAME', NULL), $this->getEnvParam('DB_PASSWORD', NULL));
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        
        $ddl = file_get_contents(__DIR__ . '/test.sql');
        
        foreach (array_map('trim', explode(';', $ddl)) as $cmd) {
            if ($cmd === '') {
                continue;
            }
            
            $pdo->exec($cmd);
        }
        
        $pdo->exec("INSERT INTO customer (name) VALUES ('KoolKode'), ('Async'), ('MySQL'), ('Git')");
        
        $executor = $this->createExecutor();
        
        $executor->runCallback(function () {
            $conn = yield from Connection::connect($this->getEnvParam('DB_DSN'), $this->getEnvParam('DB_USERNAME', ''), $this->getEnvParam('DB_PASSWORD', ''));
            
            try {
                $stmt = $conn->prepare("SELECT * FROM customer ORDER BY name DESC");
                $this->assertTrue($stmt instanceof Statement);
                
                $this->assertEquals(0, yield from $stmt->execute());
                
                try {
                    $rows = yield from $stmt->fetchRows();
                    $this->assertCount(4, $rows);
                    $this->assertEquals([
                        'MySQL',
                        'KoolKode',
                        'Git',
                        'Async'
                    ], array_column($rows, 'name'));
                } finally {
                    $stmt->close();
                }
                
                $stmt = $conn->prepare("UPDATE customer SET name = ? WHERE name = ?");
                $this->assertTrue($stmt instanceof Statement);
                
                $stmt->bindParams([
                    'GitHub',
                    'Git'
                ]);
                
                $this->assertEquals(1, yield from $stmt->execute());
                
                $stmt = $conn->prepare("SELECT * FROM customer WHERE id > ? ORDER BY name DESC");
                $stmt->bindParam(1);
                
                $this->assertEquals(0, yield from $stmt->execute());
                
                $values = yield from $stmt->fetchColumn('name');
                $this->assertCount(3, $values);
                $this->assertEquals([
                    'MySQL',
                    'GitHub',
                    'Async'
                ], $values);
            } finally {
                $conn->shutdown();
            }
        });
        
        $executor->run();
    }

    /**
     * Test is needed in order to verify OK-processing flag in Client::readNextPacket().
     */
    public function testMassInserts()
    {
        return $this->markTestSkipped();
        $pdo = new \PDO($this->getEnvParam('DB_DSN'), $this->getEnvParam('DB_USERNAME', NULL), $this->getEnvParam('DB_PASSWORD', NULL));
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        
        $ddl = file_get_contents(__DIR__ . '/test.sql');
        
        foreach (array_map('trim', explode(';', $ddl)) as $cmd) {
            if ($cmd === '') {
                continue;
            }
            
            $pdo->exec($cmd);
        }
        
        $executor = $this->createExecutor();
        
        $executor->runCallback(function () use ($executor) {
            $pool = new Pool($executor, $this->getEnvParam('DB_DSN'), $this->getEnvParam('DB_USERNAME', ''), $this->getEnvParam('DB_PASSWORD', ''), 32);
            
            try {
                $insert = function (ConnectionInterface $conn, int $i): \Generator {
                    $stmt = yield from $conn->prepare("INSERT INTO `customer` (`name`) VALUES (?)");
                    $this->assertTrue($stmt instanceof Statement);
                    
                    try {
                        $stmt->bindValue(0, bin2hex(random_bytes(16)));
                        
                        $result = yield from $stmt->execute();
                        $this->assertTrue($result instanceof ResultSet);
                    } finally {
                        $stmt->free();
                    }
                };
                
                $size = 400;
                $tasks = [];
                
                for ($i = 0; $i < $size; $i++) {
                    $tasks[] = yield runTask($insert($pool, $i));
                }
                
                yield awaitAll($tasks);
                
                $stmt = yield from $pool->prepare("SELECT * FROM `customer` ORDER BY `id`");
                
                $result = yield from $stmt->execute();
                $this->assertTrue($result instanceof ResultSet);
                $this->assertEquals(-1, $result->rowCount());
                
                $values = yield from $result->fetchColumnArray('name');
                $this->assertCount($size, $values);
            } finally {
                yield from $pool->close();
            }
        });
        
        $executor->run();
    }
}
