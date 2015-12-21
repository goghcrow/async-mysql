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

use KoolKode\Async\ExecutorFactory;

class SetupTest extends \PHPUnit_Framework_TestCase
{
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
        
        $executor = (new ExecutorFactory())->createExecutor();
        
        $executor->runNewTask(call_user_func(function () {
            $conn = yield from Connection::connect($this->getEnvParam('DB_DSN'), $this->getEnvParam('DB_USERNAME', ''), $this->getEnvParam('DB_PASSWORD', ''));
            
            try {
                $this->assertTrue(yield from $conn->ping());
                
                $stmt = yield from $conn->prepare("SELECT * FROM customer ORDER BY name DESC");
                $this->assertTrue($stmt instanceof Statement);
                
                $result = yield from $stmt->execute();
                $this->assertTrue($result instanceof ResultSet);
                $this->assertEquals(-1, $result->rowCount());
                
                try {
                    $rows = yield from $result->fetchRowsArray();
                    $this->assertCount(4, $rows);
                    $this->assertEquals([
                        'MySQL',
                        'KoolKode',
                        'Git',
                        'Async'
                    ], array_column($rows, 'name'));
                } finally {
                    yield from $result->close();
                }
                
                $stmt = yield from $conn->prepare("UPDATE customer SET name = ? WHERE name = ?");
                $stmt->bindValue(0, 'GitHub');
                $stmt->bindValue(1, 'Git');
                
                $result = yield from $stmt->execute();
                $this->assertTrue($result instanceof ResultSet);
                $this->assertEquals(1, $result->rowCount());
                
                $stmt = yield from $conn->prepare("SELECT * FROM customer WHERE id > ? ORDER BY name DESC");
                $stmt->bindValue(0, 1);
                
                $result = yield from $stmt->execute();
                $this->assertTrue($result instanceof ResultSet);
                $this->assertEquals(-1, $result->rowCount());
                
                $values = yield from $result->fetchColumnArray('name');
                $this->assertCount(3, $values);
                $this->assertEquals([
                    'MySQL',
                    'GitHub',
                    'Async'
                ], $values);
            } finally {
                yield from $conn->close();
            }
        }));
        
        $executor->run();
    }
}
