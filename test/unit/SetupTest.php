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
        
        $executor = (new ExecutorFactory())->createExecutor();
        
        $executor->runNewTask(call_user_func(function () {
            $conn = yield from Connection::connect('localhost', $this->getEnvParam('DB_USERNAME', ''), $this->getEnvParam('DB_PASSWORD', ''));
            
            try {
                $this->assertTrue(yield from $conn->ping());
            } finally {
                yield from $conn->close();
            }
        }));
        
        $executor->run();
    }
}
