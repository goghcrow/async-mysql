<?php

/*
 * This file is part of KoolKode Async MySQL.
 *
 * (c) Martin Schröder <m.schroeder2007@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types = 1);

namespace KoolKode\Async\MySQL;

use KoolKode\Async\Awaitable;
use KoolKode\Async\Coroutine;
use KoolKode\Async\Socket\SocketFactory;

class Connection
{
    protected $client;
    
    public function __construct(Client $client)
    {
        $this->client = $client;
    }
    
    public static function connect(string $dsn, string $username, string $password): Awaitable
    {
        return new Coroutine(function () use ($dsn, $username, $password) {
            if ('mysql:' !== \substr($dsn, 0, 6)) {
                throw new \InvalidArgumentException(sprintf('Invalid MySQL DSN: "%s"', $dsn));
            }
            
            $settings = [];
            
            foreach (\explode(';', \substr($dsn, 6)) as $part) {
                list ($k, $v) = \array_map('trim', \explode('=', $part));
                
                switch ($k) {
                    case 'host':
                    case 'dbname':
                    case 'unix_socket':
                        $settings[$k] = $v;
                        break;
                    case 'port':
                        $settings[$k] = (int) $v;
                        break;
                    default:
                        throw new \InvalidArgumentException(\sprintf('Unknown MySQL DSN param: "%s"', $k));
                }
            }
            
            if (empty($settings['host']) && empty($settings['unix_socket'])) {
                throw new \InvalidArgumentException('Neighter MySQL host nor Unix domain socket specified in MySQL DSN');
            }
            
            if (empty($settings['unix_socket'])) {
                $url = \sprintf('%s:%u', $settings['host'], $settings['port'] ?? 3306);
                $factory = new SocketFactory($url);
            } else {
                $factory = new SocketFactory($settings['unix_socket'], 'unix');
            }
            
            $client = new Client($socket = yield $factory->createSocketStream());
            
            try {
                yield from $client->handshake($username, $password);
                
                if (isset($settings['dbname'])) {
                    yield $client->sendCommand(function (Client $client) use ($settings) {
                        yield from $client->sendPacket($client->encodeInt8(0x02) . $settings['dbname']);
                        yield from $client->readPacket(0x00);
                    });
                }
            } catch (\Throwable $e) {
                $socket->close();
                
                throw $e;
            }
            
            return new Connection($client);
        });
    }
    
    /**
     * Shut the DB connection down.
     * 
     * @param \Throwable $e Optional cause of shutdown.
     */
    public function shutdown(\Throwable $e = null): Awaitable
    {
        return $this->client->shutdown($e);
    }

    /**
     * Ping the DB server.
     * 
     * @return int Number of milliseconds needed to send ping packets back and forth.
     */
    public function ping(): Awaitable
    {
        return $this->client->sendCommand(function (Client $client) {
            $time = \microtime(true) * 1000;
            
            yield from $client->sendPacket($client->encodeInt8(0x0E));
            yield from $client->readPacket(0x00);
            
            return (int) \ceil((\microtime(true) * 1000 - $time) + .5);
        });
    }
    
    /**
     * Create a prepared statement from the given SQL.
     * 
     * @param string $sql
     * @return Statement
     */
    public function prepare(string $sql): Statement
    {
        return new Statement($sql, $this->client);
    }
}
