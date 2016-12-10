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

/**
 * Factory that establishes connections to a MySQL DB server.
 * 
 * @author Martin Schröder
 */
class ConnectionFactory
{
    /**
     * Establish a socket connection to a MySQL server.
     * 
     * @param string $dsn PDO-style data source name.
     * @param string $username DB username.
     * @param string $password DB password.
     * @return Connection
     * 
     * @throws \InvalidArgumentException When the DSN is invalid.
     */
    public function connect(string $dsn, string $username, string $password): Awaitable
    {
        return new Coroutine($this->establishConnection($dsn, $username, $password));
    }

    protected function establishConnection(string $dsn, string $username, string $password): \Generator
    {
        $settings = $this->parseSettings($dsn);
        $factory = $this->createSocketFactory($settings);
        
        $socket = yield $factory->createSocketStream();
        
        try {
            $client = new Client($socket);
            
            yield from $client->handshake($username, $password);
            
            if (isset($settings['dbname'])) {
                yield $this->setDefaultDatabase($client, $settings['dbname']);
            }
        } catch (\Throwable $e) {
            $socket->close();
            
            throw $e;
        }
        
        return new Connection($client);
    }

    protected function parseSettings(string $dsn): array
    {
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
        
        return $settings;
    }

    protected function createSocketFactory(array $settings): SocketFactory
    {
        if (isset($settings['unix_socket'])) {
            return new SocketFactory($settings['unix_socket'], 'unix');
        }
        
        if (isset($settings['host'])) {
            $url = \sprintf('%s:%u', $settings['host'], $settings['port'] ?? 3306);
            
            return new SocketFactory($url);
        }
        
        if (empty($settings['host']) && empty($settings['unix_socket'])) {
            throw new \InvalidArgumentException('Neighter MySQL host nor Unix domain socket specified in MySQL DSN');
        }
    }

    protected function setDefaultDatabase(Client $client, string $database): Awaitable
    {
        return $client->sendCommand(function (Client $client) use ($database) {
            $builder = new PacketBuilder();
            $builder->writeInt8(0x02);
            $builder->write($database);
            
            yield from $client->sendPacket($builder->build());
            yield from $client->readPacket(0x00);
        });
    }
}
