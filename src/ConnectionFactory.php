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
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;

/**
 * Factory that establishes connections to a MySQL DB server.
 * 
 * @author Martin Schröder
 */
class ConnectionFactory implements LoggerAwareInterface
{
    use LoggerAwareTrait;
    
    /**
     * DB connection settings.
     * 
     * @var array
     */
    protected $settings;

    /**
     * DB login username.
     * 
     * @var string
     */
    protected $username;

    /**
     * DB login password.
     * 
     * @var string
     */
    protected $password;

    /**
     * Socket factory being used to connect to the DB server.
     * 
     * @var SocketFactory
     */
    protected $socketFactory;

    /**
     * Create a new MySQL DB connection factory.
     * 
     * @param string $dsn PSO-style DSN.
     * @param string $username
     * @param string $password
     */
    public function __construct(string $dsn, string $username = null, string $password = null)
    {
        $this->settings = $this->parseSettings($dsn);
        $this->username = $username ?? '';
        $this->password = $password ?? '';
        
        $this->socketFactory = $this->createSocketFactory();
    }

    /**
     * Establish a new connection to a MySQL server.
     * 
     * @return MySqlConnection
     */
    public function connect(): Awaitable
    {
        return new Coroutine($this->establishConnection());
    }
    
    /**
     * Create a new connected MySQL client.
     *
     * @return Client
     */
    public function connectClient(): Awaitable
    {
        return new Coroutine($this->establishConnection(false));
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

    protected function createSocketFactory(): SocketFactory
    {
        if (isset($this->settings['unix_socket'])) {
            return new SocketFactory($this->settings['unix_socket'], 'unix');
        }
        
        if (isset($this->settings['host'])) {
            $url = \sprintf('%s:%u', $this->settings['host'], $this->settings['port'] ?? 3306);
            
            return new SocketFactory($url);
        }
        
        throw new \InvalidArgumentException('Neighter MySQL host nor Unix domain socket specified in MySQL DSN');
    }

    protected function establishConnection(bool $wrap = true): \Generator
    {
        $socket = yield $this->socketFactory->createSocketStream();
        
        try {
            $client = new Client($socket, $this->logger);
            
            yield from $client->handshake($this->username, $this->password);
            yield $this->switchToUnicode($client);
            
            if (isset($this->settings['dbname'])) {
                yield $this->switchDefaultDatabase($client);
            }
        } catch (\Throwable $e) {
            $socket->close();
            
            throw $e;
        }
        
        return $wrap ? new MySqlConnection($client, $this->logger) : $client;
    }

    protected function switchToUnicode(Client $client): Awaitable
    {
        return $client->sendCommand(function (Client $client) {
            $builder = new PacketBuilder();
            $builder->writeInt8(0x03);
            $builder->write("SET NAMES 'utf8'");
            
            yield from $client->sendPacket($builder->build());
            yield from $client->readPacket(0x00, 0xFE);
        });
    }

    protected function switchDefaultDatabase(Client $client): Awaitable
    {
        return $client->sendCommand(function (Client $client) {
            $builder = new PacketBuilder();
            $builder->writeInt8(0x02);
            $builder->write($this->settings['dbname']);
            
            yield from $client->sendPacket($builder->build());
            yield from $client->readPacket(0x00);
        });
    }
}
