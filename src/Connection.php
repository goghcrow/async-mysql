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
                
                $client->flush();
                
                try {
                    if (isset($settings['dbname'])) {
                        yield from $client->sendCommand($client->encodeInt8(0x02) . $settings['dbname']);
                        
                        $packet = yield from $client->readNextPacket();
                        
                        if (\ord($packet[0]) !== 0x00) {
                            throw new \RuntimeException(\sprintf('Failed to switch default DB to "%s"', $settings['dbname']));
                        }
                    }
                } finally {
                    $client->flush();
                }
            } catch (\Throwable $e) {
                $socket->close();
                
                throw $e;
            }
            
            return new Connection($client);
        });
    }
}
