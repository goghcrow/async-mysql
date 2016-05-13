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

use KoolKode\Async\Database\ConnectionInterface;
use KoolKode\Async\Database\StatementInterface;
use KoolKode\Async\ExecutorInterface;
use KoolKode\Async\Socket\Socket;
use KoolKode\Async\Socket\SocketStream;
use Psr\Log\LoggerInterface;

use function KoolKode\Async\await;
use function KoolKode\Async\currentExecutor;

class Connection implements ConnectionInterface
{
    protected $client;
    
    protected $pool;
    
    protected $logger;
    
    protected $pendingTasks;
    
    protected $pendingWorker;
    
    protected $executor;
    
    public function __construct(Client $client, ExecutorInterface $executor, LoggerInterface $logger = NULL)
    {
        $this->client = $client;
        $this->executor = $executor;
        $this->logger = $logger;
        $this->pendingTasks = new \SplQueue();
    }
    
    public function __destruct()
    {
        $this->shutdown();
    }
    
    public function setPool(Pool $pool = NULL)
    {
        $this->pool = $pool;
    }
    
    public function releaseStatement(int $id)
    {
        if ($this->pool !== NULL) {
            try {
                $this->pool->releaseConnection($this);
            } finally {
                $this->pool = NULL;
            }
        }
    }
    
    public static function connect(string $dsn, string $username, string $password, LoggerInterface $logger = NULL): \Generator
    {
        if ('mysql:' !== substr($dsn, 0, 6)) {
            throw new \InvalidArgumentException(sprintf('Invalid MySQL DSN: "%s"', $dsn));
        }
        
        $settings = [];
        
        foreach (explode(';', substr($dsn, 6)) as $part) {
            list ($k, $v) = array_map('trim', explode('=', $part));
            
            switch ($k) {
                case 'host':
                case 'dbname':
                case 'unix_socket':
                    $settings[$k] = $v;
                    break;
                case 'port':
                    $settings[$k] = (int)$v;
                    break;
                default:
                    throw new \InvalidArgumentException(sprintf('Unknown MySQL DSN param: "%s"', $k));
            }
        }
        
        if (empty($settings['host']) && empty($settings['unix_socket'])) {
            throw new \InvalidArgumentException('Neighter MySQL host nor Unix domain socket specified in MySQL DSN');
        }
        
        if (!empty($settings['unix_socket'])) {
            if (!Socket::isUnixSocketSupported()) {
                throw new \RuntimeException(sprintf('Cannot connect to MySQL socket "%s", PHP was not compiled with support for Unix domain sockets', $settings['unix_socket']));
            }
            
            $client = new Client(yield from SocketStream::fromUrl('unix://' . $settings['unix_socket']), $logger);
        } else {
            $client = new Client(yield from SocketStream::connect($settings['host'], $settings['port'] ?? 3306), $logger);
        }
        
        yield from $client->handleHandshake($username, $password);
        
        $conn = new static($client, yield currentExecutor(), $logger);
        
        if (!empty($settings['dbname'])) {
            yield from $conn->changeDefaultSchema($settings['dbname']);
        }
        
        return $conn;
    }
    
    /**
     * {@inheritdoc}
     */
    public function shutdown()
    {
        if ($this->client !== NULL) {
            $this->executor->runCallback(function () {
                try {
                    yield from $this->awaitPendingTasks();
                    
                    try {
                        if ($this->client->canSendCommand()) {
                            yield from $this->client->sendCommand($this->client->encodeInt8(0x01));
                        }
                    } finally {
                        $this->client->close();
                    }
                } finally {
                    $this->client = NULL;
                }
            });
        }
    }
    
    public function awaitPendingTasks(): \Generator
    {
        if ($this->pendingWorker !== NULL) {
            yield await($this->pendingWorker);
        }
    }

    public function queueWork(callable $work, ...$args)
    {
        $this->pendingTasks->enqueue([
            $work,
            $args
        ]);
        
        if ($this->pendingWorker === NULL) {
            $this->pendingWorker = $this->executor->runCallback(function () {
                try {
                    while (!$this->pendingTasks->isEmpty()) {
                        list ($callback, $args) = $this->pendingTasks->dequeue();
                        
                        $result = $callback(...$args);
                        
                        if ($result instanceof \Generator) {
                            yield from $result;
                        }
                    }
                } finally {
                    $this->pendingWorker = NULL;
                }
            });
        }
    }
    
    public function lastInsertId()
    {
        return $this->client->getLastInsertId();
    }
    
    public function changeDefaultSchema(string $schema): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x02) . $schema);
            yield from $this->client->readNextPacket();
        } finally {
            $this->client->flush();
        }
    }
    
    public function ping(): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x0E));
            
            $response = yield from $this->client->readNextPacket();
            
            return ord($response) === 0x00;
        } finally {
            $this->client->flush();
        }
    }
    
    public function prepare(string $sql): StatementInterface
    {
        return new Statement($sql, $this, $this->client, $this->logger);
    }
    
    public function assert(bool $condition, string $message)
    {
        if (!$condition) {
            throw new ProtocolError($message);
        }
    }
    
    public function parseColumnDefinition(string $packet): array
    {
        $off = 0;
        
        $this->assert('def' === $this->client->readLengthEncodedString($packet, $off), 'Missing column def');
        
        $col = [
            'schema' => $this->client->readLengthEncodedString($packet, $off),
            'table' => $this->client->readLengthEncodedString($packet, $off),
            'org_table' => $this->client->readLengthEncodedString($packet, $off),
            'name' => $this->client->readLengthEncodedString($packet, $off),
            'org_name' => $this->client->readLengthEncodedString($packet, $off)
        ];
        
        $this->assert(0 < $this->client->readLengthEncodedInt($packet, $off), 'Missing fixed column values');
        
        $col['charset'] = $this->client->readInt16($packet, $off);
        $col['length'] = $this->client->readInt32($packet, $off);
        $col['type'] = $this->client->readInt8($packet, $off);
        $col['flags'] = $this->client->readInt16($packet, $off);
        $col['decimals'] = $this->client->readInt8($packet, $off);
        
        $this->assert("\0\0" === $this->client->readFixedLengthString($packet, 2, $off), 'Missing column definition filler');
        
        return $col;
    }
}
