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

use KoolKode\Async\Stream\SocketStream;
use Psr\Log\LoggerInterface;

class Connection implements ConnectionInterface
{
    const DEFAULT_PORT = 3306;
    
    protected $client;
    
    protected $pool;
    
    protected $logger;
    
    public function __construct(Client $client, LoggerInterface $logger = NULL)
    {
        $this->client = $client;
        $this->logger = $logger;
    }
    
    public function getClient(): Client
    {
        return $this->client;
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
                    $settings[$k] = $v;
                    break;
                case 'port':
                    $settings[$k] = (int)$v;
                    break;
                default:
                    throw new \InvalidArgumentException(sprintf('Unknown MySQL DSN param: "%s"', $k));
            }
        }
        
        if (empty($settings['host'])) {
            throw new \InvalidArgumentException('Missing MySQL host in DSN');
        }
        
        $host = $settings['host'];
        $port = $settings['port'] ?? self::DEFAULT_PORT;
        
        $client = new Client(yield from SocketStream::connect($host, $port), $logger);

        yield from $client->handleHandshake($username, $password);
        
        $conn = new static($client, $logger);
        
        if (!empty($settings['dbname'])) {
            yield from $conn->changeDefaultSchema($settings['dbname']);
        }
        
        return $conn;
    }
    
    public function lastInsertId()
    {
        return $this->client->getLastInsertId();
    }
    
    public function close(): \Generator
    {
        try {
            if ($this->client->canSendCommand()) {
                yield from $this->client->sendCommand($this->client->encodeInt8(0x01));
            }
        } finally {
            $this->client->close();
        }
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
    
    public function prepare(string $sql): \Generator
    {
        try {
            yield from $this->client->sendCommand($this->client->encodeInt8(0x16) . $sql);
            
            $packet = yield from $this->client->readNextPacket(false);
            $off = 0;
            
            $this->assert($this->client->readInt8($packet, $off) === 0x00, 'Status is not OK');
            
            $id = $this->client->readInt32($packet, $off);
            
            $cc = $this->client->readInt16($packet, $off);
            $pc = $this->client->readInt16($packet, $off);
            
            $this->assert($this->client->readInt8($packet, $off) === 0x00, 'Missing filler');
            
            // Warning count:
            $this->client->readInt16($packet, $off);
            
            $cols = [];
            $params = [];
            
            // Params:
            for ($i = 0; $i < $pc; $i++) {
                $params[] = $this->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($pc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after param definitions');
            }
            
            // Columns:
            for ($i = 0; $i < $cc; $i++) {
                $cols[] = $this->parseColumnDefinition(yield from $this->client->readNextPacket());
            }
            
            if ($cc > 0 && !$this->client->hasCapabilty(Client::CLIENT_DEPRECATE_EOF)) {
                $this->assert(0xFE === ord(yield from $this->client->readNextPacket()), 'Missing EOF packet after column definitions');
            }
            
            if ($this->logger) {
                $this->logger->debug('Prepared SQL statement: {sql}', [
                    'sql' => trim(preg_replace("'\s+'", ' ', $sql))
                ]);
            }
            
            return new Statement($this, $id, $cols, $params, $this->logger);
        } finally {
            $this->client->flush();
        }
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
