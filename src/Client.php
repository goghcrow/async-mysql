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
use KoolKode\Async\Concurrent\Executor;
use KoolKode\Async\Socket\SocketStream;
use KoolKode\Async\Success;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;

/**
 * Client that synchronizes access to a MySQL DB.
 * 
 * @author Martin Schröder
 */
class Client implements LoggerAwareInterface
{
    use LoggerAwareTrait;
    
    /**
     * Stream being used to communicate with the DB server.
     * 
     * @var SocketStream
     */
    protected $socket;

    /**
     * Connection ID.
     * 
     * @var int
     */
    protected $id;

    /**
     * Current sequence number of DB packets.
     * 
     * @var int
     */
    protected $sequence = -1;

    /**
     * Has the client been disposed yet?
     * 
     * @var bool
     */
    protected $disposed = false;

    /**
     * Informal data returned by the DB server.
     * 
     * @var array
     */
    protected $info = [];

    /**
     * Negotiated protocol capabilities.
     * 
     * @var int
     */
    protected $capabilities = 0;

    /**
     * Capabilities of this client.
     * 
     * @var int
     */
    protected $clientCaps = 0;

    /**
     * Capabilities of the DB server.
     * 
     * @var int
     */
    protected $serverCaps = 0;

    /**
     * Connection status flags.
     * 
     * @var int
     */
    protected $statusFlags = 0;

    /**
     * Executor being used to queue commands and execute them one by one in the correct order.
     * 
     * @var Executor
     */
    protected $executor;

    /**
     * Has a transaction been started?
     * 
     * @var bool
     */
    protected $transaction = false;

    public function __construct(SocketStream $socket)
    {
        $this->socket = $socket;
        $this->logger = new Logger(static::class);
        
        $this->executor = new Executor();
        
        $this->clientCaps |= Constants::CLIENT_SESSION_TRACK;
        $this->clientCaps |= Constants::CLIENT_TRANSACTIONS;
        $this->clientCaps |= Constants::CLIENT_PROTOCOL_41;
        $this->clientCaps |= Constants::CLIENT_DEPRECATE_EOF;
        $this->clientCaps |= Constants::CLIENT_SECURE_CONNECTION;
        $this->clientCaps |= Constants::CLIENT_PLUGIN_AUTH;
        $this->clientCaps |= Constants::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
    }

    public function isDisposed(): bool
    {
        return $this->disposed;
    }

    public function isWithinTransaction(): bool
    {
        return $this->transaction;
    }

    public function shutdown(\Throwable $e = null): Awaitable
    {
        if ($this->disposed) {
            return new Success();
        }
        
        $this->disposed = true;
        
        if ($e) {
            $this->executor->cancel('MySQL client shut down', $e);
        }
        
        return $this->executor->submit(function () {
            $this->logger->info('Connection closed');
            
            return $this->socket->close();
        });
    }

    public function handshake(string $username, string $password): \Generator
    {
        try {
            list ($auth, $authPlugin) = yield from $this->readAuthChallenge();
            
            $this->capabilities = $this->clientCaps & $this->serverCaps;
            
            $packet = $this->createAuthPacket($username, $password, $auth, $authPlugin);
            
            yield from $this->sendPacket($packet);
            $packet = yield from $this->readRawPacket();
            
            if ($packet->readInt8() !== 0x00) {
                throw new \RuntimeException('Authentication failed');
            }
        } finally {
            $this->sequence = -1;
        }
        
        $this->logger->debug('Authenticated as user {user}', [
            'user' => $username
        ]);
    }

    protected function readAuthChallenge(): \Generator
    {
        $packet = yield from $this->readRawPacket();
        
        if (0x0A !== ($version = $packet->readInt8())) {
            throw new \RuntimeException(\sprintf('Unsupported protocol version: "0x%02X"', $version));
        }
        
        $this->info['server'] = $packet->readNullString();
        $this->id = $packet->readInt32();
        
        $auth = $packet->readFixedLengthString(8);
        $packet->discardByte(0x00);
        
        $this->serverCaps = $packet->readInt16();
        
        if (!$packet->isConsumed()) {
            $this->info['charset'] = $packet->readInt8();
            $this->statusFlags = $packet->readInt16();
            $this->serverCaps |= ($packet->readInt16() << 16);
            
            if ($this->serverCaps & Constants::CLIENT_PLUGIN_AUTH) {
                $len2 = $packet->readInt8();
            } else {
                $packet->discardByte(0x00);
                $len2 = 0;
            }
            
            if ($this->serverCaps & Constants::CLIENT_SECURE_CONNECTION) {
                for ($i = 0; $i < 10; $i++) {
                    $packet->discardByte(0x00);
                }
                
                $auth .= $packet->readFixedLengthString(\max(13, $len2 - 8));
                
                if ($this->serverCaps & Constants::CLIENT_PLUGIN_AUTH) {
                    $authPlugin = \trim($packet->readNullString());
                }
            }
        }
        
        return [
            $auth,
            $authPlugin ?? ''
        ];
    }

    protected function createAuthPacket(string $username, string $password, string $auth, string $authPlugin): string
    {
        $builder = new PacketBuilder();
        $builder->writeInt32($this->capabilities);
        $builder->writeInt32(1 << 24 - 1);
        $builder->writeInt8(45 /* Charset 45 = utf8mb4_general_ci */);
        $builder->write(\str_repeat("\x00", 23));
        $builder->writeNullString($username);
        
        if ($password === '') {
            $credentials = '';
        } else {
            // TODO: Implement support for sha256_password authentication.
            
            switch ($authPlugin) {
                case 'mysql_native_password':
                    $credentials = $this->secureAuth($password, $auth);
                    break;
                case 'mysql_clear_password':
                    $credentials = $password;
                    break;
                default:
                    throw new \RuntimeException(\sprintf('Unsupported authentication plugin: "%s"', $authPlugin));
            }
        }
        
        if ($this->capabilities & Constants::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
            $builder->writeLengthEncodedString($credentials);
        } elseif ($this->capabilities & Constants::CLIENT_SECURE_CONNECTION) {
            $builder->writeInt8(\strlen($credentials));
            $builder->write($credentials);
        } else {
            $builder->writeNullString($credentials);
        }
        
        if ($this->capabilities & Constants::CLIENT_PLUGIN_AUTH) {
            $builder->writeNullString($authPlugin ?? '');
        }
        
        return $builder->build();
    }

    protected function secureAuth(string $password, string $scramble): string
    {
        $hash = \sha1($password, true);
        
        return $hash ^ \sha1(\substr($scramble, 0, 20) . \sha1($hash, true), true);
    }

    public function isEofDeprecated(): bool
    {
        return ($this->capabilities & Constants::CLIENT_DEPRECATE_EOF) !== 0;
    }

    public function readRawPacket(bool $object = true): \Generator
    {
        $header = yield $this->socket->readBuffer(4, true);
        
        $len = \unpack('V', \substr($header, 0, 3) . "\x00")[1];
        $this->sequence = \ord($header[3]);
        
        if ($object) {
            return new Packet($len ? yield $this->socket->readBuffer($len, true) : '');
        }
        
        return $len ? yield $this->socket->readBuffer($len, true) : '';
    }

    public function readPacket(int ...$expected): \Generator
    {
        $header = yield $this->socket->readBuffer(4, true);
        
        $len = \unpack('V', \substr($header, 0, 3) . "\x00")[1];
        $this->sequence = \ord($header[3]);
        
        $payload = $len ? yield $this->socket->readBuffer($len, true) : '';
        $packet = new Packet(\substr($payload, 1), \ord($payload[0]));
        
        if ($packet->type === 0xFF) {
            throw $this->populateError($packet);
        }
        
        if ($expected && !\in_array($packet->type, $expected, true)) {
            $expected = \implode(', ', \array_map(function (int $type) {
                return \sprintf('0x%02X', $type);
            }, $expected));
            
            throw new \RuntimeException(\sprintf('Received 0x%02X packet, expecting one of %s', $packet->type, $expected));
        }
        
        return $packet;
    }

    public function populateError(Packet $packet): \Throwable
    {
        $code = $packet->readInt16();
        $marker = $packet->readFixedLengthString(1);
        $state = $packet->readFixedLengthString(5);
        $message = $packet->readEofString();
        
        return new \RuntimeException(\sprintf('SQLSTATE [%s]: "%s"', $state, $message), $code);
    }

    public function parseOk(Packet $packet): array
    {
        $affected = $packet->readLengthEncodedInt();
        $lastId = $packet->readLengthEncodedInt();
        
        if ($this->capabilities & Constants::CLIENT_PROTOCOL_41) {
            $status = $packet->readInt16();
            $warnings = $packet->readInt16();
        } elseif ($this->capabilities & Constants::CLIENT_TRANSACTIONS) {
            $status = $packet->readInt16();
        }
        
        if ($this->capabilities & Constants::CLIENT_SESSION_TRACK) {
            $info = $packet->readLengthEncodedString();
            
            if ($status & Constants::SERVER_SESSION_STATE_CHANGED) {
                $changes = $packet->readLengthEncodedString();
            }
        } else {
            $info = $packet->readEofString();
        }
        
        $packet->reset();
        
        return [
            'affected' => $affected,
            'lastId' => $lastId,
            'status' => $status ?? 0,
            'warnings' => $warnings ?? 0,
            'info' => $info,
            'changes' => $changes ?? ''
        ];
    }
    
    public function sendCommand(callable $callback): Awaitable
    {
        return $this->executor->submit(function () use ($callback) {
            try {
                $result = $callback($this);
                
                if ($result instanceof \Generator) {
                    $result = yield from $result;
                }
                
                return $result;
            } finally {
                $this->sequence = -1;
            }
        });
    }

    public function sendPacket(string $packet): \Generator
    {
        $packet = \substr(\pack('V', \strlen($packet)), 0, 3) . \chr(++$this->sequence % 256) . $packet;
        
        return yield $this->socket->write($packet);
    }

    public function beginTransaction(bool $readOnly): Awaitable
    {
        return $this->sendCommand(function () use ($readOnly) {
            if ($this->transaction) {
                throw new \RuntimeException('Cannot start transaction while another transaction is active');
            }
            
            $sql = 'START TRANSACTION';
            
            if ($readOnly) {
                $sql .= ' READ ONLY';
            }
            
            $packet = new PacketBuilder();
            $packet->writeInt8(0x03);
            $packet->write($sql);
            
            try {
                yield from $this->sendPacket($packet->build());
                $state = $this->parseOk(yield from $this->readPacket(0x00, 0xFE));
                
                if (!($state['status'] & Constants::SERVER_STATUS_IN_TRANS)) {
                    throw new \RuntimeException('Failed to start transaction');
                }
            } catch (\Throwable $e) {
                $this->shutdown($e);
                
                throw $e;
            }
            
            $this->transaction = true;
            
            $this->logger->debug('Transaction started');
        });
    }

    public function commit(): Awaitable
    {
        return $this->sendCommand(function () {
            if (!$this->transaction) {
                throw new \RuntimeException('Cannot commit when not within a transaction');
            }
            
            $packet = new PacketBuilder();
            $packet->writeInt8(0x03);
            $packet->write('COMMIT');
            
            try {
                yield from $this->sendPacket($packet->build());
                $state = $this->parseOk(yield from $this->readPacket(0x00, 0xFE));
                
                if ($state['status'] & Constants::SERVER_STATUS_IN_TRANS) {
                    throw new \RuntimeException('Failed to terminate transaction');
                }
            } catch (\Throwable $e) {
                $this->shutdown($e);
                
                throw $e;
            }
            
            $this->transaction = false;
            
            $this->logger->debug('Transaction committed');
        });
    }

    public function rollBack(): Awaitable
    {
        return $this->sendCommand(function () {
            if (!$this->transaction) {
                throw new \RuntimeException('Cannot roll back when not within a transaction');
            }
            
            $packet = new PacketBuilder();
            $packet->writeInt8(0x03);
            $packet->write('ROLLBACK');
            
            try {
                yield from $this->sendPacket($packet->build());
                $state = $this->parseOk(yield from $this->readPacket(0x00, 0xFE));
                
                if ($state['status'] & Constants::SERVER_STATUS_IN_TRANS) {
                    throw new \RuntimeException('Failed to terminate transaction');
                }
            } catch (\Throwable $e) {
                $this->shutdown($e);
                
                throw $e;
            }
            
            $this->transaction = false;
            
            $this->logger->debug('Transaction rolled back');
        });
    }
}
