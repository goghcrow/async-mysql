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

use KoolKode\Async\Stream\BufferedDuplexStream;
use KoolKode\Async\Stream\DuplexStreamInterface;
use KoolKode\Async\Stream\SocketStream;

class Connection
{
    const DEFAULT_PORT = 3306;
    
    protected $stream;
    
    public function __construct(DuplexStreamInterface $stream)
    {
        $this->stream = $stream;
    }
    
    public static function connect(string $host, int $port = self::DEFAULT_PORT): \Generator
    {
        $stream = new BufferedDuplexStream(yield from SocketStream::connect($host, $port));
        
        var_dump(yield from $stream->read());
        
//         $version = yield from $stream->read(1);
//         $server = yield from $stream->readTo(chr(0));
        
//         var_dump(ord($version), $server);
        
        return new static($stream);
    }
}
