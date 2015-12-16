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

class Statement
{
    const CURSOR_TYPE_NO_CURSOR = 0x00;
    
    const CURSOR_TYPE_READ_ONLY = 0x01;
    
    const CURSOR_TYPE_FOR_UPDATE = 0x02;
    
    const CURSOR_TYPE_SCROLLABLE = 0x04;
    
    protected $client;
    
    protected $id;
    
    protected $columns;
    
    protected $params;
    
    public function __construct(Client $client, int $id, array $columns, array $params)
    {
        $this->client = $client;
        $this->id = $id;
        $this->columns = $columns;
        $this->params = $params;
    }
    
    public function execute(): \Generator
    {
        $packet = $this->client->encodeInt8(0x17);
        $packet .= $this->client->encodeInt32($this->id);
        
        $flags = self::CURSOR_TYPE_NO_CURSOR;
        
        $packet .= $this->client->encodeInt32($flags);
        $packet .= $this->client->encodeInt32(1);
        
        yield from $this->client->sendPacket($packet);
    }
}
