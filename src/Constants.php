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

/**
 * MySQL constants used by the binary protocol.
 * 
 * @author Martin Schröder
 */
final class Constants
{
    const CLIENT_LONG_FLAG = 0x00000004;

    const CLIENT_CONNECT_WITH_DB = 0x00000008;

    const CLIENT_COMPRESS = 0x00000020;

    const CLIENT_PROTOCOL_41 = 0x00000200;

    const CLIENT_SSL = 0x00000800;

    const CLIENT_TRANSACTIONS = 0x00002000;

    const CLIENT_SECURE_CONNECTION = 0x00008000;

    const CLIENT_MULTI_STATEMENTS = 0x00010000;

    const CLIENT_MULTI_RESULTS = 0x00020000;

    const CLIENT_PS_MULTI_RESULTS = 0x00040000;

    const CLIENT_PLUGIN_AUTH = 0x00080000;

    const CLIENT_CONNECT_ATTRS = 0x00100000;

    const CLIENT_SESSION_TRACK = 0x00800000;

    const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x00200000;

    const CLIENT_DEPRECATE_EOF = 0x01000000;

    const MYSQL_TYPE_DECIMAL = 0x00;

    const MYSQL_TYPE_TINY = 0x01;

    const MYSQL_TYPE_SHORT = 0x02;

    const MYSQL_TYPE_LONG = 0x03;

    const MYSQL_TYPE_FLOAT = 0x04;

    const MYSQL_TYPE_DOUBLE = 0x05;

    const MYSQL_TYPE_NULL = 0x06;

    const MYSQL_TYPE_TIMESTAMP = 0x07;

    const MYSQL_TYPE_LONGLONG = 0x08;

    const MYSQL_TYPE_INT24 = 0x09;

    const MYSQL_TYPE_DATE = 0x0A;

    const MYSQL_TYPE_TIME = 0x0B;

    const MYSQL_TYPE_DATETIME = 0x0C;

    const MYSQL_TYPE_YEAR = 0x0D;

    const MYSQL_TYPE_NEWDATE = 0x0E;

    const MYSQL_TYPE_VARCHAR = 0x0F;

    const MYSQL_TYPE_BIT = 0x10;

    const MYSQL_TYPE_TIMESTAMP2 = 0x11;

    const MYSQL_TYPE_DATETIME2 = 0x12;

    const MYSQL_TYPE_TIME2 = 0x13;

    const MYSQL_TYPE_NEWDECIMAL = 0xF6;

    const MYSQL_TYPE_ENUM = 0xF7;

    const MYSQL_TYPE_SET = 0xF8;

    const MYSQL_TYPE_TINY_BLOB = 0xF9;

    const MYSQL_TYPE_MEDIUM_BLOB = 0xFA;

    const MYSQL_TYPE_LONG_BLOB = 0xFB;

    const MYSQL_TYPE_BLOB = 0xFC;

    const MYSQL_TYPE_VAR_STRING = 0xFD;

    const MYSQL_TYPE_STRING = 0xFE;

    const MYSQL_TYPE_GEOMETRY = 0xFF;

    const CURSOR_TYPE_NO_CURSOR = 0x00;

    const CURSOR_TYPE_READ_ONLY = 0x01;

    const CURSOR_TYPE_FOR_UPDATE = 0x02;

    const CURSOR_TYPE_SCROLLABLE = 0x04;
}
