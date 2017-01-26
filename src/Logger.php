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

use KoolKode\Async\Database\Logger as DatabaseLogger;

/**
 * Adds the KoolKode MySQL log origin.
 *
 * @author Martin Schröder
 */
class Logger extends DatabaseLogger
{
    /**
     * {@inheritdoc}
     */
    protected function getAdditionalOrigins(): array
    {
        return \array_merge(parent::getAdditionalOrigins(), [
            'koolkode/async-mysql'
        ]);
    }
}
