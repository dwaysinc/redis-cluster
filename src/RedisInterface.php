<?php

namespace DwaysInc\RedisCluster;

use Amp\Promise;
use Amp\Redis\SetOptions;

interface RedisInterface
{
    /**
     * @param string $key
     * @param string $value
     * @param SetOptions|null $options
     *
     * @return Promise
     */
    public function set(string $key, string $value, SetOptions $options = null): Promise;

    public function get(string $key): Promise;
}