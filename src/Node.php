<?php

namespace DwaysInc\RedisCluster;

use Amp\Failure;
use Amp\Promise;
use Amp\Redis\Redis;
use RuntimeException;

final class Node
{
    private Redis $redis;

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    public function clusterNodes(): Promise
    {
        return $this->redis->query('CLUSTER', 'NODES');
    }

    public function __call(string $name, array $arguments)
    {
        if (method_exists($this->redis, $name)) {
            return $this->redis->$name(...$arguments);
        }

        return new Failure(new RuntimeException(sprintf('Unknown method called: %s', $name)));
    }
}