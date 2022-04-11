<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Config;
use Amp\Redis\Redis;
use Amp\Redis\RemoteExecutor;

Amp\Loop::run(static function () {
    $redisCluster = new DwaysInc\RedisCluster\RedisCluster(...[
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-1:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-2:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-3:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-4:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-5:6379?password=bitnami',))),
    ]);

    yield $redisCluster->set('a', 12);
});