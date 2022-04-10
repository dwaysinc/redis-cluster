<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\Config;
use Amp\Redis\Redis;
use Amp\Redis\RemoteExecutor;

Amp\Loop::run(static function () {
    $redisCluster = new DwaysInc\RedisCluster\RedisCluster(...[
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6380?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6381?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6382?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6383?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6384?password=bitnami',))),
    ]);

    yield $redisCluster->set('a', 12);
});