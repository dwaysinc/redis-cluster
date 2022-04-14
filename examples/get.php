<?php

require_once __DIR__ . '/../vendor/autoload.php';

use Amp\Log\ConsoleFormatter;
use Amp\Log\StreamHandler;
use Amp\Redis\Config;
use Amp\Redis\Redis;
use Amp\Redis\RemoteExecutor;
use Monolog\Logger;
use function Amp\ByteStream\getStdout;

Amp\Loop::run(static function () {
    $handler = new StreamHandler(getStdout());
    $handler->setFormatter(new ConsoleFormatter);

    $logger = new Logger('example');
    $logger->pushHandler($handler);

    $redisCluster = new DwaysInc\RedisCluster\RedisCluster(...[
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-0:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-1:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-2:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-3:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-4:6379?password=bitnami',))),
        new Redis(new RemoteExecutor(Config::fromUri('tcp://redis-node-5:6379?password=bitnami',))),
    ]);

    $redisCluster->setLogger($logger);

    $key = 12345678;

    yield $redisCluster->set($key, 12);

    $value = yield $redisCluster->get(12345678);

    $logger->info(sprintf('Got value by key %s - %s', $key, $value));
});