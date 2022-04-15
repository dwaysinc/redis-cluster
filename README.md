# Redis Cluster

Redis scales horizontally with a deployment topology called Redis Cluster. You can read more about Redis Cluster [here](https://redis.io/docs/manual/scaling/#:~:text=Redis%20Cluster%20provides%20a%20way,are%20not%20able%20to%20communicate.).

`dwaysinc/redis-cluster` based on [amphp/amp](https://github.com/amphp/amp) and [amphp/redis](https://github.com/amphp/redis) and provides non-blocking access to Redis Cluster.

## Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require dwaysinc/redis-cluster
```

## Usage

```php
<?php

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

    $value = yield $redisCluster->get($key); // (int) 12

    $logger->info(sprintf('Got value by key %s - %s', $key, $value));
});
```

## Contact

For contacting me use email [`dmitrii@dways.org`](mailto:dmitrii@dways.org).