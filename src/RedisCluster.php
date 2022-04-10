<?php

namespace DwaysInc\RedisCluster;

use Amp\Promise;
use Amp\Redis\Redis;
use Amp\Redis\SetOptions;
use function Amp\call;

final class RedisCluster implements RedisClusterInterface
{
    /** @var Node[] */
    private array $nodes;
    /** @var Node[] */
    private array $activeMasterNodes;
    /** @var Node[] */
    private array $activeSlaveNodes;

    private Promise $connect;

    /**
     * @param Redis ...$redisList
     */
    public function __construct(Redis ...$redisList)
    {
        foreach ($redisList as $redis) {
            $this->nodes[] = new Node($redis);
        }
    }

    public function set(string $key, string $value, SetOptions $options = null): Promise
    {
        return call(function () {
            yield $this->connect();
        });
    }

    public function clusterNodes(): Promise
    {
        return call(function () {
            foreach ($this->nodes as $node) {
                $clusterNodes = ClusterNodesParser::parse(yield $node->clusterNodes());

                var_dump($clusterNodes);
                die;
            }
        });
    }

    private function connect(): Promise
    {
        if (isset($this->connect)) {
            return $this->connect;
        }

        return $this->connect = call(function () {
            $clusterNodes = yield $this->clusterNodes();

            var_dump($clusterNodes);
        });
    }
}