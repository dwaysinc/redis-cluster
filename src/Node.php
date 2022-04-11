<?php

namespace DwaysInc\RedisCluster;

use Amp\Failure;
use Amp\Promise;
use Amp\Redis\Redis;
use RuntimeException;

final class Node
{
    private Redis $redis;
    private NodeInfo $nodeInfo;

    /** @var Node[] */
    private array $slaveNodes = [];

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    public function __call(string $name, array $arguments)
    {
        if (method_exists($this->redis, $name)) {
            return $this->redis->$name(...$arguments);
        }

        return new Failure(new RuntimeException(sprintf('Unknown method called: %s', $name)));
    }

    public function clusterNodes(): Promise
    {
        return $this->redis->query('CLUSTER', 'NODES');
    }

    /**
     * @return NodeInfo
     */
    public function getNodeInfo(): NodeInfo
    {
        return $this->nodeInfo;
    }

    /**
     * @param NodeInfo $nodeInfo
     */
    public function setNodeInfo(NodeInfo $nodeInfo): void
    {
        $this->nodeInfo = $nodeInfo;
    }

    /**
     * @return Node[]
     */
    public function getSlaveNodes(): array
    {
        return $this->slaveNodes;
    }

    /**
     * @param Node[] $slaveNodes
     */
    public function setSlaveNodes(array $slaveNodes): void
    {
        $this->slaveNodes = $slaveNodes;
    }
}