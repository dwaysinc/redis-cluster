<?php

namespace DwaysInc\RedisCluster;

use Amp\Promise;
use Amp\Redis\Redis;

/**
 * @see Redis
 */
final class Node
{
    private Redis $redis;
    private NodeInfo $nodeInfo;

    /** @var Node[] */
    private array $slaveNodes = [];
    private ?Node $masterNode = null;

    public function __construct(Redis $redis)
    {
        $this->redis = $redis;
    }

    /**
     * @return Redis
     */
    public function getRedis(): Redis
    {
        return $this->redis;
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

    /**
     * @return Node|null
     */
    public function getMasterNode(): ?Node
    {
        return $this->masterNode;
    }

    /**
     * @param Node|null $masterNode
     */
    public function setMasterNode(?Node $masterNode): void
    {
        $this->masterNode = $masterNode;
    }

    public function isValidHashSlot(int $hashSlot): bool
    {
        foreach ($this->getNodeInfo()->getSlot() as $slot) {
            $slot = explode('-', $slot);

            if ($slot[0] <= $hashSlot && $slot[1] >= $hashSlot) {
                return true;
            }
        }

        return false;
    }
}