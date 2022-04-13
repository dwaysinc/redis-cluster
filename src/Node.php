<?php

namespace DwaysInc\RedisCluster;

use Amp\Failure;
use Amp\Promise;
use Amp\Redis\Redis;
use RuntimeException;

/**
 * @see Redis
 * @method set($key, $value, $options)
 * @method get($key)
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