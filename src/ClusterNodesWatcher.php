<?php

namespace DwaysInc\RedisCluster;

use Amp\Loop;
use Amp\Promise;
use function Amp\call;

class ClusterNodesWatcher
{
    /** @var Node[] */
    private array $nodes;
    /** @var Node[] */
    private array $activeMasterNodes;
    /** @var Node[] */
    private array $activeSlaveNodes;
    private string $watchId;

    private int $reloadClusterNodesInterval = 5000;

    public function stop()
    {
        if (isset($this->watchId)) {
            Loop::cancel($this->watchId);
        }
    }

    public function reloadClusterNodes(): Promise
    {
        return call(function () {
            yield $this->clusterNodes();

            $this->initNodes();

            $this->watchId = Loop::delay($this->getReloadClusterNodesInterval(), function () {
                yield $this->reloadClusterNodes();
            });
        });
    }

    public function clusterNodes(): Promise
    {
        return call(function () {
            foreach ($this->getNodes() as $node) {
                $clusterNodes = ClusterNodesParser::parse(yield $node->clusterNodes());

                foreach ($clusterNodes as $clusterNode) {
                    if ($clusterNode->getIsSelf()) {
                        $node->setNodeInfo($clusterNode);

                        break;
                    }
                }
            }
        });
    }

    private function initNodes()
    {
        $masterNodes = [];
        $slaveNodes = [];

        foreach ($this->getNodes() as $node) {
            if ($node->getNodeInfo()->getIsMaster()) {
                $masterNodes[$node->getNodeInfo()->getId()] = $node;
            } elseif ($node->getNodeInfo()->getIsSlave()) {
                $slaveNodes[$node->getNodeInfo()->getId()] = $node;
            }
        }

        foreach ($slaveNodes as $slaveNode) {
            if (isset($masterNodes[$slaveNode->getNodeInfo()->getMaster()])) {
                $masterNode = $masterNodes[$slaveNode->getNodeInfo()->getMaster()];
                $currentSlaves = $masterNode->getSlaveNodes();
                $masterNode->setSlaveNodes(array_merge($currentSlaves, [$slaveNode->getNodeInfo()->getId() => $slaveNode]));
                $slaveNode->setMasterNode($masterNode);
            }
        }

        $this->setActiveMasterNodes($masterNodes);
        $this->setActiveSlaveNodes($slaveNodes);
    }

    /**
     * @return Node[]
     */
    public function getNodes(): array
    {
        return $this->nodes;
    }

    /**
     * @param Node[] $nodes
     */
    public function setNodes(array $nodes): void
    {
        $this->nodes = $nodes;
    }

    /**
     * @return Node[]
     */
    public function getActiveMasterNodes(): array
    {
        return $this->activeMasterNodes;
    }

    /**
     * @param Node[] $activeMasterNodes
     */
    public function setActiveMasterNodes(array $activeMasterNodes): void
    {
        $this->activeMasterNodes = $activeMasterNodes;
    }

    /**
     * @return Node[]
     */
    public function getActiveSlaveNodes(): array
    {
        return $this->activeSlaveNodes;
    }

    /**
     * @param Node[] $activeSlaveNodes
     */
    public function setActiveSlaveNodes(array $activeSlaveNodes): void
    {
        $this->activeSlaveNodes = $activeSlaveNodes;
    }

    /**
     * @return int
     */
    public function getReloadClusterNodesInterval(): int
    {
        return $this->reloadClusterNodesInterval;
    }

    /**
     * @param int $reloadClusterNodesInterval
     */
    public function setReloadClusterNodesInterval(int $reloadClusterNodesInterval): void
    {
        $this->reloadClusterNodesInterval = $reloadClusterNodesInterval;
    }
}