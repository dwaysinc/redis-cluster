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

                foreach ($clusterNodes as $clusterNode) {
                    if ($clusterNode->getIsSelf()) {
                        $node->setNodeInfo($clusterNode);

                        break;
                    }
                }
            }
        });
    }

    private function connect(): Promise
    {
        if (isset($this->connect)) {
            return $this->connect;
        }

        return $this->connect = call(function () {
            yield $this->clusterNodes();
            $this->initNodes();
        });
    }

    private function initNodes()
    {
        $masterNodes = [];
        $slaveNodes = [];

        foreach ($this->nodes as $node) {
            if ($node->getNodeInfo()->getIsMaster()) {
                $masterNodes[$node->getNodeInfo()->getId()] = $node;
            } elseif ($node->getNodeInfo()->getIsSlave()) {
                $slaveNodes[$node->getNodeInfo()->getId()] = $node;
            }
        }

        foreach ($slaveNodes as $node) {
            if (isset($masterNodes[$node->getNodeInfo()->getMaster()])) {
                $node->setSlaveNodes(array_merge($node->getSlaveNodes(), [$node->getNodeInfo()->getId() => $node]));
            }
        }

        $this->activeMasterNodes = $masterNodes;
        $this->activeSlaveNodes = $slaveNodes;
    }
}