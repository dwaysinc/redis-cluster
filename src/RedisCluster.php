<?php

namespace DwaysInc\RedisCluster;

use Amp\Promise;
use Amp\Redis\Redis;
use Amp\Redis\SetOptions;
use Monolog\Logger;
use RuntimeException;
use Throwable;
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
    private ?Logger $logger = null;

    /**
     * @param Redis ...$redisList
     */
    public function __construct(Redis ...$redisList)
    {
        foreach ($redisList as $redis) {
            $this->nodes[] = new Node($redis);
        }
    }

    public function set(string $key, string $value, SetOptions $options = null, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return call(function () use ($key, $findHashSlotStrategyEnum, $value, $options) {
            yield $this->connect();

            // todo: refactor
            $nodes = $this->getNodesByKey($key, $findHashSlotStrategyEnum);

            if (empty($nodes)) {
                throw new RuntimeException('Nodes not found');
            }

            $this->getLogger()?->debug(sprintf('Found nodes to execute: %s', print_r(array_keys($nodes), true)));

            foreach ($nodes as $id => $node) {
                try {
                    $this->getLogger()?->debug(sprintf('Trying to execute set on node %s', $id));

                    return yield $node->set($key, $value, $options);
                } catch (Throwable $e) {
                    $this->getLogger()?->error($e->getMessage() . PHP_EOL . $e->getTraceAsString());
                }
            }

            throw new RuntimeException('Cannot execute set command');
        });
    }

    public function get(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return call(function () use ($key, $findHashSlotStrategyEnum) {
            yield $this->connect();

            // todo: refactor
            $nodes = $this->getNodesByKey($key, $findHashSlotStrategyEnum);

            if (empty($nodes)) {
                throw new RuntimeException('Nodes not found');
            }

            $this->getLogger()?->debug(sprintf('Found nodes to execute: %s', print_r(array_keys($nodes), true)));

            foreach ($nodes as $id => $node) {
                try {
                    $this->getLogger()?->debug(sprintf('Trying to execute set on node %s', $id));

                    return yield $node->get($key);
                } catch (Throwable $e) {
                    $this->getLogger()?->error($e->getMessage() . PHP_EOL . $e->getTraceAsString());
                }
            }

            throw new RuntimeException('Cannot execute set command');
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

        foreach ($slaveNodes as $slaveNode) {
            if (isset($masterNodes[$slaveNode->getNodeInfo()->getMaster()])) {
                $masterNode = $masterNodes[$slaveNode->getNodeInfo()->getMaster()];
                $currentSlaves = $masterNode->getSlaveNodes();
                $masterNode->setSlaveNodes(array_merge($currentSlaves, [$slaveNode->getNodeInfo()->getId() => $slaveNode]));
                $slaveNode->setMasterNode($masterNode);
            }
        }

        $this->activeMasterNodes = $masterNodes;
        $this->activeSlaveNodes = $slaveNodes;
    }

    /**
     * @param string $key
     * @param FindHashSlotStrategyEnum $findHashSlotStrategyEnum
     *
     * @return Node[]
     */
    private function getNodesByKey(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum): array
    {
        $hashSlot = Crc16::calculate($key) % 16384;

        $this->getLogger()?->debug(sprintf('Hash slot for key %s is %d', $key, $hashSlot));

        return $this->getNodesByHashSlot($hashSlot, $findHashSlotStrategyEnum);
    }

    /**
     * @param int $hashSlot
     * @param FindHashSlotStrategyEnum $findHashSlotStrategyEnum
     *
     * @return Node[]
     */
    private function getNodesByHashSlot(int $hashSlot, FindHashSlotStrategyEnum $findHashSlotStrategyEnum): array
    {
        $availableNodes = [];

        switch ($findHashSlotStrategyEnum) {
            case FindHashSlotStrategyEnum::MASTER_ONLY:
                foreach ($this->activeMasterNodes as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;

                        break;
                    }
                }

                break;
            case FindHashSlotStrategyEnum::SLAVE_ONLY:
                foreach ($this->activeMasterNodes as $node) {
                    if ($node->isValidHashSlot($hashSlot) && !empty($node->getSlaveNodes())) {
                        $availableNodes = array_merge($availableNodes, $node->getSlaveNodes());

                        break;
                    }
                }

                foreach ($this->activeSlaveNodes as $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes = array_merge($availableNodes, [$node]);
                    }
                }

                break;
            case FindHashSlotStrategyEnum::MASTER_FIRST:
                foreach ($this->activeMasterNodes as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                        $availableNodes = array_merge($availableNodes, $node->getSlaveNodes());

                        break;
                    }
                }

                foreach ($this->activeSlaveNodes as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                    }
                }

                break;
            case FindHashSlotStrategyEnum::SLAVE_FIRST:
                foreach ($this->activeSlaveNodes as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                    }
                }

                foreach ($this->activeMasterNodes as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes = array_merge($availableNodes, $node->getSlaveNodes());
                        $availableNodes[$id] = $node;

                        break;
                    }
                }

                break;
            default:
                throw new RuntimeException(sprintf('Unexpected FindHashSlotStrategyEnum given %s', $findHashSlotStrategyEnum->name));
        }

        return $availableNodes;
    }

    /**
     * @return Logger|null
     */
    public function getLogger(): ?Logger
    {
        return $this->logger;
    }

    /**
     * @param Logger|null $logger
     */
    public function setLogger(?Logger $logger): void
    {
        $this->logger = $logger;
    }
}