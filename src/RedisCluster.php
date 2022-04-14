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
    private Promise $connect;
    private ?Logger $logger = null;
    private ClusterNodesWatcher $clusterNodesWatcher;

    /**
     * @param Redis ...$redisList
     */
    public function __construct(Redis ...$redisList)
    {
        $nodes = [];

        foreach ($redisList as $redis) {
            $nodes[] = new Node($redis);
        }

        $this->clusterNodesWatcher = new ClusterNodesWatcher();
        $this->clusterNodesWatcher->setNodes($nodes);
    }

    public function set(string $key, string $value, SetOptions $options = null, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'set', [$value, $options], $findHashSlotStrategyEnum);
    }

    public function get(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'get', [], $findHashSlotStrategyEnum);
    }

    private function connect(): Promise
    {
        if (isset($this->connect)) {
            return $this->connect;
        }

        return $this->connect = call(function () {
            yield $this->clusterNodesWatcher->reloadClusterNodes();
        });
    }

    private function getNodesByKey(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum): array
    {
        $hashSlot = Crc16::calculate($key) % 16384;

        $this->getLogger()?->debug(sprintf('Hash slot for key %s is %d', $key, $hashSlot));

        return $this->getNodesByHashSlot($hashSlot, $findHashSlotStrategyEnum);
    }

    private function getNodesByHashSlot(int $hashSlot, FindHashSlotStrategyEnum $findHashSlotStrategyEnum): array
    {
        $availableNodes = [];

        switch ($findHashSlotStrategyEnum) {
            case FindHashSlotStrategyEnum::MASTER_ONLY:
                foreach ($this->clusterNodesWatcher->getActiveMasterNodes() as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;

                        break;
                    }
                }

                break;
            case FindHashSlotStrategyEnum::SLAVE_ONLY:
                foreach ($this->clusterNodesWatcher->getActiveMasterNodes() as $node) {
                    if ($node->isValidHashSlot($hashSlot) && !empty($node->getSlaveNodes())) {
                        $availableNodes = array_merge($availableNodes, $node->getSlaveNodes());

                        break;
                    }
                }

                foreach ($this->clusterNodesWatcher->getActiveSlaveNodes() as $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes = array_merge($availableNodes, [$node]);
                    }
                }

                break;
            case FindHashSlotStrategyEnum::MASTER_FIRST:
                foreach ($this->clusterNodesWatcher->getActiveMasterNodes() as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                        $availableNodes = array_merge($availableNodes, $node->getSlaveNodes());

                        break;
                    }
                }

                foreach ($this->clusterNodesWatcher->getActiveSlaveNodes() as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                    }
                }

                break;
            case FindHashSlotStrategyEnum::SLAVE_FIRST:
                foreach ($this->clusterNodesWatcher->getActiveSlaveNodes() as $id => $node) {
                    if ($node->isValidHashSlot($hashSlot)) {
                        $availableNodes[$id] = $node;
                    }
                }

                foreach ($this->clusterNodesWatcher->getActiveMasterNodes() as $id => $node) {
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

    public function executeByKey(string $key, string $command, array $arguments = [], FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return call(function () use ($key, $command, $arguments, $findHashSlotStrategyEnum) {
            yield $this->connect();

            $nodes = $this->getNodesByKey($key, $findHashSlotStrategyEnum);

            try {
                return yield $this->executeOnNodes($nodes, $key, $command, $arguments);
            } catch (Throwable $e) {
                $this->getLogger()?->error($e->getMessage() . PHP_EOL . $e->getTraceAsString());
            }

            throw new RuntimeException(sprintf('Cannot execute "%s" command', $command));
        });
    }

    public function executeOnNodes(array $nodes, string $key, string $command, array $arguments): Promise
    {
        return call(function () use ($nodes, $command, $key, $arguments) {
            if (empty($nodes)) {
                throw new RuntimeException('Nodes not found');
            }

            $this->getLogger()?->debug(sprintf('Found nodes to execute: %s', implode(', ', array_keys($nodes))));

            foreach ($nodes as $id => $node) {
                try {
                    $this->getLogger()?->debug(sprintf('Execute command "%s" on node "%s"', $command, $id));

                    return yield $node->$command($key, ...$arguments);
                } catch (Throwable $e) {
                    $this->getLogger()?->error($e->getMessage() . PHP_EOL . $e->getTraceAsString());
                }
            }

            throw new RuntimeException(sprintf('Cannot execute "%s" command on nodes %s', $command, implode(', ', array_keys($nodes))));
        });
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

    /**
     * @return int
     */
    public function getReloadClusterNodesInterval(): int
    {
        return $this->clusterNodesWatcher->getReloadClusterNodesInterval();
    }

    /**
     * @param int $reloadClusterNodesInterval
     */
    public function setReloadClusterNodesInterval(int $reloadClusterNodesInterval): void
    {
        $this->clusterNodesWatcher->setReloadClusterNodesInterval($reloadClusterNodesInterval);
    }

    public function __destruct()
    {
        $this->clusterNodesWatcher->stop();
    }
}