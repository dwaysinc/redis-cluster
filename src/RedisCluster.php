<?php

namespace DwaysInc\RedisCluster;

use Amp\Failure;
use Amp\Promise;
use Amp\Redis\Redis;
use Amp\Redis\RedisList;
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

    public function getList(string $key): RedisList
    {
        throw new RuntimeException(sprintf('Method %s not supported!', 'getList'));
    }

    public function lindex(string $key, int $index, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lindex', $key, $index]);
    }

    public function linsert(string $key, ListInsertPositionEnum $listInsertPositionEnum, $pivot, $value, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['linsert', $key, $listInsertPositionEnum->name, $pivot, $value]);
    }

    public function llen(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['llen', $key]);
    }

    public function lpush(string $key, array $values, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lpush', $key, ...$values]);
    }

    public function lpushx(string $key, array $values, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lpushx', $key, ...$values]);
    }

    public function rpush(string $key, array $values, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['rpush', $key, ...$values]);
    }

    public function rpushx(string $key, array $values, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['rpushx', $key, ...$values]);
    }

    public function lpop(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lpop', $key]);
    }

    public function rpop(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['rpop', $key]);
    }

    public function blpop(string $key, int $timeout = 0, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return call(function () use ($key, $timeout, $findHashSlotStrategyEnum) {
            $response = yield $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['blpop', $key, $timeout]);

            return $response[1] ?? null;
        });
    }

    public function brpop(string $key, int $timeout = 0, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return call(function () use ($key, $timeout, $findHashSlotStrategyEnum) {
            $response = yield $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['brpop', $key, $timeout]);

            return $response[1] ?? null;
        });
    }

    public function lrange($key, int $start = 0, int $end = -1, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lrange', $key, $start, $end]);
    }

    public function lrem(string $key, string $value, int $count = 0, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lrem', $key, $count, $value]);
    }

    public function lset(string $key, int $index, string $value, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['lset', $key, $index, $value]);
    }

    public function ltrim(string $key, int $start = 0, int $stop = -1, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'query', $findHashSlotStrategyEnum, ['ltrim', $key, $start, $stop]);
    }

    public function set(string $key, string $value, SetOptions $options = null, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'set', $findHashSlotStrategyEnum, [$key, $value, $options]);
    }

    public function get(string $key, FindHashSlotStrategyEnum $findHashSlotStrategyEnum = FindHashSlotStrategyEnum::MASTER_FIRST): Promise
    {
        return $this->executeByKey($key, 'get', $findHashSlotStrategyEnum, [$key, $key]);
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

    public function executeByKey(string $key, string $command, FindHashSlotStrategyEnum $findHashSlotStrategyEnum, array $arguments = []): Promise
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

    /**
     * @param Node[] $nodes
     * @param string $key
     * @param string $command
     * @param array $arguments
     *
     * @return Promise
     */
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

                    return yield $node->getRedis()->$command(...$arguments);
                } catch (Throwable $e) {
                    $this->getLogger()?->error($e->getMessage() . PHP_EOL . $e->getTraceAsString());

                    $message = explode(' ', $e->getMessage());

                    if ($message[0] === 'MOVED') {
                        $this->getLogger()?->warning($e->getMessage() . ' : ' . $key . ' : ' . $command);

                        $node = $this->getNodeByAddress($message[2]);

                        return yield $this->executeOnNodes([$node], $key, $command, $arguments);
                    }
                }
            }

            throw new RuntimeException(sprintf('Cannot execute "%s" command on nodes %s', $command, implode(', ', array_keys($nodes))));
        });
    }

    private function getNodeByAddress(string $address): Node
    {
        foreach ($this->clusterNodesWatcher->getNodes() as $node) {
            if ($node->getNodeInfo()->getAddress() === $address) {
                return $node;
            }
        }

        throw new RuntimeException(sprintf('Node by address %s not found', $address));
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