<?php

namespace DwaysInc\RedisCluster;

class ClusterNode
{
    /**
     * The node ID, a 40 characters random string generated when a node is created and never changed
     * again (unless CLUSTER RESET HARD is used).
     *
     * @var string
     */
    private string $id;

    /**
     * ip:port@cport: The node address where clients should contact the node to run queries.
     *
     * @var string
     */
    private string $address;

    /**
     * A list of flags: myself, master, slave, fail?, fail, handshake, noaddr, nofailover, noflags.
     * Flags are explained in detail in the next section.
     *
     * @var array
     */
    private array $flags;

    /**
     * If the node is a replica, and the master is known, the master node ID, otherwise the "-" character.
     *
     * @var string
     */
    private string $master;

    /**
     * Milliseconds unix time at which the currently active ping was sent, or zero if there are no pending pings.
     *
     * @var int
     */
    private int $pingSent;

    /**
     * Milliseconds unix time the last pong was received.
     *
     * @var int
     */
    private int $pongRecv;

    /**
     * The configuration epoch (or version) of the current node (or of the current master if the node is a replica).
     * Each time there is a failover, a new, unique, monotonically increasing configuration epoch is created.
     * If multiple nodes claim to serve the same hash slots, the one with higher configuration epoch wins.
     *
     * @var int
     */
    private int $configEpoch;

    /**
     * The state of the link used for the node-to-node cluster bus. We use this link to communicate with the node.
     * Can be connected or disconnected.
     *
     * @var string
     */
    private string $linkState;

    /**
     * A hash slot number or range. Starting from argument number 9, but there may be up to 16384 entries in total
     * (limit never reached). This is the list of hash slots served by this node. If the entry is just a number, is
     * parsed as such. If it is a range, it is in the form start-end, and means that the node is responsible for all the
     * hash slots from start to end including the start and end values.
     *
     * @var array
     */
    private array $slot;

    public static function fromString(string $clusterNodeInfo)
    {
        $clusterNodeInfo = str_getcsv($clusterNodeInfo, ' ');

        $clusterNode = new ClusterNode();
        $clusterNode->setId($clusterNodeInfo[0]);
        $clusterNode->setAddress($clusterNodeInfo[1]);

        $flags = explode(',', $clusterNodeInfo[2]);
        $clusterNode->setFlags(array_combine($flags, $flags));

        $clusterNode->setMaster($clusterNodeInfo[3]);
        $clusterNode->setPingSent($clusterNodeInfo[4]);
        $clusterNode->setPongRecv($clusterNodeInfo[5]);
        $clusterNode->setConfigEpoch($clusterNodeInfo[6]);
        $clusterNode->setLinkState($clusterNodeInfo[7]);

        $slots = [];

        for ($i = 8; $i < count($clusterNodeInfo); $i++) {
            $slots[] = $clusterNodeInfo[$i];
        }

        $clusterNode->setSlot($slots);

        return $clusterNode;
    }

    /**
     * @return string
     */
    public function getId(): string
    {
        return $this->id;
    }

    /**
     * @param string $id
     */
    public function setId(string $id): void
    {
        $this->id = $id;
    }

    /**
     * @return string
     */
    public function getAddress(): string
    {
        return $this->address;
    }

    /**
     * @param string $address
     */
    public function setAddress(string $address): void
    {
        $this->address = $address;
    }

    /**
     * @return array
     */
    public function getFlags(): array
    {
        return $this->flags;
    }

    /**
     * @param array $flags
     */
    public function setFlags(array $flags): void
    {
        $this->flags = $flags;
    }

    /**
     * @return string
     */
    public function getMaster(): string
    {
        return $this->master;
    }

    /**
     * @param string $master
     */
    public function setMaster(string $master): void
    {
        $this->master = $master;
    }

    /**
     * @return int
     */
    public function getPingSent(): int
    {
        return $this->pingSent;
    }

    /**
     * @param int $pingSent
     */
    public function setPingSent(int $pingSent): void
    {
        $this->pingSent = $pingSent;
    }

    /**
     * @return int
     */
    public function getPongRecv(): int
    {
        return $this->pongRecv;
    }

    /**
     * @param int $pongRecv
     */
    public function setPongRecv(int $pongRecv): void
    {
        $this->pongRecv = $pongRecv;
    }

    /**
     * @return int
     */
    public function getConfigEpoch(): int
    {
        return $this->configEpoch;
    }

    /**
     * @param int $configEpoch
     */
    public function setConfigEpoch(int $configEpoch): void
    {
        $this->configEpoch = $configEpoch;
    }

    /**
     * @return string
     */
    public function getLinkState(): string
    {
        return $this->linkState;
    }

    /**
     * @param string $linkState
     */
    public function setLinkState(string $linkState): void
    {
        $this->linkState = $linkState;
    }

    /**
     * @return array
     */
    public function getSlot(): array
    {
        return $this->slot;
    }

    /**
     * @param array $slot
     */
    public function setSlot(array $slot): void
    {
        $this->slot = $slot;
    }
}