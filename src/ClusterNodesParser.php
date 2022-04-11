<?php

namespace DwaysInc\RedisCluster;

class ClusterNodesParser
{
    /**
     * @param string $clusterNodesResponse
     * @return NodeInfo[]
     */
    public static function parse(string $clusterNodesResponse): array
    {
        $clusterNodes = [];
        $clusterNodesResponse = explode("\n", $clusterNodesResponse);

        foreach ($clusterNodesResponse as $clusterNodeInfo) {
            if (empty($clusterNodeInfo)) {
                continue;
            }

            $clusterNodes[] = NodeInfo::fromString($clusterNodeInfo);
        }

        return $clusterNodes;
    }
}