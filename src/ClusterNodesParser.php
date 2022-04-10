<?php

namespace DwaysInc\RedisCluster;

class ClusterNodesParser
{
    /**
     * @param string $clusterNodesResponse
     * @return ClusterNode[]
     */
    public static function parse(string $clusterNodesResponse): array
    {
        $clusterNodes = [];
        $clusterNodesResponse = explode("\n", $clusterNodesResponse);

        foreach ($clusterNodesResponse as $clusterNodeInfo) {
            if (empty($clusterNodeInfo)) {
                continue;
            }

            $clusterNodes[] = ClusterNode::fromString($clusterNodeInfo);
        }

        var_dump($clusterNodes);die;

        return $clusterNodes;
    }
}