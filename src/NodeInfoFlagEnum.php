<?php

namespace DwaysInc\RedisCluster;

enum NodeInfoFlagEnum
{
    case myself;
    case master;
    case slave;
}