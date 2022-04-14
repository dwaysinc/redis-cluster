<?php

namespace DwaysInc\RedisCluster;

enum FindHashSlotStrategyEnum
{
    case MASTER_ONLY;
    case SLAVE_ONLY;
    case MASTER_FIRST;
    case SLAVE_FIRST;
}