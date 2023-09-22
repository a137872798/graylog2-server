/*
 * Copyright (C) 2020 Graylog, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the Server Side Public License, version 1,
 * as published by MongoDB, Inc.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Server Side Public License for more details.
 *
 * You should have received a copy of the Server Side Public License
 * along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package org.graylog2.indexer.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import org.graylog2.indexer.cluster.health.ClusterAllocationDiskSettings;
import org.graylog2.indexer.cluster.health.NodeDiskUsageStats;
import org.graylog2.indexer.cluster.health.NodeFileDescriptorStats;
import org.graylog2.indexer.indices.HealthStatus;
import org.graylog2.rest.models.system.indexer.responses.ClusterHealth;
import org.graylog2.system.stats.elasticsearch.ClusterStats;
import org.graylog2.system.stats.elasticsearch.NodeInfo;
import org.graylog2.system.stats.elasticsearch.ShardStats;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 提供集群层面的api
 */
public interface ClusterAdapter {

    /**
     * 检查当前集群状态
     * @return
     */
    Optional<HealthStatus> health();

    /**
     * 获取节点上相关文件长度信息
     * @return
     */
    Set<NodeFileDescriptorStats> fileDescriptorStats();

    /**
     * 获取每个节点上磁盘的消耗情况
     * @return
     */
    Set<NodeDiskUsageStats> diskUsageStats();

    /**
     * 描述磁盘的分配情况  关联一个水位对象
     * @return
     */
    ClusterAllocationDiskSettings clusterAllocationDiskSettings();

    /**
     * 将节点id转换成name
     * @param nodeId
     * @return
     */
    Optional<String> nodeIdToName(String nodeId);

    /**
     * 将节点id 转换成主机名
     * @param nodeId
     * @return
     */
    Optional<String> nodeIdToHostName(String nodeId);

    /**
     * 判断能否正常访问到集群
     * @return
     */
    boolean isConnected();

    /**
     * 获取集群名字
     * @return
     */
    Optional<String> clusterName();

    /**
     * 查看当前集群健康信息
     * @return
     */
    Optional<ClusterHealth> clusterHealthStats();

    /**
     * 包含集群统计信息
     * @return
     */
    ClusterStats clusterStats();

    JsonNode rawClusterStats();

    /**
     * 获取有关待处理任务的统计信息
     * PendingTasksStats 中包含每个任务的等待时间
     * @return
     */
    PendingTasksStats pendingTasks();

    /**
     * 获取集群中节点信息
     * @return
     */
    Map<String, NodeInfo> nodesInfo();

    /**
     * 获取分片信息
     * @return
     */
    ShardStats shardStats();

    /**
     * 获取某些索引的健康信息
     * @param indices
     * @return
     */
    Optional<HealthStatus> deflectorHealth(Collection<String> indices);
}
