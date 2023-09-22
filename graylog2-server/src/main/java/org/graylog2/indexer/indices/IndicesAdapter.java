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
package org.graylog2.indexer.indices;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.joschi.jadconfig.util.Duration;
import org.graylog2.indexer.indices.blocks.IndicesBlockStatus;
import org.graylog2.indexer.indices.stats.IndexStatistics;
import org.graylog2.indexer.searches.IndexRangeStats;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 暴露索引相关的api
 */
public interface IndicesAdapter {

    /**
     * 将索引从source移动到target    使用resultCallback处理结果
     * @param source
     * @param target
     * @param resultCallback
     */
    void move(String source, String target, Consumer<IndexMoveResult> resultCallback);

    /**
     * 删除某个索引
     * @param indexName
     */
    void delete(String indexName);

    /**
     * 返回使用该别名的一组索引  多个索引是可以使用同一个别名的  实际上业务中只会使用一个
     * @param alias
     * @return
     */
    Set<String> resolveAlias(String alias);

    /**
     * 创建索引
     * @param indexName
     * @param indexSettings  包含副本数和分片数信息
     */
    void create(String indexName, IndexSettings indexSettings);

    /**
     * Add fields to an existing index or to change search only settings of existing fields
     * @param indexName existing index name
     * @param mappingType target mapping type (e.g. message). Not relevant for ES7+ (will be simply ignored).
     * @param mapping field mappings
     *
     *                mapping 相当于 db的schema
     */
    void updateIndexMapping(@Nonnull String indexName, @Nonnull String mappingType, @Nonnull Map<String, Object> mapping);

    /**
     * Updates the metadata field (_meta) of an index mapping
     * @param indexName existing index name
     * @param metaData  the new metadata
     * @param mergeExisting merge or overwrite existing metadata
     *                      更新索引元数据
     */
    void updateIndexMetaData(@Nonnull String indexName, @Nonnull Map<String, Object> metaData, boolean mergeExisting);

    /**
     * 获取索引元数据
     * @param indexName
     * @return
     */
    Map<String, Object> getIndexMetaData(@Nonnull String indexName);

    /**
     * 创建索引模板
     * @param templateName
     * @param template
     * @return
     */
    boolean ensureIndexTemplate(String templateName, Map<String, Object> template);

    /**
     * 检测模板是否存在   索引模板的作用就是会自动将settings和mapping作用到之后创建且命中匹配的index上
     * @param templateName
     * @return
     */
    boolean indexTemplateExists(String templateName);

    Optional<DateTime> indexCreationDate(String index);

    Optional<DateTime> indexClosingDate(String index);

    /**
     * 开启索引  开启关闭就是决定索引能否使用
     * @param index
     */
    void openIndex(String index);

    /**
     * 将索引设置为只可读取 不可写入
     * @param index
     */
    void setReadOnly(String index);

    void flush(String index);

    /**
     * 将索引标记成 reopened状态
     * @param index
     */
    void markIndexReopened(String index);

    /**
     * 移除索引的某个别名
     * @param indexName
     * @param alias
     */
    void removeAlias(String indexName, String alias);

    /**
     * 关闭索引
     * @param indexName
     */
    void close(String indexName);

    /**
     * 该索引下有多少记录
     * @param indexName
     * @return
     */
    long numberOfMessages(String indexName);

    /**
     * 检查是否存在某个别名
     * @param alias
     * @return
     * @throws IOException
     */
    boolean aliasExists(String alias) throws IOException;

    /**
     * 查询某组索引的别名
     * @param indexPattern
     * @return
     */
    Map<String, Set<String>> aliases(String indexPattern);

    /**
     * 删除某个索引模板
     * @param templateName
     * @return
     */
    boolean deleteIndexTemplate(String templateName);

    /**
     * 查询这组索引的字段
     * @param writeIndexWildcards
     * @return
     */
    Map<String, Set<String>> fieldsInIndices(String[] writeIndexWildcards);

    /**
     * 关闭一组索引
     * @param indices
     * @return
     */
    Set<String> closedIndices(Collection<String> indices);

    /**
     * 获取索引的统计信息
     * @param indices
     * @return
     */
    Set<IndexStatistics> indicesStats(Collection<String> indices);

    Optional<IndexStatistics> getIndexStats(String index);

    /**
     * 将索引统计数据 作为json来看待
     * @param index
     * @return
     */
    JsonNode getIndexStats(Collection<String> index);

    /**
     * 查看这组索引的状态
     * @param indices
     * @return
     */
    IndicesBlockStatus getIndicesBlocksStatus(List<String> indices);

    /**
     * 检查某个索引是否存在
     * @param indexName
     * @return
     * @throws IOException
     */
    boolean exists(String indexName) throws IOException;

    /**
     * 根据这些条件查询索引名
     * @param indexWildcard
     * @param status
     * @param id
     * @return
     */
    Set<String> indices(String indexWildcard, List<String> status, String id);

    /**
     * 该索引下存储的数据总大小
     * @param index
     * @return
     */
    Optional<Long> storeSizeInBytes(String index);

    /**
     * 应该是要把别名绑在新的index上
     * @param aliasName
     * @param targetIndex
     */
    void cycleAlias(String aliasName, String targetIndex);

    void cycleAlias(String aliasName, String targetIndex, String oldIndex);

    /**
     * 从这些索引上移除掉该别名
     */
    void removeAliases(Set<String> indices, String alias);

    /**
     * 代表对该index的多个segment进行合并
     * @param index
     * @param maxNumSegments
     * @param timeout
     */
    void optimizeIndex(String index, int maxNumSegments, Duration timeout);

    /**
     * 获取该索引多个range的统计数据
     * @param index
     * @return
     */
    IndexRangeStats indexRangeStatsOfIndex(String index);

    HealthStatus waitForRecovery(String index);
    HealthStatus waitForRecovery(String index, int timeout);

    boolean isOpen(String index);

    boolean isClosed(String index);

    String getIndexId(String index);

    void refresh(String... indices);
}
