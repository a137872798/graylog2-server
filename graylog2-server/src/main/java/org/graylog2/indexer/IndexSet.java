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
package org.graylog2.indexer;

import com.google.common.collect.ComparisonChain;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.indices.TooManyAliasesException;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * 索引集对象   索引集配置提示了如何使用索引
 */
public interface IndexSet extends Comparable<IndexSet> {
    /**
     * Returns an array with all managed indices in this index set.
     * <p>
     * Example: {@code ["graylog_0", "graylog_1", "graylog_2"]}
     *
     * @return array of index names
     * 获取索引集下的所有索引
     */
    String[] getManagedIndices();

    /**
     * Returns the write index alias name for this index set.
     * <p>
     * The write index alias always points to the newest index.
     * <p>
     * Example: {@code "graylog_deflector"}
     *
     * @return the write index alias name
     * 获取写入索引   写入索引总是指向最新的索引
     */
    String getWriteIndexAlias();

    /**
     * Returns the index wildcard for this index set.
     * <p>
     * This can be used in Elasticsearch queries to match all managed indices in this index set.
     * <p>
     * Example: {@code "graylog_*"}
     *
     * @return the index wildcard
     * 返回索引通配符
     */
    String getIndexWildcard();

    /**
     * Returns the newest index.
     * <p>
     * Example: {@code "graylog_42"}
     *
     * @return the newest index
     * @throws NoTargetIndexException if there are no indices in this index set yet
     * 获取最新的索引
     */
    String getNewestIndex() throws NoTargetIndexException;

    /**
     * Returns the active write index.
     * <p>
     * Incoming messages for this index set will be written into this index.
     * <p>
     * Example: {@code "graylog_42"}
     *
     * @return the active write index
     * @throws TooManyAliasesException if the write index alias points to more than one index
     * 获取活跃的写入索引
     */
    @Nullable
    String getActiveWriteIndex() throws TooManyAliasesException;

    /**
     * Returns a map where the key is an index name and the value a set of aliases for this index.
     * <p>
     * Only the active write index should have an alias, the other values should be empty.
     * <p>
     * Example: {@code {graylog_0=[], graylog_1=[], graylog_2=[graylog_deflector}}
     *
     * @return map of index names to index aliases
     * 获取索引集中每个索引的别名
     */
    Map<String, Set<String>> getAllIndexAliases();

    /**
     * Returns the index prefix for this index set.
     * <p>
     * Example: {@code "graylog"}
     *
     * @return index prefix for this index set
     */
    String getIndexPrefix();

    /**
     * Checks if the write index alias exists.
     *
     * @return true if the write index alias exists, false if not
     * 判断写入索引别名是否存在
     */
    boolean isUp();

    /**
     * Checks if the given index name is equals to the write index alias.
     *
     * @param index index name to check
     * @return true if given index name is the write index alias, false if not
     * 检查传入的是否是写入索引名
     */
    boolean isWriteIndexAlias(String index);

    /**
     * Checks if the given index name is part of this index set.
     *
     * @param index index name to check
     * @return true if part of index set, false if not
     */
    boolean isManagedIndex(String index);

    /**
     * Prepares this index set to receive new messages.
     * 准备好索引集去接收新消息  也就是说stream，索引集 都要提前准备好
     */
    void setUp();

    /**
     * Creates a new index and points the write index alias to it.
     * 创建新索引 并将索引别名指向它
     */
    void cycle();

    /**
     * This ensures that the write index alias only points to the newest index.
     * <p>
     * Can be used to fix the aliases in this index set when a {@link TooManyAliasesException} has been thrown.
     *
     * @param indices list of indices where the index alias points to
     *                确保别名仅指向最新索引
     */
    void cleanupAliases(Set<String> indices);

    /**
     * Changes the write index alias from the old index to the new one.
     *
     * @param newIndexName index to add the write index alias to
     * @param oldIndexName index to remove the write index alias from
     */
    void pointTo(String newIndexName, String oldIndexName);

    /**
     * Extracts the index number from an index name.
     * <p>
     * Example: {@code "graylog_42" => 42}
     *
     * @param index index name
     * @return a filled {@link Optional} with the extracted index number, an empty one if the number couldn't be parsed
     * 从索引名中抽取出编码
     */
    Optional<Integer> extractIndexNumber(String index);

    /**
     * The configuration for this index set.
     *
     * @return index set configuration object
     */
    IndexSetConfig getConfig();

    class IndexNameComparator implements Comparator<String> {
        private final IndexSet indexSet;

        IndexNameComparator(IndexSet indexSet) {
            this.indexSet = requireNonNull(indexSet);
        }

        @Override
        public int compare(String o1, String o2) {
            final int indexNumber1 = indexSet.extractIndexNumber(o1).orElse(-1);
            final int indexNumber2 = indexSet.extractIndexNumber(o2).orElse(-1);
            return ComparisonChain.start()
                    .compare(indexNumber1, indexNumber2)
                    .result();
        }
    }
}
