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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.indexset.IndexSetService;
import org.graylog2.indexer.indexset.events.IndexSetCreatedEvent;
import org.graylog2.indexer.indexset.events.IndexSetDeletedEvent;
import org.graylog2.indexer.indices.TooManyAliasesException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * 通过mongodb来管理所有索引集
 */
@Singleton
public class MongoIndexSetRegistry implements IndexSetRegistry {

    /**
     * 通过该对象可以获取 IndexSetConfig
     */
    private final IndexSetService indexSetService;

    /**
     * 通过工厂可以创建索引集对象
     */
    private final MongoIndexSet.Factory mongoIndexSetFactory;

    /**
     * 索引集缓存
     */
    static class IndexSetsCache {

        /**
         * 通过该对象可以拿到索引集配置
         */
        private final IndexSetService indexSetService;
        /**
         * 通过该对象可以获取一组索引集配置    调用supplier会间接调用 findAll
         */
        private AtomicReference<Supplier<List<IndexSetConfig>>> indexSetConfigs;

        @Inject
        IndexSetsCache(IndexSetService indexSetService,
                       EventBus serverEventBus) {
            this.indexSetService = requireNonNull(indexSetService);
            this.indexSetConfigs = new AtomicReference<>(Suppliers.memoize(this.indexSetService::findAll));
            serverEventBus.register(this);
        }

        List<IndexSetConfig> get() {
            return Collections.unmodifiableList(indexSetConfigs.get().get());
        }

        @VisibleForTesting
        void invalidate() {
            this.indexSetConfigs.set(Suppliers.memoize(this.indexSetService::findAll));
        }

        // 感知到事件后 刷新索引
        @Subscribe
        void handleIndexSetCreation(IndexSetCreatedEvent indexSetCreatedEvent) {
            this.invalidate();
        }

        @Subscribe
        void handleIndexSetDeletion(IndexSetDeletedEvent indexSetDeletedEvent) {
            this.invalidate();
        }
    }

    /**
     * 索引集缓存对象
     */
    private final IndexSetsCache indexSetsCache;

    @Inject
    public MongoIndexSetRegistry(IndexSetService indexSetService,
                                 MongoIndexSet.Factory mongoIndexSetFactory,
                                 IndexSetsCache indexSetsCache) {
        this.indexSetService = indexSetService;
        this.mongoIndexSetFactory = requireNonNull(mongoIndexSetFactory);
        this.indexSetsCache = indexSetsCache;
    }

    /**
     * 从缓存中加载所有索引集配置 并挨个创建索引集
     * @return
     */
    private Set<MongoIndexSet> findAllMongoIndexSets() {
        final List<IndexSetConfig> configs = this.indexSetsCache.get();
        final ImmutableSet.Builder<MongoIndexSet> mongoIndexSets = ImmutableSet.builder();
        for (IndexSetConfig config : configs) {
            // 根据配置构建索引集
            final MongoIndexSet mongoIndexSet = mongoIndexSetFactory.create(config);
            mongoIndexSets.add(mongoIndexSet);
        }
        return mongoIndexSets.build();
    }
    @Override
    public Set<IndexSet> getAll() {
        return ImmutableSet.copyOf(findAllMongoIndexSets());
    }

    /**
     * 通过id 找到对应的索引集配置 并生成索引集
     * @param indexSetId ID of the index set
     * @return
     */
    @Override
    public Optional<IndexSet> get(final String indexSetId) {
        return this.indexSetsCache.get()
                .stream()
                .filter(indexSet -> Objects.equals(indexSet.id(), indexSetId))
                .map(indexSetConfig -> (IndexSet) mongoIndexSetFactory.create(indexSetConfig))
                .findFirst();
    }

    /**
     * 通过索引名称 反查索引集
     * @param indexName name of the index
     * @return
     */
    @Override
    public Optional<IndexSet> getForIndex(String indexName) {
        return findAllMongoIndexSets()
            .stream()
            .filter(indexSet -> indexSet.isManagedIndex(indexName))
            .map(indexSet -> (IndexSet)indexSet)
            .findFirst();
    }

    @Override
    public Set<IndexSet> getForIndices(Collection<String> indices) {
        // 获取所有索引集
        final Set<? extends IndexSet> indexSets = findAllMongoIndexSets();
        final ImmutableSet.Builder<IndexSet> resultBuilder = ImmutableSet.builder();
        // 挨个匹配索引名 将匹配的返回
        for (IndexSet indexSet : indexSets) {
            for (String index : indices) {
                if (indexSet.isManagedIndex(index)) {
                    resultBuilder.add(indexSet);
                }
            }
        }

        return resultBuilder.build();
    }

    /**
     * 通过配置创建索引集
     * @param indexSetConfigs Collection of index configurations
     * @return
     */
    @Override
    public Set<IndexSet> getFromIndexConfig(Collection<IndexSetConfig> indexSetConfigs) {
        final ImmutableSet.Builder<MongoIndexSet> mongoIndexSets = ImmutableSet.builder();
        for (IndexSetConfig config : indexSetConfigs) {
            final MongoIndexSet mongoIndexSet = mongoIndexSetFactory.create(config);
            mongoIndexSets.add(mongoIndexSet);
        }
        return ImmutableSet.copyOf(mongoIndexSets.build());
    }

    @Override
    public IndexSet getDefault() {
        return mongoIndexSetFactory.create(indexSetService.getDefault());
    }

    /**
     * 把每个索引集管理的每个索引 加入到列表中
     * @return
     */
    @Override
    public String[] getManagedIndices() {
        final ImmutableSet.Builder<String> indexNamesBuilder = ImmutableSet.builder();
        for (MongoIndexSet indexSet : findAllMongoIndexSets()) {
            indexNamesBuilder.add(indexSet.getManagedIndices());
        }

        final ImmutableSet<String> indexNames = indexNamesBuilder.build();
        return indexNames.toArray(new String[0]);
    }

    @Override
    public boolean isManagedIndex(String indexName) {
        return isManagedIndex(findAllMongoIndexSets(), indexName);
    }

    @Override
    public Map<String, Boolean> isManagedIndex(Collection<String> indices) {
        final Set<MongoIndexSet> indexSets = findAllMongoIndexSets();
        return indices.stream()
                .collect(Collectors.toMap(Function.identity(), index -> isManagedIndex(indexSets, index)));
    }

    private boolean isManagedIndex(Collection<? extends IndexSet> indexSets, String index) {
        for (IndexSet indexSet : indexSets) {
            if (indexSet.isManagedIndex(index)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 返回所有管理的索引集的通配符
     * @return
     */
    @Override
    public String[] getIndexWildcards() {
        final ImmutableSet.Builder<String> wildcardsBuilder = ImmutableSet.builder();
        for (MongoIndexSet indexSet : findAllMongoIndexSets()) {
            if (indexSet.getConfig().isWritable()) {
                wildcardsBuilder.add(indexSet.getIndexWildcard());
            }
        }

        final ImmutableSet<String> wildcards = wildcardsBuilder.build();
        return wildcards.toArray(new String[0]);
    }

    /**
     * 返回所有写入用的索引
     * @return
     */
    @Override
    public String[] getWriteIndexAliases() {
        final ImmutableSet.Builder<String> indexNamesBuilder = ImmutableSet.builder();
        for (MongoIndexSet indexSet : findAllMongoIndexSets()) {
            if (indexSet.getConfig().isWritable()) {
                indexNamesBuilder.add(indexSet.getWriteIndexAlias());
            }
        }

        final ImmutableSet<String> indexNames = indexNamesBuilder.build();
        return indexNames.toArray(new String[0]);
    }

    @Override
    public boolean isUp() {
        return findAllMongoIndexSets().stream()
            .filter(indexSet -> indexSet.getConfig().isWritable())
            .allMatch(MongoIndexSet::isUp);
    }

    /**
     * 判断传入的是否是可写索引
     * @param indexName the name of the index to check
     * @return
     */
    @Override
    public boolean isCurrentWriteIndexAlias(String indexName) {
        for (MongoIndexSet indexSet : findAllMongoIndexSets()) {
            if (indexSet.isWriteIndexAlias(indexName)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean isCurrentWriteIndex(String indexName) throws TooManyAliasesException {
        for (MongoIndexSet indexSet : findAllMongoIndexSets()) {
            if (indexSet.getActiveWriteIndex() != null && indexSet.getActiveWriteIndex().equals(indexName)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Iterator<IndexSet> iterator() {
        return getAll().iterator();
    }
}
