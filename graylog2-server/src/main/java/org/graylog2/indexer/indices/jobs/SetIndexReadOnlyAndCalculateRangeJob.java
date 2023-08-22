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
package org.graylog2.indexer.indices.jobs;

import com.google.inject.assistedinject.Assisted;
import org.graylog2.indexer.IndexSetRegistry;
import org.graylog2.indexer.SetIndexReadOnlyJob;
import org.graylog2.indexer.fieldtypes.IndexFieldTypePoller;
import org.graylog2.indexer.fieldtypes.IndexFieldTypesService;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.ranges.CreateNewSingleIndexRangeJob;
import org.graylog2.system.jobs.SystemJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * 用于将索引设置成只读 并且自动计算索引range的后台任务
 */
public class SetIndexReadOnlyAndCalculateRangeJob extends SystemJob {
    private static final Logger LOG = LoggerFactory.getLogger(SetIndexReadOnlyAndCalculateRangeJob.class);

    public interface Factory {
        SetIndexReadOnlyAndCalculateRangeJob create(String indexName);
    }

    /**
     * 该对象负责将索引修改成只读
     */
    private final SetIndexReadOnlyJob.Factory setIndexReadOnlyJobFactory;
    /**
     * 产生range对象
     */
    private final CreateNewSingleIndexRangeJob.Factory createNewSingleIndexRangeJobFactory;
    private final IndexSetRegistry indexSetRegistry;
    private final Indices indices;
    /**
     * 将索引字段信息存储在mongodb中
     */
    private final IndexFieldTypesService indexFieldTypesService;

    /**
     * 该服务可以拉取索引的字段
     */
    private final IndexFieldTypePoller indexFieldTypePoller;
    private final String indexName;

    @Inject
    public SetIndexReadOnlyAndCalculateRangeJob(SetIndexReadOnlyJob.Factory setIndexReadOnlyJobFactory,
                                                CreateNewSingleIndexRangeJob.Factory createNewSingleIndexRangeJobFactory,
                                                IndexSetRegistry indexSetRegistry,
                                                Indices indices,
                                                IndexFieldTypesService indexFieldTypesService,
                                                IndexFieldTypePoller indexFieldTypePoller,
                                                @Assisted String indexName) {
        this.setIndexReadOnlyJobFactory = setIndexReadOnlyJobFactory;
        this.createNewSingleIndexRangeJobFactory = createNewSingleIndexRangeJobFactory;
        this.indexSetRegistry = indexSetRegistry;
        this.indices = indices;
        this.indexFieldTypesService = indexFieldTypesService;
        this.indexFieldTypePoller = indexFieldTypePoller;
        this.indexName = indexName;
    }

    @Override
    public void execute() {
        if (!indices.exists(indexName)) {
            LOG.debug("Not running job for deleted index <{}>", indexName);
            return;
        }
        if (indices.isClosed(indexName)) {
            LOG.debug("Not running job for closed index <{}>", indexName);
            return;
        }
        // 该任务用于将索引设置成只读 (会强制触发一次刷盘)
        final SystemJob setIndexReadOnlyJob = setIndexReadOnlyJobFactory.create(indexName);
        setIndexReadOnlyJob.execute();

        // 当索引修改成只读后 就不会继续更新数据了 然后这里为该索引创建range对象
        final SystemJob createNewSingleIndexRangeJob = createNewSingleIndexRangeJobFactory.create(indexSetRegistry.getAll(), indexName);
        createNewSingleIndexRangeJob.execute();

        // Update field type information again to make sure we got the latest state
        // 拉取索引集最新的字段信息 并存储到mongodb中
        indexSetRegistry.getForIndex(indexName)
                .ifPresent(indexSet -> {
                    indexFieldTypePoller.pollIndex(indexName, indexSet.getConfig().id())
                            .ifPresent(indexFieldTypesService::upsert);
                });
    }

    @Override
    public void requestCancel() {}

    @Override
    public int getProgress() {
        return 0;
    }

    @Override
    public int maxConcurrency() {
        return 1000;
    }

    @Override
    public boolean providesProgress() {
        return false;
    }

    @Override
    public boolean isCancelable() {
        return false;
    }

    @Override
    public String getDescription() {
        return "Makes index " + indexName + " read only and calculates and adds its index range afterwards.";
    }

    @Override
    public String getClassName() {
        return this.getClass().getCanonicalName();
    }
}
