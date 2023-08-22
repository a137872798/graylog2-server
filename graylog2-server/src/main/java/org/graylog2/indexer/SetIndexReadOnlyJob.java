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

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.indices.jobs.OptimizeIndexJob;
import org.graylog2.plugin.Tools;
import org.graylog2.shared.system.activities.Activity;
import org.graylog2.shared.system.activities.ActivityWriter;
import org.graylog2.system.jobs.SystemJob;
import org.graylog2.system.jobs.SystemJobConcurrencyException;
import org.graylog2.system.jobs.SystemJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * 该对象用于将索引设置成只读
 */
public class SetIndexReadOnlyJob extends SystemJob {
    private static final Logger log = LoggerFactory.getLogger(SetIndexReadOnlyJob.class);

    public interface Factory {
        SetIndexReadOnlyJob create(String index);

    }

    /**
     * 索引对象
     */
    private final Indices indices;

    /**
     * 通过该对象可以将索引集配置转换成索引集
     */
    private final IndexSetRegistry indexSetRegistry;
    /**
     * 用于优化索引的任务 也就是触发段合并
     */
    private final OptimizeIndexJob.Factory optimizeIndexJobFactory;

    /**
     * 该对象管理所有后台任务
     */
    private final SystemJobManager systemJobManager;
    private final String index;
    private final ActivityWriter activityWriter;

    @AssistedInject
    public SetIndexReadOnlyJob(Indices indices,
                               IndexSetRegistry indexSetRegistry,
                               SystemJobManager systemJobManager,
                               OptimizeIndexJob.Factory optimizeIndexJobFactory,
                               ActivityWriter activityWriter,
                               @Assisted String index) {
        this.indices = indices;
        this.indexSetRegistry = indexSetRegistry;
        this.optimizeIndexJobFactory = optimizeIndexJobFactory;
        this.systemJobManager = systemJobManager;
        this.index = index;
        this.activityWriter = activityWriter;
    }

    @Override
    public void execute() {
        if (!indices.exists(index)) {
            log.debug("Not running job for deleted index <{}>", index);
            return;
        }
        if (indices.isClosed(index)) {
            log.debug("Not running job for closed index <{}>", index);
            return;
        }

        // 通过名字找到索引集对象
        final Optional<IndexSet> indexSet = indexSetRegistry.getForIndex(index);

        if (!indexSet.isPresent()) {
            log.error("Couldn't find index set for index <{}>", index);
            return;
        }

        log.info("Flushing old index <{}>.", index);
        // 先强制刷盘
        indices.flush(index);

        // Record the time an index was set read-only.
        // We call this the "closing date" because it denotes when we stopped writing to it.
        // 关闭旧索引
        indices.setClosingDate(index, Tools.nowUTC());

        log.info("Setting old index <{}> to read-only.", index);
        // 设置为只读
        indices.setReadOnly(index);

        activityWriter.write(new Activity("Flushed and set <" + index + "> to read-only.", SetIndexReadOnlyJob.class));

        // 还会对该索引进行优化
        if (!indexSet.get().getConfig().indexOptimizationDisabled()) {
            try {
                systemJobManager.submit(optimizeIndexJobFactory.create(index, indexSet.get().getConfig().indexOptimizationMaxNumSegments()));
            } catch (SystemJobConcurrencyException e) {
                // The concurrency limit is very high. This should never happen.
                log.error("Cannot optimize index <" + index + ">.", e);
            }
        }
    }

    @Override
    public void requestCancel() {
        // not possible, ignore
    }

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
        return "Sets an index to read only for performance and optionally triggers an optimization.";
    }

    @Override
    public String getClassName() {
        return this.getClass().getCanonicalName();
    }

    @Override
    public String getInfo() {
        return "Setting index " + index + " to read-only.";
    }
}
