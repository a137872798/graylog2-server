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
package org.graylog2.indexer.ranges;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Ints;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import org.bson.types.ObjectId;
import org.graylog2.audit.AuditActor;
import org.graylog2.audit.AuditEventSender;
import org.graylog2.bindings.providers.MongoJackObjectMapperProvider;
import org.graylog2.database.MongoConnection;
import org.graylog2.database.NotFoundException;
import org.graylog2.indexer.IndexSetRegistry;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.indices.events.IndicesClosedEvent;
import org.graylog2.indexer.indices.events.IndicesDeletedEvent;
import org.graylog2.indexer.indices.events.IndicesReopenedEvent;
import org.graylog2.indexer.searches.IndexRangeStats;
import org.graylog2.plugin.system.NodeId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mongojack.DBCursor;
import org.mongojack.DBQuery;
import org.mongojack.JacksonDBCollection;
import org.mongojack.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;

import static org.graylog2.audit.AuditEventTypes.ES_INDEX_RANGE_CREATE;
import static org.graylog2.audit.AuditEventTypes.ES_INDEX_RANGE_DELETE;
import static org.graylog2.indexer.indices.Indices.checkIfHealthy;

/**
 * 可以检索索引的范围数据
 */
@Singleton
public class MongoIndexRangeService implements IndexRangeService {
    private static final Logger LOG = LoggerFactory.getLogger(MongoIndexRangeService.class);
    private static final String COLLECTION_NAME = "index_ranges";

    /**
     * 真正的索引对象 对外开放了各种操作索引的api
     */
    private final Indices indices;

    /**
     * 通过它可以检索索引集
     */
    private final IndexSetRegistry indexSetRegistry;

    /**
     * NOOP
     */
    private final AuditEventSender auditEventSender;
    private final NodeId nodeId;
    private final JacksonDBCollection<MongoIndexRange, ObjectId> collection;

    @Inject
    public MongoIndexRangeService(MongoConnection mongoConnection,
                                  MongoJackObjectMapperProvider objectMapperProvider,
                                  Indices indices,
                                  IndexSetRegistry indexSetRegistry,
                                  AuditEventSender auditEventSender,
                                  NodeId nodeId,
                                  EventBus eventBus) {
        this.indices = indices;
        this.indexSetRegistry = indexSetRegistry;
        this.auditEventSender = auditEventSender;
        this.nodeId = nodeId;
        this.collection = JacksonDBCollection.wrap(
                mongoConnection.getDatabase().getCollection(COLLECTION_NAME),
                MongoIndexRange.class,
                ObjectId.class,
                objectMapperProvider.get());

        eventBus.register(this);

        // 创建 index_name/begin_time/end_time 索引
        collection.createIndex(new BasicDBObject(MongoIndexRange.FIELD_INDEX_NAME, 1));
        collection.createIndex(BasicDBObjectBuilder.start()
                .add(MongoIndexRange.FIELD_BEGIN, 1)
                .add(MongoIndexRange.FIELD_END, 1)
                .get());
    }

    //  包含 "start" 代表旧的索引 忽略

    /**
     * 通过名字查询索引范围
     * @param index
     * @return
     * @throws NotFoundException
     */
    @Override
    public IndexRange get(String index) throws NotFoundException {
        final DBQuery.Query query = DBQuery.and(
                DBQuery.notExists("start"),
                DBQuery.is(IndexRange.FIELD_INDEX_NAME, index));
        // 只查询第一个
        final MongoIndexRange indexRange = collection.findOne(query);
        if (indexRange == null) {
            throw new NotFoundException("Index range for index <" + index + "> not found.");
        }

        return indexRange;
    }

    /**
     * 查询时间范围内所有索引
     * @param begin
     * @param end
     * @return
     */
    @Override
    public SortedSet<IndexRange> find(DateTime begin, DateTime end) {
        final DBQuery.Query query = DBQuery.or(
                DBQuery.and(
                        DBQuery.notExists("start"),  // "start" has been used by the old index ranges in MongoDB
                        DBQuery.lessThanEquals(IndexRange.FIELD_BEGIN, end.getMillis()),
                        DBQuery.greaterThanEquals(IndexRange.FIELD_END, begin.getMillis())
                ),
                DBQuery.and(
                        DBQuery.notExists("start"),  // "start" has been used by the old index ranges in MongoDB
                        DBQuery.lessThanEquals(IndexRange.FIELD_BEGIN, 0L),
                        DBQuery.greaterThanEquals(IndexRange.FIELD_END, 0L)
                )
        );

        try (DBCursor<MongoIndexRange> indexRanges = collection.find(query)) {
            return ImmutableSortedSet.copyOf(IndexRange.COMPARATOR, (Iterator<? extends IndexRange>) indexRanges);
        }
    }

    @Override
    public SortedSet<IndexRange> findAll() {
        try (DBCursor<MongoIndexRange> cursor = collection.find(DBQuery.notExists("start"))) {
            return ImmutableSortedSet.copyOf(IndexRange.COMPARATOR, (Iterator<? extends IndexRange>) cursor);
        }
    }

    @Override
    public IndexRange calculateRange(String index) {
        checkIfHealthy(indices.waitForRecovery(index),
                (status) -> new RuntimeException("Unable to calculate range for index <" + index + ">, index is unhealthy: " + status));
        final DateTime now = DateTime.now(DateTimeZone.UTC);
        final Stopwatch sw = Stopwatch.createStarted();

        // 获取该索引的统计数据  其中包含最早时间最晚时间 以及所有streamid
        final IndexRangeStats stats = indices.indexRangeStatsOfIndex(index);
        final int duration = Ints.saturatedCast(sw.stop().elapsed(TimeUnit.MILLISECONDS));

        LOG.info("Calculated range of [{}] in [{}ms].", index, duration);

        // 根据统计信息产生一个range对象
        return MongoIndexRange.create(index, stats.min(), stats.max(), now, duration, stats.streamIds());
    }

    /**
     * 创建一个空range对象
     * @param index
     * @return
     */
    @Override
    public IndexRange createUnknownRange(String index) {
        final DateTime begin = new DateTime(0L, DateTimeZone.UTC);
        final DateTime end = new DateTime(0L, DateTimeZone.UTC);
        final DateTime now = DateTime.now(DateTimeZone.UTC);
        return MongoIndexRange.create(index, begin, end, now, 0);
    }

    @Override
    public WriteResult<MongoIndexRange, ObjectId> save(IndexRange indexRange) {
        // 移除旧的range存储新的  也就是range照理说始终只有一个
        remove(indexRange.indexName());
        final WriteResult<MongoIndexRange, ObjectId> save = collection.save(MongoIndexRange.create(indexRange));
        return save;
    }

    @Override
    public boolean remove(String index) {
        final WriteResult<MongoIndexRange, ObjectId> remove = collection.remove(DBQuery.in(IndexRange.FIELD_INDEX_NAME, index));
        return remove.getN() > 0;
    }

    /**
     * 感知到索引删除事件后 同步删除range数据
     * @param event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void handleIndexDeletion(IndicesDeletedEvent event) {
        for (String index : event.indices()) {
            if (!indexSetRegistry.isManagedIndex(index)) {
                LOG.debug("Not handling deleted index <{}> because it's not managed by any index set.", index);
                continue;
            }
            LOG.debug("Index \"{}\" has been deleted. Removing index range.", index);
            if (remove(index)) {
                auditEventSender.success(AuditActor.system(nodeId), ES_INDEX_RANGE_DELETE, ImmutableMap.of("index_name", index));
            }
        }
    }

    /**
     * close 也是删除range数据
     * @param event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void handleIndexClosing(IndicesClosedEvent event) {
        for (String index : event.indices()) {
            if (!indexSetRegistry.isManagedIndex(index)) {
                LOG.debug("Not handling closed index <{}> because it's not managed by any index set.", index);
                continue;
            }
            LOG.debug("Index \"{}\" has been closed. Removing index range.", index);
            if (remove(index)) {
                auditEventSender.success(AuditActor.system(nodeId), ES_INDEX_RANGE_DELETE, ImmutableMap.of("index_name", index));
            }
        }
    }

    /**
     * 重新生成range对象
     * @param event
     */
    @Subscribe
    @AllowConcurrentEvents
    public void handleIndexReopening(IndicesReopenedEvent event) {
        for (final String index : event.indices()) {
            if (!indexSetRegistry.isManagedIndex(index)) {
                LOG.debug("Not handling reopened index <{}> because it's not managed by any index set.", index);
                continue;
            }
            LOG.debug("Index \"{}\" has been reopened. Calculating index range.", index);

            checkIfHealthy(indices.waitForRecovery(index), (status) -> new RuntimeException("Not handling reopened index <" + index + ">, index is unhealthy: " + status));

            final IndexRange indexRange;
            try {
                indexRange = calculateRange(index);
                auditEventSender.success(AuditActor.system(nodeId), ES_INDEX_RANGE_CREATE, ImmutableMap.of("index_name", index));
            } catch (Exception e) {
                final String message = "Couldn't calculate index range for index \"" + index + "\"";
                LOG.error(message, e);
                auditEventSender.failure(AuditActor.system(nodeId), ES_INDEX_RANGE_CREATE, ImmutableMap.of("index_name", index));
                throw new RuntimeException(message, e);
            }

            save(indexRange);
        }
    }
}
