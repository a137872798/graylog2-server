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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.inject.assistedinject.Assisted;
import org.graylog2.audit.AuditActor;
import org.graylog2.audit.AuditEventSender;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.indexer.indices.HealthStatus;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.indices.TooManyAliasesException;
import org.graylog2.indexer.indices.jobs.SetIndexReadOnlyAndCalculateRangeJob;
import org.graylog2.indexer.ranges.IndexRange;
import org.graylog2.indexer.ranges.IndexRangeService;
import org.graylog2.plugin.system.NodeId;
import org.graylog2.shared.system.activities.Activity;
import org.graylog2.shared.system.activities.ActivityWriter;
import org.graylog2.system.jobs.SystemJob;
import org.graylog2.system.jobs.SystemJobConcurrencyException;
import org.graylog2.system.jobs.SystemJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static org.graylog2.audit.AuditEventTypes.ES_WRITE_INDEX_UPDATE;
import static org.graylog2.indexer.indices.Indices.checkIfHealthy;

/**
 * graylog 索引集的默认实现
 */
public class MongoIndexSet implements IndexSet {
    private static final Logger LOG = LoggerFactory.getLogger(MongoIndexSet.class);

    public static final String SEPARATOR = "_";
    /**
     * 偏转器是什么玩意?
     */
    public static final String DEFLECTOR_SUFFIX = "deflector";

    // TODO: Hardcoded archive suffix. See: https://github.com/Graylog2/graylog2-server/issues/2058
    // TODO 3.0: Remove this in 3.0, only used for pre 2.2 backwards compatibility.
    public static final String RESTORED_ARCHIVE_SUFFIX = "_restored_archive";
    public interface Factory {
        MongoIndexSet create(IndexSetConfig config);
    }

    /**
     * 该索引集相关的配置对象
     */
    private final IndexSetConfig config;
    /**
     * 此时写入的索引别名
     */
    private final String writeIndexAlias;

    /**
     * 真正的索引对象 内部接入了存储引擎
     */
    private final Indices indices;

    // 索引正则/偏转索引正则
    private final Pattern indexPattern;
    private final Pattern deflectorIndexPattern;

    /**
     * 索引通配符
     */
    private final String indexWildcard;
    /**
     * 索引的范围信息会存储在mongodb中  可以通过该服务查询
     */
    private final IndexRangeService indexRangeService;

    /**
     * NOOP
     */
    private final AuditEventSender auditEventSender;

    /**
     * 该索引集所在的节点id
     */
    private final NodeId nodeId;

    /**
     * 该对象会管理各种后台任务
     */
    private final SystemJobManager systemJobManager;

    /**
     * 该job的作用是 将索引设置成只读,并计算最新的range,然后将索引的field信息拉取到本地并存储到mongodb中
     */
    private final SetIndexReadOnlyAndCalculateRangeJob.Factory jobFactory;

    /**
     * 用于写入activity对象
     */
    private final ActivityWriter activityWriter;

    @Inject
    public MongoIndexSet(@Assisted final IndexSetConfig config,
                         final Indices indices,
                         final NodeId nodeId,
                         final IndexRangeService indexRangeService,
                         final AuditEventSender auditEventSender,
                         final SystemJobManager systemJobManager,
                         final SetIndexReadOnlyAndCalculateRangeJob.Factory jobFactory,
                         final ActivityWriter activityWriter
    ) {
        this.config = requireNonNull(config);
        this.writeIndexAlias = config.indexPrefix() + SEPARATOR + DEFLECTOR_SUFFIX;
        this.indices = requireNonNull(indices);
        this.nodeId = requireNonNull(nodeId);
        this.indexRangeService = requireNonNull(indexRangeService);
        this.auditEventSender = requireNonNull(auditEventSender);
        this.systemJobManager = requireNonNull(systemJobManager);
        this.jobFactory = requireNonNull(jobFactory);
        this.activityWriter = requireNonNull(activityWriter);

        // Part of the pattern can be configured in IndexSetConfig. If set we use the indexMatchPattern from the config.
        final String indexPattern = isNullOrEmpty(config.indexMatchPattern())
                ? Pattern.quote(config.indexPrefix())
                : config.indexMatchPattern();

        this.indexPattern = Pattern.compile("^" + indexPattern + SEPARATOR + "\\d+(?:" + RESTORED_ARCHIVE_SUFFIX + ")?");
        this.deflectorIndexPattern = Pattern.compile("^" + indexPattern + SEPARATOR + "\\d+");

        // The index wildcard can be configured in IndexSetConfig. If not set we use a default one based on the index
        // prefix.
        if (isNullOrEmpty(config.indexWildcard())) {
            this.indexWildcard = config.indexPrefix() + SEPARATOR + "*";
        } else {
            this.indexWildcard = config.indexWildcard();
        }
    }

    /**
     *
     * @return
     */
    @Override
    public String[] getManagedIndices() {
        // 查询满足正则的所有索引名  这些就是归属于该索引集的索引
        final Set<String> indexNames = indices.getIndexNamesAndAliases(getIndexWildcard()).keySet();
        // also allow restore archives to be returned
        final List<String> result = indexNames.stream()
                .filter(this::isManagedIndex)  // 对索引做一次过滤
                .collect(Collectors.toList());

        return result.toArray(new String[result.size()]);
    }

    /**
     * 返回此时可写索引的别名  一个索引集只有一个可写索引
     * @return
     */
    @Override
    public String getWriteIndexAlias() {
        return writeIndexAlias;
    }

    @Override
    public String getIndexWildcard() {
        return indexWildcard;
    }

    @Override
    public String getNewestIndex() throws NoTargetIndexException {
        return buildIndexName(getNewestIndexNumber());
    }

    /**
     * 获取最新的索引编号
     * @return
     * @throws NoTargetIndexException
     */
    @VisibleForTesting
    int getNewestIndexNumber() throws NoTargetIndexException {
        // 返回匹配正则的所有索引
        final Set<String> indexNames = indices.getIndexNamesAndAliases(getIndexWildcard()).keySet();

        if (indexNames.isEmpty()) {
            throw new NoTargetIndexException("Couldn't find any indices for wildcard " + getIndexWildcard());
        }

        int highestIndexNumber = -1;
        for (String indexName : indexNames) {
            // 只需要处理deflector索引
            if (!isGraylogDeflectorIndex(indexName)) {
                continue;
            }

            final int currentHighest = highestIndexNumber;
            highestIndexNumber = extractIndexNumber(indexName)
                    .map(indexNumber -> Math.max(indexNumber, currentHighest))
                    .orElse(highestIndexNumber);
        }

        if (highestIndexNumber == -1) {
            throw new NoTargetIndexException("Couldn't get newest index number for indices " + indexNames);
        }

        return highestIndexNumber;
    }

    /**
     * 从索引名字中抽取编号
     * @param indexName index name
     * @return
     */
    @Override
    public Optional<Integer> extractIndexNumber(final String indexName) {
        final int beginIndex = config.indexPrefix().length() + 1;
        if (indexName.length() < beginIndex) {
            return Optional.empty();
        }

        final String suffix = indexName.substring(beginIndex);
        try {
            return Optional.of(Integer.parseInt(suffix));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    @VisibleForTesting
    String buildIndexName(final int number) {
        return config.indexPrefix() + SEPARATOR + number;
    }

    @VisibleForTesting
    boolean isGraylogDeflectorIndex(final String indexName) {
        return !isNullOrEmpty(indexName) && !isWriteIndexAlias(indexName) && deflectorIndexPattern.matcher(indexName).matches();
    }

    /**
     * 从别名转换成普通名字
     * @return
     * @throws TooManyAliasesException
     */
    @Override
    @Nullable
    public String getActiveWriteIndex() throws TooManyAliasesException {
        return indices.aliasTarget(getWriteIndexAlias()).orElse(null);
    }

    /**
     * 注意只需要deflector索引
     * @return
     */
    @Override
    public Map<String, Set<String>> getAllIndexAliases() {
        final Map<String, Set<String>> indexNamesAndAliases = indices.getIndexNamesAndAliases(getIndexWildcard());

        // filter out the restored archives from the result set
        return indexNamesAndAliases.entrySet().stream()
                .filter(e -> isGraylogDeflectorIndex(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public String getIndexPrefix() {
        return config.indexPrefix();
    }

    @Override
    public boolean isUp() {
        return indices.aliasExists(getWriteIndexAlias());
    }

    @Override
    public boolean isWriteIndexAlias(String index) {
        return getWriteIndexAlias().equals(index);
    }

    @Override
    public boolean isManagedIndex(String index) {
        return !isNullOrEmpty(index) && !isWriteIndexAlias(index) && indexPattern.matcher(index).matches();
    }

    /**
     * 准备索引集去接收消息
     */
    @Override
    public void setUp() {

        // 表明索引集已经被关闭
        if (!getConfig().isWritable()) {
            LOG.debug("Not setting up non-writable index set <{}> ({})", getConfig().id(), getConfig().title());
            return;
        }

        // Check if there already is an deflector index pointing somewhere.
        if (isUp()) {
            LOG.info("Found deflector alias <{}>. Using it.", getWriteIndexAlias());
        } else {
            LOG.info("Did not find a deflector alias. Setting one up now.");

            // Do we have a target index to point to?
            try {
                // 产生一个最新的索引名
                final String currentTarget = getNewestIndex();
                LOG.info("Pointing to already existing index target <{}>", currentTarget);

                // 更新索引
                pointTo(currentTarget);
            } catch (NoTargetIndexException ex) {
                final String msg = "There is no index target to point to. Creating one now.";
                LOG.info(msg);
                activityWriter.write(new Activity(msg, IndexSet.class));

                cycle(); // No index, so automatically cycling to a new one.
            }
        }
    }

    @Override
    public void cycle() {
        if (!getConfig().isWritable()) {
            LOG.debug("Not cycling non-writable index set <{}> ({})", getConfig().id(), getConfig().title());
            return;
        }

        int oldTargetNumber;

        try {
            oldTargetNumber = getNewestIndexNumber();
        } catch (NoTargetIndexException ex) {
            oldTargetNumber = -1;
        }
        final int newTargetNumber = oldTargetNumber + 1;

        final String newTarget = buildIndexName(newTargetNumber);
        final String oldTarget = buildIndexName(oldTargetNumber);

        if (oldTargetNumber == -1) {
            LOG.info("Cycling from <none> to <{}>.", newTarget);
        } else {
            LOG.info("Cycling from <{}> to <{}>.", oldTarget, newTarget);
        }

        // Create new index.
        LOG.info("Creating target index <{}>.", newTarget);
        if (!indices.create(newTarget, this)) {
            throw new RuntimeException("Could not create new target index <" + newTarget + ">.");
        }

        LOG.info("Waiting for allocation of index <{}>.", newTarget);
        final HealthStatus healthStatus = indices.waitForRecovery(newTarget);
        checkIfHealthy(healthStatus, (status) -> new RuntimeException("New target index did not become healthy (target index: <" + newTarget + ">)"));
        LOG.debug("Health status of index <{}>: {}", newTarget, healthStatus);

        addDeflectorIndexRange(newTarget);
        LOG.info("Index <{}> has been successfully allocated.", newTarget);

        // Point deflector to new index.
        final String indexAlias = getWriteIndexAlias();
        LOG.info("Pointing index alias <{}> to new index <{}>.", indexAlias, newTarget);

        final Activity activity = new Activity(IndexSet.class);
        if (oldTargetNumber == -1) {
            // Only pointing, not cycling.
            pointTo(newTarget);
            activity.setMessage("Cycled index alias <" + indexAlias + "> from <none> to <" + newTarget + ">.");
        } else {
            // Re-pointing from existing old index to the new one.
            LOG.debug("Switching over index alias <{}>.", indexAlias);
            pointTo(newTarget, oldTarget);
            setIndexReadOnlyAndCalculateRange(oldTarget);
            activity.setMessage("Cycled index alias <" + indexAlias + "> from <" + oldTarget + "> to <" + newTarget + ">.");
        }

        LOG.info("Successfully pointed index alias <{}> to index <{}>.", indexAlias, newTarget);

        activityWriter.write(activity);
        auditEventSender.success(AuditActor.system(nodeId), ES_WRITE_INDEX_UPDATE, ImmutableMap.of("indexName", newTarget));
    }

    /**
     * 为某个索引设置定时任务
     * @param indexName
     */
    private void setIndexReadOnlyAndCalculateRange(String indexName) {
        // perform these steps after a delay, so we don't race with indexing into the alias
        // it can happen that an index request still writes to the old deflector target, while we cycled it above.
        // setting the index to readOnly would result in ClusterBlockExceptions in the indexing request.
        // waiting 30 seconds to perform the background task should completely get rid of these errors.
        final SystemJob setIndexReadOnlyAndCalculateRangeJob = jobFactory.create(indexName);
        try {
            systemJobManager.submitWithDelay(setIndexReadOnlyAndCalculateRangeJob, 30, TimeUnit.SECONDS);
        } catch (SystemJobConcurrencyException e) {
            LOG.error("Cannot set index <" + indexName + "> to read only and calculate its range. It won't be optimized.", e);
        }
    }

    /**
     * 为该索引创建一个空的range对象
     * @param indexName
     */
    private void addDeflectorIndexRange(String indexName) {
        final IndexRange deflectorRange = indexRangeService.createUnknownRange(indexName);
        indexRangeService.save(deflectorRange);
    }

    @Override
    public void cleanupAliases(Set<String> indexNames) {
        final SortedSet<String> sortedSet = ImmutableSortedSet
                .orderedBy(new IndexNameComparator(this))
                .addAll(indexNames)
                .build();

        indices.removeAliases(getWriteIndexAlias(), sortedSet.headSet(sortedSet.last()));
    }

    /**
     * 更换索引
     * @param newIndexName index to add the write index alias to
     * @param oldIndexName index to remove the write index alias from
     */
    @Override
    public void pointTo(String newIndexName, String oldIndexName) {
        indices.cycleAlias(getWriteIndexAlias(), newIndexName, oldIndexName);
    }

    private void pointTo(final String indexName) {
        indices.cycleAlias(getWriteIndexAlias(), indexName);
    }

    @Override
    public IndexSetConfig getConfig() {
        return config;
    }

    @Override
    public int compareTo(IndexSet o) {
        return ComparisonChain.start()
                .compare(this.getConfig(), o.getConfig())
                .result();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoIndexSet that = (MongoIndexSet) o;
        return Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return config.hashCode();
    }

    @Override
    public String toString() {
        return "MongoIndexSet{" + "config=" + config + '}';
    }
}
