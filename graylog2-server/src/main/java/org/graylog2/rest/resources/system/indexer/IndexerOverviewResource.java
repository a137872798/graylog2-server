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
package org.graylog2.rest.resources.system.indexer;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.IndexSetRegistry;
import org.graylog2.indexer.MongoIndexSet;
import org.graylog2.indexer.cluster.Cluster;
import org.graylog2.indexer.counts.Counts;
import org.graylog2.indexer.indices.Indices;
import org.graylog2.indexer.indices.TooManyAliasesException;
import org.graylog2.indexer.indices.util.NumberBasedIndexNameComparator;
import org.graylog2.rest.models.count.responses.MessageCountResponse;
import org.graylog2.rest.models.system.deflector.responses.DeflectorSummary;
import org.graylog2.rest.models.system.indexer.responses.IndexRangeSummary;
import org.graylog2.rest.models.system.indexer.responses.IndexSizeSummary;
import org.graylog2.rest.models.system.indexer.responses.IndexSummary;
import org.graylog2.rest.models.system.indexer.responses.IndexerClusterOverview;
import org.graylog2.rest.models.system.indexer.responses.IndexerOverview;
import org.graylog2.rest.resources.system.DeflectorResource;
import org.graylog2.rest.resources.system.IndexRangesResource;
import org.graylog2.shared.rest.resources.RestResource;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.ServiceUnavailableException;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.graylog2.shared.rest.documentation.generator.Generator.CLOUD_VISIBLE;

/**
 * 查看索引概述
 */
@RequiresAuthentication
@Api(value = "Indexer/Overview", description = "Indexing overview", tags={CLOUD_VISIBLE})
@Path("/system/indexer/overview")
public class IndexerOverviewResource extends RestResource {
    private final DeflectorResource deflectorResource;
    private final IndexerClusterResource indexerClusterResource;
    private final IndexRangesResource indexRangesResource;
    private final Counts counts;
    private final IndexSetRegistry indexSetRegistry;
    private final Indices indices;
    private final Cluster cluster;

    @Inject
    public IndexerOverviewResource(DeflectorResource deflectorResource,
                                   IndexerClusterResource indexerClusterResource,
                                   IndexRangesResource indexRangesResource,
                                   Counts counts,
                                   IndexSetRegistry indexSetRegistry,
                                   Indices indices,
                                   Cluster cluster) {
        this.deflectorResource = deflectorResource;
        this.indexerClusterResource = indexerClusterResource;
        this.indexRangesResource = indexRangesResource;
        this.counts = counts;
        this.indexSetRegistry = indexSetRegistry;
        this.indices = indices;
        this.cluster = cluster;
    }

    /**
     * 获取当前索引集的状态
     * @return
     * @throws TooManyAliasesException
     */
    @GET
    @Timed
    @ApiOperation(value = "Get overview of current indexing state, including deflector config, cluster state, index ranges & message counts.")
    @Produces(MediaType.APPLICATION_JSON)
    @Deprecated
    public IndexerOverview index() throws TooManyAliasesException {
        // 要求能正常访问到es服务器  且datanode数量 >1
        if (!cluster.isConnected()) {
            throw new ServiceUnavailableException("Elasticsearch cluster is not available, check your configuration and logs for more information.");
        }

        try {
            // 这里使用默认的索引集
            return getIndexerOverview(indexSetRegistry.getDefault());
        } catch (IllegalStateException e) {
            throw new NotFoundException("Default index set not found");
        }
    }

    @GET
    @Timed
    @Path("/{indexSetId}")
    @ApiOperation(value = "Get overview of current indexing state for the given index set, including deflector config, cluster state, index ranges & message counts.")
    @Produces(MediaType.APPLICATION_JSON)
    public IndexerOverview index(@ApiParam(name = "indexSetId") @PathParam("indexSetId") String indexSetId) throws TooManyAliasesException {
        if (!cluster.isConnected()) {
            throw new ServiceUnavailableException("Elasticsearch cluster is not available, check your configuration and logs for more information.");
        }

        final IndexSet indexSet = getIndexSet(indexSetRegistry, indexSetId);

        return getIndexerOverview(indexSet);
    }

    /**
     * 获取索引概述
     * @param indexSet
     * @return
     * @throws TooManyAliasesException
     */
    private IndexerOverview getIndexerOverview(IndexSet indexSet) throws TooManyAliasesException {
        final String indexSetId = indexSet.getConfig().id();

        // 先从mongodb中获取indexSet的相关信息
        final DeflectorSummary deflectorSummary = deflectorResource.deflector(indexSetId);
        // 这里返回了所有indexRange的描述信息
        final List<IndexRangeSummary> indexRanges = indexRangesResource.list().ranges();

        // 获取该索引集的全部统计信息
        final JsonNode indexStats = indices.getIndexStats(indexSet);
        final List<String> indexNames = new ArrayList<>();
        // 把json打散后存入 indexNames中
        indexStats.fieldNames().forEachRemaining(indexNames::add);
        // 根据索引是否有_reopened的别名 判断是否是 reopened索引
        final Map<String, Boolean> areReopened = indices.areReopened(indexNames);
        final List<IndexSummary> indicesSummaries = buildIndexSummaries(deflectorSummary, indexSet, indexRanges, indexStats, areReopened);

        return IndexerOverview.create(deflectorSummary,
                IndexerClusterOverview.create(indexerClusterResource.clusterHealth(), indexerClusterResource.clusterName().name()),
                MessageCountResponse.create(counts.total(indexSet)),
                indicesSummaries);
    }

    private List<IndexSummary> buildIndexSummaries(DeflectorSummary deflectorSummary, IndexSet indexSet, List<IndexRangeSummary> indexRanges, JsonNode indexStats, Map<String, Boolean> areReopened) {
        final Iterator<Map.Entry<String, JsonNode>> fields = indexStats.fields();
        final List<IndexSummary> indexSummaries = new ArrayList<>();
        while (fields.hasNext()) {
            final Map.Entry<String, JsonNode> entry = fields.next();
            indexSummaries.add(buildIndexSummary(entry, indexRanges, deflectorSummary, areReopened));

        }
        indices.getClosedIndices(indexSet).forEach(indexName -> indexSummaries.add(IndexSummary.create(
                indexName,
                null,
                indexRanges.stream().filter((indexRangeSummary) -> indexRangeSummary.indexName().equals(indexName)).findFirst().orElse(null),
                indexName.equals(deflectorSummary.currentTarget()),
                true,
                false
        )));
        indexSummaries.sort(Comparator.comparing(IndexSummary::indexName, new NumberBasedIndexNameComparator(MongoIndexSet.SEPARATOR)));
        return indexSummaries;
    }

    private IndexSummary buildIndexSummary(Map.Entry<String, JsonNode> indexStats,
                                           List<IndexRangeSummary> indexRanges,
                                           DeflectorSummary deflectorSummary,
                                           Map<String, Boolean> areReopened) {
        final String index = indexStats.getKey();
        final JsonNode primaries = indexStats.getValue().path("primaries");
        final JsonNode docs = primaries.path("docs");
        final long count = docs.path("count").asLong();
        final long deleted = docs.path("deleted").asLong();
        final JsonNode store = primaries.path("store");
        final long sizeInBytes = store.path("size_in_bytes").asLong();

        final Optional<IndexRangeSummary> range = indexRanges.stream()
                .filter(indexRangeSummary -> indexRangeSummary.indexName().equals(index))
                .findFirst();
        final boolean isDeflector = index.equals(deflectorSummary.currentTarget());
        final boolean isReopened = areReopened.get(index);

        return IndexSummary.create(
                indexStats.getKey(),
                IndexSizeSummary.create(count, deleted, sizeInBytes),
                range.orElse(null),
                isDeflector,
                false,
                isReopened);
    }

}
