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

import org.graylog2.indexer.cluster.Node;
import org.graylog2.indexer.indexset.IndexSetConfig;
import org.graylog2.storage.SearchVersion;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;

import static org.graylog2.shared.utilities.StringUtils.f;

@Singleton
public class IndexMappingFactory {

    /**
     * 通过该对象可以获取存储引擎的类型以及版本
     */
    private final Node node;

    /**
     * 生成一个IndexMapping对象 可以生成索引模板   模板就是告知数据存储的样式
     */
    private final Map<String, IndexTemplateProvider> providers;

    @Inject
    public IndexMappingFactory(Node node, Map<String, IndexTemplateProvider> providers) {
        this.node = node;
        this.providers = providers;
    }

    /**
     * 提供参数 产生模板
     * @param indexSetConfig
     * @return
     * @throws IgnoreIndexTemplate
     */
    @Nonnull
    public IndexMappingTemplate createIndexMapping(@Nonnull IndexSetConfig indexSetConfig)  throws IgnoreIndexTemplate {
        final SearchVersion elasticsearchVersion = node.getVersion()
                .orElseThrow(() -> new ElasticsearchException("Unable to retrieve Elasticsearch version."));

        // 表示该index 会有一个默认type 叫做messages type对应db中的table
        final String templateType = indexSetConfig
                .indexTemplateType()
                .orElse(IndexSetConfig.DEFAULT_INDEX_TEMPLATE_TYPE);

        // type messages/event 是不同的template
        return resolveIndexMappingTemplateProvider(templateType)
                .create(elasticsearchVersion, indexSetConfig);
    }

    private IndexTemplateProvider resolveIndexMappingTemplateProvider(@Nonnull String templateType) {
        if (providers.containsKey(templateType)) {
            return providers.get(templateType);
        } else {
            throw new IllegalStateException(f("No index template provider found for type '%s'", templateType));
        }
    }
}
