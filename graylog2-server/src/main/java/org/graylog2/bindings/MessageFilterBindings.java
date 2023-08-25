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
package org.graylog2.bindings;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;
import org.graylog2.filters.ExtractorFilter;
import org.graylog2.filters.StaticFieldFilter;
import org.graylog2.plugin.filters.MessageFilter;

/**
 * 该对象用于配置消息过滤器
 */
public class MessageFilterBindings extends AbstractModule {
    @Override
    protected void configure() {
        Multibinder<MessageFilter> messageFilters = Multibinder.newSetBinder(binder(), MessageFilter.class);
        // 为message追加静态字段
        messageFilters.addBinding().to(StaticFieldFilter.class);
        // 对message进行字段抽取 和转换
        messageFilters.addBinding().to(ExtractorFilter.class);
    }
}
