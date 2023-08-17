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
package org.graylog2.shared.bindings.providers;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.graylog2.shared.events.DeadEventLoggingListener;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * 该对象描述了如何产生 EventBus
 */
public class EventBusProvider implements Provider<EventBus> {
    private final int asyncEventbusProcessors;
    private final MetricRegistry metricRegistry;

    /**
     *
     * @param asyncEventbusProcessors 该属性会提前注册到guice内
     * @param metricRegistry
     */
    @Inject
    public EventBusProvider(final @Named("async_eventbus_processors") int asyncEventbusProcessors, final MetricRegistry metricRegistry) {
        this.asyncEventbusProcessors = asyncEventbusProcessors;
        this.metricRegistry = metricRegistry;
    }

    /**
     * 通过该方法产生实例
     * @return
     */
    @Override
    public EventBus get() {
        final EventBus eventBus = new AsyncEventBus("graylog-eventbus", executorService(asyncEventbusProcessors));
        // 为bus设置处理逻辑
        eventBus.register(new DeadEventLoggingListener());

        return eventBus;
    }

    private ExecutorService executorService(int nThreads) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("eventbus-handler-%d").build();
        return new InstrumentedExecutorService(
                Executors.newFixedThreadPool(nThreads, threadFactory),
                metricRegistry,
                name(this.getClass(), "executor-service"));
    }
}
