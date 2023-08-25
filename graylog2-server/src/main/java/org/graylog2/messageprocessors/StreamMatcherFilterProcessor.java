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
package org.graylog2.messageprocessors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Messages;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.messageprocessors.MessageProcessor;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.streams.StreamRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * 当收到message后 是要经过消息处理器的
 * 该对象会分析message 并将其关联到所有匹配的stream
 */
public class StreamMatcherFilterProcessor implements MessageProcessor  {
    private static final Logger LOG = LoggerFactory.getLogger(StreamMatcherFilterProcessor.class);

    public static class Descriptor implements MessageProcessor.Descriptor {
        @Override
        public String name() {
            return "Stream Rule Processor";
        }

        @Override
        public String className() {
            return StreamMatcherFilterProcessor.class.getCanonicalName();
        }
    }

    private final MetricRegistry metricRegistry;
    /**
     * 相当于服务的一个开关
     */
    private final ServerStatus serverStatus;

    /**
     * 用于分析message会关联到的stream
     */
    private final StreamRouter streamRouter;

    @Inject
    public StreamMatcherFilterProcessor(MetricRegistry metricRegistry,
                                        ServerStatus serverStatus,
                                        StreamRouter streamRouter) {
        this.metricRegistry = metricRegistry;
        this.serverStatus = serverStatus;
        this.streamRouter = streamRouter;
    }

    private void route(Message msg) {
        // 为message找到关联的stream 并添加到message中
        List<Stream> streams = streamRouter.route(msg);
        msg.addStreams(streams);

        LOG.debug("Routed message <{}> to {} streams.", msg.getId(), streams.size());

    }

    @Override
    public Messages process(Messages messages) {
        for (Message msg : messages) {
            // Keep the old metric name for backwards compatibility
            final String timerName = name("org.graylog2.filters.StreamMatcherFilter", "executionTime");
            final Timer timer = metricRegistry.timer(timerName);
            final Timer.Context timerContext = timer.time();

            route(msg);

            final long elapsedNanos = timerContext.stop();
            msg.recordTiming(serverStatus, timerName, elapsedNanos);
        }
        return messages;
    }
}
