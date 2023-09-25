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
package org.graylog2.shared.buffers.processors;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.lmax.disruptor.WorkHandler;
import de.huxhorn.sulky.ulid.ULID;
import org.graylog.failure.FailureSubmissionService;
import org.graylog2.buffers.OutputBuffer;
import org.graylog2.messageprocessors.OrderedMessageProcessors;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.Messages;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.buffers.MessageEvent;
import org.graylog2.plugin.messageprocessors.MessageProcessor;
import org.graylog2.plugin.streams.DefaultStream;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.system.processing.ProcessingStatusRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Provider;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * 表示如何处理RingBuffer的数据 (从MessageInput转移到第二层Buffer后使用的处理器)
 */
public class ProcessBufferProcessor implements WorkHandler<MessageEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessBufferProcessor.class);

    private final Meter incomingMessages;

    private final Timer processTime;
    private final Meter outgoingMessages;

    /**
     * 表示一组被排序的processor对象
     */
    private final OrderedMessageProcessors orderedMessageProcessors;

    /**
     * 当数据被设置到该buffer后  会有专门的handler处理数据 并发送到下游
     */
    private final OutputBuffer outputBuffer;
    /**
     * 用于统计数据
     */
    private final ProcessingStatusRecorder processingStatusRecorder;
    private final ULID ulid;
    /**
     * 用于为消息生成唯一id
     */
    private final MessageULIDGenerator messageULIDGenerator;
    /**
     * 为原始消息解码
     */
    private final DecodingProcessor decodingProcessor;

    /**
     * 数据流在没有关联任何stream时 会关联到默认stream上
     */
    private final Provider<Stream> defaultStreamProvider;
    /**
     * 处理失败时 提交的服务
     */
    private final FailureSubmissionService failureSubmissionService;

    /**
     * 该处理器此时正在处理的消息
     */
    private volatile Message currentMessage;

    /**
     *
     * @param metricRegistry
     * @param orderedMessageProcessors
     * @param outputBuffer
     * @param processingStatusRecorder
     * @param ulid
     * @param messageULIDGenerator
     * @param decodingProcessor
     * @param defaultStreamProvider
     * @param failureSubmissionService
     */
    @AssistedInject   // 这个注解的作用是 除了携带@Assisted注解的参数需要显式传入外 其他参数由guice进行辅助注入
    public ProcessBufferProcessor(MetricRegistry metricRegistry,
                                  OrderedMessageProcessors orderedMessageProcessors,
                                  OutputBuffer outputBuffer,
                                  ProcessingStatusRecorder processingStatusRecorder,
                                  ULID ulid,
                                  MessageULIDGenerator messageULIDGenerator,
                                  @Assisted DecodingProcessor decodingProcessor,
                                  @DefaultStream Provider<Stream> defaultStreamProvider,
                                  FailureSubmissionService failureSubmissionService) {
        this.orderedMessageProcessors = orderedMessageProcessors;
        this.outputBuffer = outputBuffer;
        this.processingStatusRecorder = processingStatusRecorder;
        this.ulid = ulid;
        this.messageULIDGenerator = messageULIDGenerator;
        this.decodingProcessor = decodingProcessor;
        this.defaultStreamProvider = defaultStreamProvider;
        this.failureSubmissionService = failureSubmissionService;

        incomingMessages = metricRegistry.meter(name(ProcessBufferProcessor.class, "incomingMessages"));
        outgoingMessages = metricRegistry.meter(name(ProcessBufferProcessor.class, "outgoingMessages"));
        processTime = metricRegistry.timer(name(ProcessBufferProcessor.class, "processTime"));
        currentMessage = null;
    }

    /**
     * 当收到RingBuffer的新事件时 触发该方法
     * @param event
     * @throws Exception
     */
    @Override
    public void onEvent(MessageEvent event) throws Exception {
        try {
            // Decode the RawMessage to a Message object. The DecodingProcessor used to be a separate handler in the
            // ProcessBuffer. Due to performance problems discovered during 1.0.0 testing, we decided to move this here.
            // TODO The DecodingProcessor does not need to be a EventHandler. We decided to do it like this to keep the change as small as possible for 1.0.0.
            // 先通过该对象解码原始消息
            decodingProcessor.onEvent(event, 0L, false);

            // 根据解码出来是单条消息还是多条消息 走不同分支
            if (event.isSingleMessage()) {
                dispatchMessage(event.getMessage());
            } else {
                final Collection<Message> messageList = event.getMessages();
                if (messageList == null) {
                    // skip message events which could not be decoded properly
                    return;
                }

                for (final Message message : messageList) {
                    dispatchMessage(message);
                }
            }
        } finally {
            event.clearMessages();
        }
    }

    public Optional<Message> getCurrentMessage() {
        return Optional.ofNullable(currentMessage);
    }

    /**
     * 分发消息
     * @param msg
     */
    private void dispatchMessage(final Message msg) {
        // 设置当前消息
        currentMessage = msg;
        incomingMessages.mark();

        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting to process message <{}>.", msg.getId());
        }

        try (final Timer.Context ignored = processTime.time()) {
            handleMessage(msg);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Finished processing message <{}>. Writing to output buffer.", msg.getId());
            }
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                // Log warning including the stacktrace
                LOG.warn("Unable to process message <{}>:", msg.getId(), e);
                // Log full message content to aid debugging
                LOG.debug("Failed message <{}>: {}", msg.getId(), msg.toDumpString());
            } else {
                // Only logs a single line warning without stacktrace
                LOG.warn("Unable to process message <{}>: {}", msg.getId(), e);
            }

            // 失败时提交错误信息
            failureSubmissionService.submitUnknownProcessingError(msg, String.format(Locale.ENGLISH,
                    "Unable to process message <%s>: %s",
                    msg.getId(), e));

        } finally {
            currentMessage = null;
            outgoingMessages.mark();
        }
    }

    /**
     * 如何处理消息
     * @param msg
     */
    private void handleMessage(@Nonnull Message msg) {
        // 先关联默认流 然后再跟其他流进行碰撞
        msg.addStream(defaultStreamProvider.get());
        Messages messages = msg;

        // 使用消息处理器 挨个处理消息
        for (MessageProcessor messageProcessor : orderedMessageProcessors) {
            messages = messageProcessor.process(messages);
        }

        for (Message message : messages) {
            message.ensureValidTimestamp();

            // If a message is received via the Cluster-to-Cluster Forwarder, it already has this field set
            if (!message.hasField(Message.FIELD_GL2_MESSAGE_ID) || isNullOrEmpty(message.getFieldAs(String.class, Message.FIELD_GL2_MESSAGE_ID))) {
                // Set the message ID once all message processors have finished
                // See documentation of Message.FIELD_GL2_MESSAGE_ID for details
                message.addField(Message.FIELD_GL2_MESSAGE_ID, messageULIDGenerator.createULID(message));
            }

            // The processing time should only be set once all message processors have finished
            message.setProcessingTime(Tools.nowUTC());
            processingStatusRecorder.updatePostProcessingReceiveTime(message.getReceiveTime());

            // message中没有error信息也会返回true
            if(failureSubmissionService.submitProcessingErrors(message)) {
                // 处理完消息后 将消息发往下游 OutputBuffer (下游的RingBuffer)
                outputBuffer.insertBlocking(message);
            }
        }
    }

    public interface Factory {
        ProcessBufferProcessor create(DecodingProcessor decodingProcessor);
    }
}
