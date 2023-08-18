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
package org.graylog2.plugin.inputs;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import org.graylog2.plugin.AbstractDescriptor;
import org.graylog2.plugin.GlobalMetricNames;
import org.graylog2.plugin.IOState;
import org.graylog2.plugin.InputFailureRecorder;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Stoppable;
import org.graylog2.plugin.Tools;
import org.graylog2.plugin.buffers.InputBuffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.codecs.Codec;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 描述一个消息源 可以产生并发送input
 */
public abstract class MessageInput implements Stoppable {
    private static final Logger LOG = LoggerFactory.getLogger(MessageInput.class);

    // 这些字段可以对应到input对象上
    public static final String FIELD_ID = "_id";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_NODE_ID = "node_id";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_TITLE = "title";
    public static final String FIELD_CONFIGURATION = "configuration";
    public static final String FIELD_CREATOR_USER_ID = "creator_user_id";
    public static final String FIELD_CREATED_AT = "created_at";
    public static final String FIELD_STARTED_AT = "started_at";
    public static final String FIELD_ATTRIBUTES = "attributes";
    public static final String FIELD_STATIC_FIELDS = "static_fields";
    public static final String FIELD_GLOBAL = "global";
    public static final String FIELD_DESIRED_STATE = "desired_state";
    public static final String FIELD_CONTENT_PACK = "content_pack";

    @SuppressWarnings("StaticNonFinalField")
    private static int defaultRecvBufferSize = 1024 * 1024;

    // 每条消息对应一个序列号
    private final AtomicLong sequenceNr;
    private final MetricRegistry metricRegistry;
    /**
     * 通过该对象进行数据传输
     */
    private final Transport transport;
    private final MetricRegistry localRegistry;
    /**
     * 编解码器
     */
    private final Codec codec;

    /**
     * 每个MessageInput对象 对应一种消息 消息有自己的描述信息
     */
    private final Descriptor descriptor;
    /**
     * 通过该对象可以启停服务
     */
    private final ServerStatus serverStatus;
    // TODO
    private final Meter incomingMessages;
    private final Meter rawSize;

    /**
     * 存储静态字段
     */
    private final Map<String, String> staticFields = Maps.newConcurrentMap();

    /**
     * 描述一个配置对象  配置值可以作用到该对象上
     */
    private final ConfigurationRequest requestedConfiguration;
    /**
     * This is being used to decide which minimal set of configuration values need to be serialized when a message
     * is written to the journal. The message input's config contains transport configuration as well, but we want to
     * avoid serialising those parts of the configuration in order to save bytes on disk/network.
     * 内部按照类型存放了不同的配置项
     */
    private final Configuration codecConfig;
    private final Counter globalIncomingMessages;
    private final Counter emptyMessages;
    private final Counter globalRawSize;

    protected String title;
    protected String creatorUserId;
    protected String persistId;
    protected DateTime createdAt;
    protected Boolean global = false;
    protected IOState.Type desiredState = IOState.Type.RUNNING;
    protected String contentPack;

    protected final Configuration configuration;

    /**
     * 存储原始数据的buffer
     */
    protected InputBuffer inputBuffer;
    private String nodeId;
    private MetricSet transportMetrics;

    /**
     *
     * @param metricRegistry
     * @param configuration
     * @param transport
     * @param localRegistry
     * @param codec
     * @param config
     * @param descriptor
     * @param serverStatus
     */
    public MessageInput(MetricRegistry metricRegistry,
                        Configuration configuration,
                        Transport transport,
                        LocalMetricRegistry localRegistry, Codec codec, Config config, Descriptor descriptor, ServerStatus serverStatus) {
        this.configuration = configuration;
        if (metricRegistry == localRegistry) {
            LOG.error("########### Do not add the global metric registry twice, the localRegistry parameter is " +
                              "the same as the global metricRegistry. " +
                              "This will cause duplicated metrics and is a bug. " +
                              "Use LocalMetricRegistry in your input instead.");
        }
        this.metricRegistry = metricRegistry;
        this.transport = transport;
        this.localRegistry = localRegistry;
        this.codec = codec;
        this.descriptor = descriptor;
        this.serverStatus = serverStatus;
        // 将config 转换成 ConfigurationRequest
        this.requestedConfiguration = config.combinedRequestedConfiguration();
        // 生成编码器相关的配置项
        this.codecConfig = config.codecConfig.getRequestedConfiguration().filter(codec.getConfiguration());
        globalRawSize = metricRegistry.counter(GlobalMetricNames.INPUT_TRAFFIC);
        rawSize = localRegistry.meter("rawSize");
        incomingMessages = localRegistry.meter("incomingMessages");
        globalIncomingMessages = metricRegistry.counter(GlobalMetricNames.INPUT_THROUGHPUT);
        emptyMessages = localRegistry.counter("emptyMessages");

        // 标注消息的序列号
        sequenceNr = new AtomicLong(0);
    }

    public static int getDefaultRecvBufferSize() {
        return defaultRecvBufferSize;
    }

    public static void setDefaultRecvBufferSize(int size) {
        defaultRecvBufferSize = size;
    }

    /**
     * TODO
     */
    public void initialize() {
        this.transportMetrics = transport.getMetricSet();

        try {
            if (transportMetrics != null) {
                metricRegistry.register(getUniqueReadableId(), transportMetrics);
            }
            metricRegistry.register(getUniqueReadableId(), localRegistry);
        } catch (IllegalArgumentException ignored) {
            // This happens for certain types of inputs, see https://github.com/Graylog2/graylog2-server/issues/1049#issuecomment-88857134
        }
    }

    public void checkConfiguration() throws ConfigurationException {
        final ConfigurationRequest cr = getRequestedConfiguration();
        cr.check(getConfiguration());
    }

    /**
     * 不断的处理 RawMessage 后 消息会堆积在buffer中  之后通过该方法完成发送
     * @param buffer
     * @param inputFailureRecorder
     * @throws MisfireException
     */
    public void launch(final InputBuffer buffer, InputFailureRecorder inputFailureRecorder) throws MisfireException {
        this.inputBuffer = buffer;
        try {
            // TODO 兼容性代码
            launch(buffer); // call this for inputs that still overload the one argument launch method

            // 为传输层设置消息累加器
            transport.setMessageAggregator(codec.getAggregator());

            // 通过传输层发送消息
            transport.launch(this, inputFailureRecorder);
        } catch (Exception e) {
            inputBuffer = null;
            throw new MisfireException(e);
        }
    }

    @Deprecated
    public void launch(final InputBuffer buffer) throws MisfireException {
        // kept for backwards compat with inputs that overload this method
    }

    @Override
    public void stop() {
        transport.stop();
        cleanupMetrics();
    }

    public void terminate() {
        cleanupMetrics();
    }

    /**
     * TODO
     */
    private void cleanupMetrics() {
        if (localRegistry != null && localRegistry.getMetrics() != null) {
            for (String metricName : localRegistry.getMetrics().keySet()) {
                metricRegistry.remove(getUniqueReadableId() + "." + metricName);
            }
        }

        if (this.transportMetrics != null && this.transportMetrics.getMetrics() != null) {
            for (String metricName : this.transportMetrics.getMetrics().keySet()) {
                metricRegistry.remove(getUniqueReadableId() + "." + metricName);
            }
        }
    }

    public ConfigurationRequest getRequestedConfiguration() {
        return requestedConfiguration;
    }

    public Descriptor getDescriptor() {
        return descriptor;
    }

    public String getName() {
        return descriptor.getName();
    }

    public boolean isExclusive() {
        return descriptor.isExclusive();
    }

    public String getId() {
        return persistId;
    }

    public String getPersistId() {
        return persistId;
    }

    public void setPersistId(String id) {
        this.persistId = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCreatorUserId() {
        return creatorUserId;
    }

    public void setCreatorUserId(String creatorUserId) {
        this.creatorUserId = creatorUserId;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Boolean isGlobal() {
        return global;
    }

    /**
     * Determines if Graylog should only launch a single instance of this input at a time in the cluster.
     * <p>
     * This might be useful for an input which polls data from an external source and maintains local state, i.e. a
     * cursor, to determine records have been fetched already. In that case, running a second instance of the input at
     * the same time on a different node in the cluster might then lead to the same data fetched again, which would
     * produce duplicate log messages in Graylog.
     * <p>
     * Returning {@code true} from this method will only really make sense if the input also {@code isGlobal}.
     *
     * @return {@code true} if only a single instance of the input should be launched in the cluster. It will be
     * launched on the <em>leader</em> node.
     * <p>
     * {@code false} otherwise
     */
    public boolean onlyOnePerCluster() {
        return false;
    }

    public void setGlobal(Boolean global) {
        this.global = global;
    }

    public IOState.Type getDesiredState() {
        return desiredState;
    }

    public void setDesiredState(IOState.Type newDesiredState) {
        if (newDesiredState.equals(IOState.Type.RUNNING)
                || newDesiredState.equals(IOState.Type.STOPPED)) {
            desiredState = newDesiredState;
        } else {
            LOG.error("Ignoring unexpected desired state " + newDesiredState + " for input " + title);
        }
    }

    public String getContentPack() {
        return contentPack;
    }

    public void setContentPack(String contentPack) {
        this.contentPack = contentPack;
    }

    @Deprecated
    public Map<String, Object> getAttributesWithMaskedPasswords() {
        return configuration.getSource();
    }

    @JsonValue
    public Map<String, Object> asMapMasked() {
        final Map<String, Object> result = asMap();
        result.remove(FIELD_CONFIGURATION);
        result.put(FIELD_ATTRIBUTES, getAttributesWithMaskedPasswords());

        return result;
    }

    /**
     * 抽取出本对象的各字段信息
     * @return
     */
    public Map<String, Object> asMap() {
        // This has to be mutable (see #asMapMasked) and support null values!
        final Map<String, Object> map = new HashMap<>();
        map.put(FIELD_TYPE, getClass().getCanonicalName());
        map.put(FIELD_NAME, getName());
        map.put(FIELD_TITLE, getTitle());
        map.put(FIELD_CREATOR_USER_ID, getCreatorUserId());
        map.put(FIELD_GLOBAL, isGlobal());
        map.put(FIELD_DESIRED_STATE, getDesiredState().toString());
        map.put(FIELD_CONTENT_PACK, getContentPack());
        map.put(FIELD_CONFIGURATION, getConfiguration().getSource());

        if (getCreatedAt() != null) {
            map.put(FIELD_CREATED_AT, getCreatedAt());
        } else {
            map.put(FIELD_CREATED_AT, Tools.nowUTC());
        }


        if (getStaticFields() != null && !getStaticFields().isEmpty()) {
            map.put(FIELD_STATIC_FIELDS, getStaticFields());
        }

        if (!isGlobal()) {
            map.put(FIELD_NODE_ID, getNodeId());
        }

        return map;
    }

    public void addStaticField(String key, String value) {
        this.staticFields.put(key, value);
    }

    public void addStaticFields(Map<String, String> staticFields) {
        this.staticFields.putAll(staticFields);
    }

    public Map<String, String> getStaticFields() {
        return this.staticFields;
    }

    public String getUniqueReadableId() {
        return getClass().getName() + "." + getId();
    }

    @Override
    public int hashCode() {
        return getPersistId().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof MessageInput) {
            final MessageInput input = (MessageInput) obj;
            return this.getPersistId().equals(input.getPersistId());
        } else {
            return false;
        }
    }

    public Codec getCodec() {
        return codec;
    }

    /**
     * 处理原始消息
     * @param rawMessage
     */
    public void processRawMessage(RawMessage rawMessage) {
        final int payloadLength = rawMessage.getPayload().length;

        // 代表处理了一个空消息
        if (payloadLength == 0) {
            LOG.debug("Discarding empty message {} from input {} (remote address {}). Turn logger org.graylog2.plugin.journal.RawMessage to TRACE to see originating stack trace.",
                    rawMessage.getId(),
                    toIdentifier(),
                    rawMessage.getRemoteAddress() == null ? "unknown" : rawMessage.getRemoteAddress());
            emptyMessages.inc();
            return;
        }

        // add the common message metadata for this input/codec
        rawMessage.setCodecName(codec.getName());
        rawMessage.setCodecConfig(codecConfig);
        rawMessage.addSourceNode(getId(), serverStatus.getNodeId());
        // Wrap at unsigned int maximum  每条原始消息经过该方法处理后 被设置序列号  并且序列号递增
        rawMessage.setSequenceNr((int) sequenceNr.getAndUpdate(i -> i == 0xFFFF_FFFFL ? 0 : i + 1));

        // 将加工后的原始消息设置到buffer中
        inputBuffer.insert(rawMessage);

        incomingMessages.mark();
        globalIncomingMessages.inc();
        rawSize.mark(payloadLength);
        globalRawSize.inc(payloadLength);
    }

    public String getType() {
        return this.getClass().getCanonicalName();
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isCloudCompatible() {
        return descriptor.isCloudCompatible();
    }

    public boolean isForwarderCompatible() {
        return descriptor.isForwarderCompatible();
    }

    /**
     * 可以通过工厂创建 MessageInput对象
     * @param <M>
     */
    public interface Factory<M> {
        M create(Configuration configuration);

        Config getConfig();

        Descriptor getDescriptor();
    }

    public static class Config {
        public final Transport.Config transportConfig;
        public final Codec.Config codecConfig;

        // required for guice, but isn't called.
        Config() {
            throw new IllegalStateException("This class should not be instantiated directly, this is a bug.");
        }

        protected Config(Transport.Config transportConfig, Codec.Config codecConfig) {
            this.transportConfig = transportConfig;
            this.codecConfig = codecConfig;
        }

        public ConfigurationRequest combinedRequestedConfiguration() {
            final ConfigurationRequest transport = transportConfig.getRequestedConfiguration();
            final ConfigurationRequest codec = codecConfig.getRequestedConfiguration();
            final ConfigurationRequest r = new ConfigurationRequest();
            r.putAll(transport.getFields());
            r.putAll(codec.getFields());

            // give the codec the opportunity to override default values for certain configuration fields,
            // this is commonly being used to default to some well known port for protocols such as GELF or syslog
            codecConfig.overrideDefaultValues(r);

            return r;
        }
    }

    public static abstract class Descriptor extends AbstractDescriptor {
        public Descriptor() {
            super();
        }

        protected Descriptor(String name, boolean exclusive, String linkToDocs) {
            super(name, exclusive, linkToDocs);
        }

        public boolean isCloudCompatible() {
            return false;
        }

        public boolean isForwarderCompatible() {
            return true;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("title", getTitle())
                .add("type", getType())
                .add("nodeId", getNodeId())
                .toString();
    }

    public String toIdentifier() {
        return "[" + getName() + "/" + getTitle() + "/" + getId() + "]";
    }
}
