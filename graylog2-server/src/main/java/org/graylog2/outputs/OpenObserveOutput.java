package org.graylog2.outputs;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import okhttp3.*;
import org.graylog2.indexer.IndexSet;
import org.graylog2.indexer.messages.Messages;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.outputs.MessageOutput;
import org.graylog2.plugin.streams.Stream;
import org.graylog2.shared.bindings.providers.OkHttpClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.codahale.metrics.MetricRegistry.name;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * 将处理好的日志数据发往 OpenObserve
 */
public class OpenObserveOutput implements MessageOutput {

    private static final Logger log = LoggerFactory.getLogger(OpenObserveOutput.class);

    private static final String OO_BULK_ENDPOINT = System.getenv().getOrDefault("OPENOBSERVE_ADDRESS", "http://127.0.0.1:5080") + "/api/default/_bulk";

    private static final String OO_ROOT_USERNAME = "xueleiguo843@gmail.com";

    private static final String OO_ROOT_PASSWORD = "123456";

    private static final String OO_AUTHORIZATION = Base64.getEncoder().encodeToString((OO_ROOT_USERNAME + ":" + OO_ROOT_PASSWORD).getBytes());

    private final OkHttpClient okHttpClient;

    private final int maxBufferSize;
    private final int outputFlushInterval;
    private final Timer processTime;
    private final AtomicLong lastFlushTime = new AtomicLong();

    private final Meter bufferFlushFailures;
    private final Histogram batchSize;
    private final Meter bufferFlushes;
    private final Meter invalidTimestampMeter;

    private ScheduledFuture<?> flushTask;

    private volatile List<Map.Entry<String, Message>> buffer;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private static final AtomicInteger activeFlushThreads = new AtomicInteger(0);

    private final ScheduledExecutorService daemonScheduler;

    private final Meter bufferFlushesRequested;

    private final ObjectMapper objectMapper;

    private final int shutdownTimeoutMs;


    @Inject
    public OpenObserveOutput(MetricRegistry metricRegistry,
                             OkHttpClientProvider okHttpClientProvider,
                             org.graylog2.Configuration serverConfiguration,
                             @Named("daemonScheduler") ScheduledExecutorService daemonScheduler,
                             ObjectMapper objectMapper) {

        this.maxBufferSize = serverConfiguration.getOutputBatchSize();
        this.outputFlushInterval = serverConfiguration.getOutputFlushInterval();
        this.buffer = new ArrayList<>(maxBufferSize);
        this.processTime = metricRegistry.timer(name(this.getClass(), "processTime"));
        this.bufferFlushFailures = metricRegistry.meter(name(this.getClass(), "bufferFlushFailures"));
        this.batchSize = metricRegistry.histogram(name(this.getClass(), "batchSize"));
        this.bufferFlushes = metricRegistry.meter(name(this.getClass(), "bufferFlushes"));
        this.daemonScheduler = daemonScheduler;
        this.bufferFlushesRequested = metricRegistry.meter(name(this.getClass(), "bufferFlushesRequested"));
        this.invalidTimestampMeter = metricRegistry.meter(name(Messages.class, "invalid-timestamps"));
        this.shutdownTimeoutMs = serverConfiguration.getShutdownTimeout();

        this.objectMapper = objectMapper;

        okHttpClient = okHttpClientProvider.get();
        isRunning.set(true);
    }

    @Override
    public void stop() {
        if (flushTask != null) {
            flushTask.cancel(false);
        }
        final ExecutorService executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat("oo-output-shutdown-flush").build());
        try {
            executorService.submit(this::forceFlush).get(shutdownTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // OK, we are shutting down anyway
        } catch (ExecutionException e) {
            log.warn("Flushing current batch to indexer while stopping failed with message: {}.", e.getMessage());
        } catch (TimeoutException e) {
            log.warn("Timed out flushing current batch to indexer while stopping.");
        } catch (Exception e) {
            log.warn("flushing current batch to indexer while stopping failed");
        } finally {
            executorService.shutdownNow();
        }
        isRunning.set(false);
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void write(Message message) throws Exception {
        for (IndexSet indexSet : message.getIndexSets()) {
            writeMessageEntry(Maps.immutableEntry(indexSet.getIndexPrefix(), message));
        }
    }

    private void writeMessageEntry(Map.Entry<String, Message> entry) {
        List<Map.Entry<String, Message>> flushBatch = null;

        synchronized (this) {
            buffer.add(entry);

            if (buffer.size() >= maxBufferSize) {
                flushBatch = buffer;
                buffer = new ArrayList<>(maxBufferSize);
            }
        }
        // if the current thread found it had to flush any messages, it does so but blocks.
        // this ensures we don't flush more than 'processorCount' in parallel.
        // TODO this will still be time limited by the OutputBufferProcessor and thus be called more often than it should
        if (flushBatch != null) {
            flush(flushBatch);
        }
    }

    /**
     * 刷盘数据
     *
     * @param entries
     */
    private void flush(List<Map.Entry<String, Message>> entries) {
        // never try to flush an empty buffer
        if (entries.isEmpty()) {
            return;
        }

        activeFlushThreads.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("Starting flushing {} messages, flush threads active {}",
                    entries.size(),
                    activeFlushThreads.get());
        }

        try {
            try (Timer.Context ignored = processTime.time()) {
                lastFlushTime.set(System.nanoTime());
                writeToOO(entries);
                batchSize.update(entries.size());
                bufferFlushes.mark();
            }
        } catch (Exception e) {
            log.error("Unable to flush message buffer", e);
            bufferFlushFailures.mark();
        }
        activeFlushThreads.decrementAndGet();
        log.debug("Flushing {} messages completed", entries.size());
    }

    private void writeToOO(List<Map.Entry<String, Message>> messages) {
        // 按照 bulk格式构建body
        StringBuilder bodyBuilder = new StringBuilder();
        for (Map.Entry<String, Message> entry : messages) {
            StringBuilder lineBuilder = new StringBuilder();
            String streamName = entry.getKey();
            lineBuilder.append("{ \"create\" : { \"_index\" : \"");
            lineBuilder.append(streamName);
            lineBuilder.append("\", \"_id\" : \"");
            lineBuilder.append(entry.getValue().getId());
            lineBuilder.append("\" } }");
            bodyBuilder.append(lineBuilder);
            bodyBuilder.append("\n");

            lineBuilder = new StringBuilder();

            try {
                lineBuilder.append(this.objectMapper.writeValueAsString(entry.getValue().toElasticSearchObject(objectMapper, this.invalidTimestampMeter)));
            } catch (JsonProcessingException e) {
                log.error("convert message to Json fail {}", entry.getValue(), e);
                throw new RuntimeException(e);
            }
            bodyBuilder.append(lineBuilder);
            bodyBuilder.append("\n");
        }

        RequestBody body = RequestBody.create(MediaType.parse("application/json"), bodyBuilder.toString());

        Request request = new Request.Builder()
                .url(OO_BULK_ENDPOINT)
                .post(body)
                .header("Authorization", "Basic " + OO_AUTHORIZATION)
                .build();

        Response response = null;
        try {
            response = okHttpClient.newCall(request).execute();
        } catch (IOException e) {
            log.error("send bulk request fail resp:{}", response, e);
            throw new RuntimeException(e);
        }

        if (!response.isSuccessful()) {
            log.error("write to OpenObserve fail resp:{}", response);
            throw new RuntimeException("write to OpenObserve fail");
        }
    }


    @Override
    public void write(List<Message> messages) throws Exception {
        throw new UnsupportedOperationException("Method not supported!");
    }

    @Override
    public void initialize() throws Exception {
        // 开启后台任务进行定时刷盘
        this.flushTask = daemonScheduler.scheduleAtFixedRate(() -> {
                    try {
                        forceFlushIfTimedout();
                    } catch (Exception e) {
                        log.error("Caught exception while trying to flush output", e);
                    }
                },
                outputFlushInterval, outputFlushInterval, TimeUnit.SECONDS);
    }

    public void forceFlushIfTimedout() {
        // if we shouldn't flush at all based on the last flush time, no need to synchronize on this.
        if (lastFlushTime.get() != 0 &&
                outputFlushInterval > NANOSECONDS.toSeconds(System.nanoTime() - lastFlushTime.get())) {
            return;
        }

        forceFlush();
    }

    private void forceFlush() {
        List<Map.Entry<String, Message>> flushBatch = null;

        synchronized (this) {
            flushBatch = buffer;
            buffer = new ArrayList<>(maxBufferSize);
        }
        if (flushBatch != null) {
            bufferFlushesRequested.mark();
            flush(flushBatch);
        }
    }

    public interface Factory extends MessageOutput.Factory<OpenObserveOutput> {
        @Override
        OpenObserveOutput create(Stream stream, Configuration configuration);

        @Override
        OpenObserveOutput.Config getConfig();

        @Override
        OpenObserveOutput.Descriptor getDescriptor();
    }

    public static class Config extends MessageOutput.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            // Built in output. This is just for plugin compat. No special configuration required.
            return new ConfigurationRequest();
        }
    }

    public static class Descriptor extends MessageOutput.Descriptor {
        public Descriptor() {
            super("OpenObserve Output", false, "", "OpenObserve Output");
        }

        public Descriptor(String name, boolean exclusive, String linkToDocs, String humanName) {
            super(name, exclusive, linkToDocs, humanName);
        }
    }
}
