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
package org.graylog.datanode.initializers;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.glassfish.grizzly.http.CompressionConfig;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.ssl.SSLContextConfigurator;
import org.glassfish.grizzly.ssl.SSLEngineConfigurator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.model.Resource;
import org.graylog.datanode.Configuration;
import org.graylog.datanode.configuration.variants.KeystoreInformation;
import org.graylog.datanode.configuration.variants.OpensearchSecurityConfiguration;
import org.graylog.datanode.management.OpensearchConfigurationChangeEvent;
import org.graylog.datanode.process.OpensearchConfiguration;
import org.graylog.security.certutil.CertConstants;
import org.graylog2.configuration.TLSProtocolsConfiguration;
import org.graylog2.plugin.inject.Graylog2Module;
import org.graylog2.rest.MoreMediaTypes;
import org.graylog2.shared.rest.exceptionmappers.JacksonPropertyExceptionMapper;
import org.graylog2.shared.rest.exceptionmappers.JsonProcessingExceptionMapper;
import org.graylog2.shared.security.tls.KeyStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import javax.net.ssl.SSLContext;
import javax.ws.rs.container.DynamicFeature;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.ExceptionMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.codahale.metrics.MetricRegistry.name;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * graylog开放的http端点
 * 只需要实现AbstractIdleService的startUp和 shutDown即可
 * 具体查看提供了哪些资源能力 还是要查找Module的注入逻辑
 */
public class JerseyService extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(JerseyService.class);
    private static final String RESOURCE_PACKAGE_WEB = "org.graylog2.web.resources";

    // 包含graylog需要的各种配置
    private final Configuration configuration;
    private final Set<Class<?>> systemRestResources;

    private final Set<Class<? extends DynamicFeature>> dynamicFeatures;
    private final Set<Class<? extends ExceptionMapper>> exceptionMappers;
    private final ObjectMapper objectMapper;
    private final MetricRegistry metricRegistry;

    // TLS 先忽略
    private final TLSProtocolsConfiguration tlsConfiguration;

    /**
     * 通过该对象提供http端点
     */
    private HttpServer apiHttpServer = null;

    @Inject
    public JerseyService(final Configuration configuration,
                         Set<Class<? extends DynamicFeature>> dynamicFeatures,
                         Set<Class<? extends ExceptionMapper>> exceptionMappers,
                         // 获得提供Rest服务的所有资源
                         @Named(Graylog2Module.SYSTEM_REST_RESOURCES) final Set<Class<?>> systemRestResources,
                         ObjectMapper objectMapper,
                         MetricRegistry metricRegistry,
                         TLSProtocolsConfiguration tlsConfiguration, EventBus eventBus) {
        this.configuration = requireNonNull(configuration, "configuration");
        this.dynamicFeatures = requireNonNull(dynamicFeatures, "dynamicFeatures");
        this.exceptionMappers = requireNonNull(exceptionMappers, "exceptionMappers");
        this.systemRestResources = systemRestResources;
        this.objectMapper = requireNonNull(objectMapper, "objectMapper");
        this.metricRegistry = requireNonNull(metricRegistry, "metricRegistry");
        this.tlsConfiguration = requireNonNull(tlsConfiguration);
        // 本对象也监听 EventBus中的事件
        eventBus.register(this);
    }

    /**
     * 监听 OpenSearcher 配置项改变的事件
     * @param event
     * @throws Exception
     */
    @Subscribe
    public synchronized void handleOpensearchConfigurationChange(OpensearchConfigurationChangeEvent event) throws Exception {
        LOG.info("Opensearch config changed, restarting jersey service to apply security changes");
        shutDown();
        doStartup(extractSslConfiguration(event.config()));
    }

    /**
     * TODO: replace this map magic with proper types in OpensearchConfiguration
     */
    private SSLEngineConfigurator extractSslConfiguration(OpensearchConfiguration config) throws GeneralSecurityException, IOException {
        final OpensearchSecurityConfiguration securityConfiguration = config.opensearchSecurityConfiguration();
        if (securityConfiguration != null && securityConfiguration.securityEnabled()) {
            // caution, this path is relative to the opensearch config directory!
            return buildSslEngineConfigurator(securityConfiguration.getHttpCertificate());
        } else {
            return null;
        }

    }

    /**
     * 可以通过监听到配置项变更事件自动启动 不需要手动启动
     */
    @Override
    protected void startUp() {
        // do nothing, the actual startup will be triggered at the moment opensearch configuration is available
    }

    private void doStartup(SSLEngineConfigurator sslEngineConfigurator) throws Exception {
        // we need to work around the change introduced in https://github.com/GrizzlyNIO/grizzly-mirror/commit/ba9beb2d137e708e00caf7c22603532f753ec850
        // because the PooledMemoryManager which is default now uses 10% of the heap no matter what
        System.setProperty("org.glassfish.grizzly.DEFAULT_MEMORY_MANAGER", "org.glassfish.grizzly.memory.HeapMemoryManager");
        startUpApi(sslEngineConfigurator);
    }

    @Override
    protected void shutDown() {
        shutdownHttpServer(apiHttpServer, configuration.getHttpBindAddress());
    }

    /**
     * 终止http服务
     * @param httpServer
     * @param bindAddress
     */
    private void shutdownHttpServer(HttpServer httpServer, HostAndPort bindAddress) {
        if (httpServer != null && httpServer.isStarted()) {
            LOG.info("Shutting down HTTP listener at <{}>", bindAddress);
            httpServer.shutdownNow();
        }
    }

    /**
     * 启动http服务
     * @param sslEngineConfigurator
     * @throws Exception
     */
    private void startUpApi(SSLEngineConfigurator sslEngineConfigurator) throws Exception {

        // 获取http服务器绑定的ip 以及 contextPath
        final HostAndPort bindAddress = configuration.getHttpBindAddress();
        final String contextPath = configuration.getHttpPublishUri().getPath();
        final URI listenUri = new URI(
                configuration.getUriScheme(),
                null,
                bindAddress.getHost(),
                bindAddress.getPort(),
                isNullOrEmpty(contextPath) ? "/" : contextPath,
                null,
                null
        );

        // 根据传入的一些http服务器配置项 进行初始化
        apiHttpServer = setUp(
                listenUri,
                sslEngineConfigurator,
                configuration.getHttpThreadPoolSize(),
                configuration.getHttpSelectorRunnersCount(),
                configuration.getHttpMaxHeaderSize(),
                configuration.isHttpEnableGzip(),
                Set.of());

        apiHttpServer.start();

        LOG.info("Started REST API at <{}>", configuration.getHttpBindAddress());
    }

    /**
     * 填充端点配置
     * @param additionalResources
     * @return
     */
    private ResourceConfig buildResourceConfig(final Set<Resource> additionalResources) {
        final ResourceConfig rc = new ResourceConfig()
                .property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE, true)
                .property(ServerProperties.WADL_FEATURE_DISABLE, true)
                .property(ServerProperties.MEDIA_TYPE_MAPPINGS, mediaTypeMappings())
                // 注册json解析相关的
                .registerClasses(
                        JacksonJaxbJsonProvider.class,
                        JsonProcessingExceptionMapper.class,
                        JsonMappingExceptionMapper.class,
                        JacksonPropertyExceptionMapper.class)
                // Replacing this with a lambda leads to missing subtypes - https://github.com/Graylog2/graylog2-server/pull/10617#discussion_r630236360
                .register(new ContextResolver<ObjectMapper>() {
                    @Override
                    public ObjectMapper getContext(Class<?> type) {
                        return objectMapper;
                    }
                })
                .register(MultiPartFeature.class)
                // 注册Rest资源对象
                .registerClasses(systemRestResources)
                // 注册额外的资源
                .registerResources(additionalResources);

        exceptionMappers.forEach(rc::registerClasses);
        dynamicFeatures.forEach(rc::registerClasses);

        return rc;
    }

    private Map<String, MediaType> mediaTypeMappings() {
        return ImmutableMap.of(
                "json", MediaType.APPLICATION_JSON_TYPE,
                "ndjson", MoreMediaTypes.APPLICATION_NDJSON_TYPE,
                "csv", MoreMediaTypes.TEXT_CSV_TYPE,
                "log", MoreMediaTypes.TEXT_PLAIN_TYPE
        );
    }

    /**
     * 启动http服务器
     * @param listenUri
     * @param sslEngineConfigurator
     * @param threadPoolSize
     * @param selectorRunnersCount
     * @param maxHeaderSize
     * @param enableGzip
     * @param additionalResources
     * @return
     */
    private HttpServer setUp(URI listenUri,
                             SSLEngineConfigurator sslEngineConfigurator,
                             int threadPoolSize,
                             int selectorRunnersCount,
                             int maxHeaderSize,
                             boolean enableGzip,
                             Set<Resource> additionalResources) {

        // 将各个类注册到 ResourceConfig中
        final ResourceConfig resourceConfig = buildResourceConfig(additionalResources);
        final HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(
                listenUri,
                resourceConfig,
                sslEngineConfigurator != null,
                sslEngineConfigurator,
                false);

        final NetworkListener listener = httpServer.getListener("grizzly");
        listener.setMaxHttpHeaderSize(maxHeaderSize);

        final ExecutorService workerThreadPoolExecutor = instrumentedExecutor(
                "http-worker-executor",
                "http-worker-%d",
                threadPoolSize);
        listener.getTransport().setWorkerThreadPool(workerThreadPoolExecutor);

        // The Grizzly default value is equal to `Runtime.getRuntime().availableProcessors()` which doesn't make
        // sense for Graylog because we are not mainly a web server.
        // See "Selector runners count" at https://grizzly.java.net/bestpractices.html for details.
        listener.getTransport().setSelectorRunnersCount(selectorRunnersCount);

        if (enableGzip) {
            final CompressionConfig compressionConfig = listener.getCompressionConfig();
            compressionConfig.setCompressionMode(CompressionConfig.CompressionMode.ON);
            compressionConfig.setCompressionMinSize(512);
        }

        return httpServer;
    }

    private SSLEngineConfigurator buildSslEngineConfigurator(KeystoreInformation keystoreInformation)
            throws GeneralSecurityException, IOException {
        if (keystoreInformation == null || !Files.isRegularFile(keystoreInformation.location()) || !Files.isReadable(keystoreInformation.location())) {
            throw new IllegalArgumentException("Unreadable to read private key");
        }


        final SSLContextConfigurator sslContextConfigurator = new SSLContextConfigurator();
        final char[] password = firstNonNull(keystoreInformation.passwordAsString(), "").toCharArray();

        final KeyStore keyStore = readKeystore(keystoreInformation);

        sslContextConfigurator.setKeyStorePass(password);
        sslContextConfigurator.setKeyStoreBytes(KeyStoreUtils.getBytes(keyStore, password));

        final SSLContext sslContext = sslContextConfigurator.createSSLContext(true);
        final SSLEngineConfigurator sslEngineConfigurator = new SSLEngineConfigurator(sslContext, false, false, false);
        sslEngineConfigurator.setEnabledProtocols(tlsConfiguration.getEnabledTlsProtocols().toArray(new String[0]));
        return sslEngineConfigurator;
    }

    private static KeyStore readKeystore(KeystoreInformation keystoreInformation) {
        try (var in = Files.newInputStream(keystoreInformation.location())) {
            KeyStore caKeystore = KeyStore.getInstance(CertConstants.PKCS12);
            caKeystore.load(in, keystoreInformation.password());
            return caKeystore;
        } catch (IOException | GeneralSecurityException ex) {
            throw new RuntimeException("Could not read keystore: " + ex.getMessage(), ex);
        }
    }

    private ExecutorService instrumentedExecutor(final String executorName,
                                                 final String threadNameFormat,
                                                 int poolSize) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(threadNameFormat)
                .setDaemon(true)
                .build();

        return new InstrumentedExecutorService(
                Executors.newFixedThreadPool(poolSize, threadFactory),
                metricRegistry,
                name(JerseyService.class, executorName));
    }
}
