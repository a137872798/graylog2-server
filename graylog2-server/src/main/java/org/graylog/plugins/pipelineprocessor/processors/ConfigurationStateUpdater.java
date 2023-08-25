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
package org.graylog.plugins.pipelineprocessor.processors;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.swrve.ratelimitedlogger.RateLimitedLog;
import org.graylog.plugins.pipelineprocessor.ast.Pipeline;
import org.graylog.plugins.pipelineprocessor.ast.Rule;
import org.graylog.plugins.pipelineprocessor.db.PipelineService;
import org.graylog.plugins.pipelineprocessor.db.PipelineStreamConnectionsService;
import org.graylog.plugins.pipelineprocessor.db.RuleMetricsConfigDto;
import org.graylog.plugins.pipelineprocessor.db.RuleMetricsConfigService;
import org.graylog.plugins.pipelineprocessor.db.RuleService;
import org.graylog.plugins.pipelineprocessor.events.PipelineConnectionsChangedEvent;
import org.graylog.plugins.pipelineprocessor.events.PipelinesChangedEvent;
import org.graylog.plugins.pipelineprocessor.events.RuleMetricsConfigChangedEvent;
import org.graylog.plugins.pipelineprocessor.events.RulesChangedEvent;
import org.graylog.plugins.pipelineprocessor.parser.ParseException;
import org.graylog.plugins.pipelineprocessor.parser.PipelineRuleParser;
import org.graylog.plugins.pipelineprocessor.rest.PipelineConnections;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static org.graylog.plugins.pipelineprocessor.processors.PipelineInterpreter.getRateLimitedLog;

/**
 * 该对象负责定期更新 State对象  state记录了每个stream关联的pipeline 每个pipeline会关联一组stage 每个stage包含表达式以及statement 用于处理数据
 */
@Singleton
public class ConfigurationStateUpdater {
    private static final RateLimitedLog log = getRateLimitedLog(ConfigurationStateUpdater.class);

    /**
     * 通过mongodb提供ruleDao的CRUD
     */
    private final RuleService ruleService;
    /**
     * 提供pipeline的CRUD
     */
    private final PipelineService pipelineService;
    /**
     * 提供PipelineConnections的CRUD
     */
    private final PipelineStreamConnectionsService pipelineStreamConnectionsService;

    /**
     * 该对象与antlr4交互 可以解析rule和pipeline
     */
    private final PipelineRuleParser pipelineRuleParser;
    /**
     * TODO Metrics 相关的先忽略
     */
    private final RuleMetricsConfigService ruleMetricsConfigService;
    private final MetricRegistry metricRegistry;
    private final ScheduledExecutorService scheduler;
    private final EventBus serverEventBus;

    // 该工厂可以产生一个状态对象
    private final PipelineInterpreter.State.Factory stateFactory;
    /**
     * non-null if the update has successfully loaded a state
     * 本对象就是负责更新 State 的
     */
    private final AtomicReference<PipelineInterpreter.State> latestState = new AtomicReference<>();

    @Inject
    public ConfigurationStateUpdater(RuleService ruleService,
                                     PipelineService pipelineService,
                                     PipelineStreamConnectionsService pipelineStreamConnectionsService,
                                     PipelineRuleParser pipelineRuleParser,
                                     RuleMetricsConfigService ruleMetricsConfigService,
                                     MetricRegistry metricRegistry,
                                     @Named("daemonScheduler") ScheduledExecutorService scheduler,
                                     EventBus serverEventBus,
                                     PipelineInterpreter.State.Factory stateFactory) {
        this.ruleService = ruleService;
        this.pipelineService = pipelineService;
        this.pipelineStreamConnectionsService = pipelineStreamConnectionsService;
        this.pipelineRuleParser = pipelineRuleParser;
        this.ruleMetricsConfigService = ruleMetricsConfigService;
        this.metricRegistry = metricRegistry;
        this.scheduler = scheduler;
        this.serverEventBus = serverEventBus;
        this.stateFactory = stateFactory;

        // listens to cluster wide Rule, Pipeline and pipeline stream connection changes
        serverEventBus.register(this);

        reloadAndSave();
    }

    // only the singleton instance should mutate itself, others are welcome to reload a new state, but we don't
    // currently allow direct global state updates from external sources (if you need to, send an event on the bus instead)
    // 更新 state
    private synchronized PipelineInterpreter.State reloadAndSave() {
        // read all rules and parse them
        // 加载的表达式 会被解析成Rule对象
        Map<String, Rule> ruleNameMap = Maps.newHashMap();

        // 加载系统中所有的 rule
        ruleService.loadAll().forEach(ruleDao -> {
            Rule rule;
            try {
                rule = pipelineRuleParser.parseRule(ruleDao.id(), ruleDao.source(), false);
            } catch (ParseException e) {
                log.warn("Ignoring non parseable rule <{}/{}> with errors <{}>", ruleDao.title(), ruleDao.id(), e.getErrors());
                rule = Rule.alwaysFalse("Failed to parse rule: " + ruleDao.id());
            }
            ruleNameMap.put(rule.name(), rule);
        });

        // read all pipelines and parse them
        // 解析 pipeline
        ImmutableMap.Builder<String, Pipeline> pipelineIdMap = ImmutableMap.builder();
        pipelineService.loadAll().forEach(pipelineDao -> {
            Pipeline pipeline;
            try {
                pipeline = pipelineRuleParser.parsePipeline(pipelineDao.id(), pipelineDao.source());
            } catch (ParseException e) {
                pipeline = Pipeline.empty("Failed to parse pipeline" + pipelineDao.id());
            }
            // noinspection ConstantConditions
            // pipeline内记录了关联的rule的name 这里是去map中根据name查询rule对象 并进行关联
            pipelineIdMap.put(pipelineDao.id(), resolvePipeline(pipeline, ruleNameMap));
        });

        final ImmutableMap<String, Pipeline> currentPipelines = pipelineIdMap.build();

        // read all stream connections of those pipelines to allow processing messages through them
        // 这里负责把 stream 与 pipeline 关联起来
        final HashMultimap<String, Pipeline> connections = HashMultimap.create();

        for (PipelineConnections streamConnection : pipelineStreamConnectionsService.loadAll()) {
            streamConnection.pipelineIds().stream()
                    .map(currentPipelines::get)
                    .filter(Objects::nonNull)
                    .forEach(pipeline -> connections.put(streamConnection.streamId(), pipeline));
        }
        ImmutableSetMultimap<String, Pipeline> streamPipelineConnections = ImmutableSetMultimap.copyOf(connections);

        final RuleMetricsConfigDto ruleMetricsConfig = ruleMetricsConfigService.get();

        // 通过这些参数来构造state对象
        final PipelineInterpreter.State newState = stateFactory.newState(currentPipelines, streamPipelineConnections, ruleMetricsConfig);
        latestState.set(newState);
        return newState;
    }


    /**
     * Can be used to inspect or use the current state of the pipeline system.
     * For example, the interpreter
     *
     * @return the currently loaded state of the updater
     */
    public PipelineInterpreter.State getLatestState() {
        return latestState.get();
    }


    /**
     *
     * @param pipeline
     * @param ruleNameMap
     * @return
     */
    @Nonnull
    private Pipeline resolvePipeline(Pipeline pipeline, Map<String, Rule> ruleNameMap) {
        log.debug("Resolving pipeline {}", pipeline.name());

        pipeline.stages().forEach(stage -> {
            final List<Rule> resolvedRules = stage.ruleReferences().stream()
                    .map(ref -> {
                        Rule rule = ruleNameMap.get(ref);
                        if (rule == null) {
                            rule = Rule.alwaysFalse("Unresolved rule " + ref);
                        }
                        // make a copy so that the metrics match up (we don't share actual objects between stages)
                        rule = rule.copy();
                        log.debug("Resolved rule `{}` to {}", ref, rule);
                        // include back reference to stage
                        rule.registerMetrics(metricRegistry, pipeline.id(), String.valueOf(stage.stage()));
                        return rule;
                    })
                    .collect(Collectors.toList());
            // 将stage与rules pipeline 关联起来
            stage.setRules(resolvedRules);
            stage.setPipeline(pipeline);
            stage.registerMetrics(metricRegistry, pipeline.id());
        });

        pipeline.registerMetrics(metricRegistry);
        return pipeline;
    }

    // 下面是监听pipeline，rule的更新事件

    // TODO avoid reloading everything on every change, certain changes can get away with doing less work
    @Subscribe
    public void handleRuleChanges(RulesChangedEvent event) {
        event.deletedRuleIds().forEach(id -> {
            log.debug("Invalidated rule {}", id);
            metricRegistry.removeMatching((name, metric) -> name.startsWith(name(Rule.class, id)));
        });
        event.updatedRuleIds().forEach(id -> log.debug("Refreshing rule {}", id));
        scheduler.schedule(() -> serverEventBus.post(reloadAndSave()), 0, TimeUnit.SECONDS);
    }

    @Subscribe
    public void handlePipelineChanges(PipelinesChangedEvent event) {
        event.deletedPipelineIds().forEach(id -> {
            log.debug("Invalidated pipeline {}", id);
            metricRegistry.removeMatching((name, metric) -> name.startsWith(name(Pipeline.class, id)));
        });
        event.updatedPipelineIds().forEach(id -> log.debug("Refreshing pipeline {}", id));
        scheduler.schedule(() -> serverEventBus.post(reloadAndSave()), 0, TimeUnit.SECONDS);
    }

    @Subscribe
    public void handlePipelineConnectionChanges(PipelineConnectionsChangedEvent event) {
        log.debug("Pipeline stream connection changed: {}", event);
        scheduler.schedule(() -> serverEventBus.post(reloadAndSave()), 0, TimeUnit.SECONDS);
    }

    @Subscribe
    public void handlePipelineStateChange(PipelineInterpreter.State event) {
        log.debug("Pipeline interpreter state got updated");
    }

    @Subscribe
    public void handleRuleMetricsConfigChange(RuleMetricsConfigChangedEvent event) {
        log.debug("Rule metrics config changed: {}", event);
        scheduler.schedule(() -> serverEventBus.post(reloadAndSave()), 0, TimeUnit.SECONDS);
    }
}
