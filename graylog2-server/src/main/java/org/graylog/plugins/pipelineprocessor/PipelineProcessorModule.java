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
package org.graylog.plugins.pipelineprocessor;

import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.graylog.plugins.pipelineprocessor.audit.PipelineProcessorAuditEventTypes;
import org.graylog.plugins.pipelineprocessor.functions.ProcessorFunctionsModule;
import org.graylog.plugins.pipelineprocessor.periodical.LegacyDefaultStreamMigration;
import org.graylog.plugins.pipelineprocessor.processors.PipelineInterpreter;
import org.graylog.plugins.pipelineprocessor.rest.PipelineConnectionsResource;
import org.graylog.plugins.pipelineprocessor.rest.PipelineResource;
import org.graylog.plugins.pipelineprocessor.rest.PipelineRestPermissions;
import org.graylog.plugins.pipelineprocessor.rest.RuleResource;
import org.graylog.plugins.pipelineprocessor.rest.SimulatorResource;
import org.graylog.plugins.pipelineprocessor.rulebuilder.RuleBuilderModule;
import org.graylog2.plugin.PluginModule;

/**
 * 当安装MessageProcessorModule时 被间接安装的Module
 */
public class PipelineProcessorModule extends PluginModule {
    @Override
    protected void configure() {
        addPeriodical(LegacyDefaultStreamMigration.class);

        // 该对象获取stream关联的一组pipeline 并加载pipeline中每个stage相关的rule 执行表达式和函数
        addMessageProcessor(PipelineInterpreter.class, PipelineInterpreter.Descriptor.class);

        // 通过该对象可以获得权限信息
        addPermissions(PipelineRestPermissions.class);

        // 绑定rest服务
        addSystemRestResource(PipelineConnectionsResource.class);
        addSystemRestResource(PipelineResource.class);
        addSystemRestResource(RuleResource.class);
        addSystemRestResource(SimulatorResource.class);

        // 触发处理函数的设置
        install(new ProcessorFunctionsModule());
        // TODO
        install(new RuleBuilderModule());

        // 安装查询结果装饰器
        installSearchResponseDecorator(searchResponseDecoratorBinder(),
                PipelineProcessorMessageDecorator.class,
                PipelineProcessorMessageDecorator.Factory.class);

        install(new FactoryModuleBuilder().build(PipelineInterpreter.State.Factory.class));

        addAuditEventTypes(PipelineProcessorAuditEventTypes.class);
    }
}
