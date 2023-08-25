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
package org.graylog.plugins.pipelineprocessor.ast.expressions;

import org.graylog.plugins.pipelineprocessor.EvaluationContext;

/**
 * 表示一个数字
 */
public interface NumericExpression extends Expression {

    /**
     * 不可缺少的?
     * @return
     */
    boolean isIntegral();

    // 将数字转换成 long/double

    long evaluateLong(EvaluationContext context);

    double evaluateDouble(EvaluationContext context);
}
