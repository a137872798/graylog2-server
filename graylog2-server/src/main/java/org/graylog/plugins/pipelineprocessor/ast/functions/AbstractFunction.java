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
package org.graylog.plugins.pipelineprocessor.ast.functions;

import org.graylog.plugins.pipelineprocessor.EvaluationContext;
import org.graylog.plugins.pipelineprocessor.ast.expressions.Expression;

/**
 * Helper Function implementation which evaluates and memoizes all constant FunctionArgs.
 *
 * @param <T> the return type
 */
public abstract class AbstractFunction<T> implements Function<T> {

    /**
     * 常量表达式直接调用 evaluateUnsafe 一般就能得到一个常量结果
     * @param args the function args for this functions, usually you don't need this
     * @param name the name of the argument to potentially precompute
     * @param arg the expression tree for the argument
     * @return
     */
    @Override
    public Object preComputeConstantArgument(FunctionArgs args, String name, Expression arg) {
        return arg.evaluateUnsafe(EvaluationContext.emptyContext());
    }
}
