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

import org.antlr.v4.runtime.Token;
import org.graylog.plugins.pipelineprocessor.EvaluationContext;

/**
 * 二元表达式 表示 同时满足2个子表达式
 */
public class AndExpression extends BinaryExpression implements LogicalExpression {
    public AndExpression(Token start, Expression left,
                         Expression right) {
        super(start, left, right);
    }

    @Override
    public Object evaluateUnsafe(EvaluationContext context) {
        return evaluateBool(context);
    }

    @Override
    public boolean evaluateBool(EvaluationContext context) {
        // 能够被and表示的2个expression 必然都可以得到一个boolean结果
        return ((LogicalExpression)left).evaluateBool(context) && ((LogicalExpression)right).evaluateBool(context);
    }

    @Override
    public Class getType() {
        return Boolean.class;
    }

    @Override
    public String toString() {
        return left.toString() + " AND " + right.toString();
    }
}
