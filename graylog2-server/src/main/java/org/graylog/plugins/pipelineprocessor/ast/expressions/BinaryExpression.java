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

import com.google.common.collect.ImmutableList;

import org.antlr.v4.runtime.Token;

/**
 * 二元表达式 内部包含 left/right 2个表达式
 */
public abstract class BinaryExpression extends UnaryExpression {

    /**
     * 左边表达式
     */
    protected Expression left;

    public BinaryExpression(Token start, Expression left, Expression right) {
        super(start, right);
        this.left = left;
    }

    @Override
    public boolean isConstant() {
        return left.isConstant() && right.isConstant();
    }

    public Expression left() {
        return left;
    }

    public void left(Expression left) {
        this.left = left;
    }
    @Override
    public Iterable<Expression> children() {
        return ImmutableList.of(left, right);
    }
}
