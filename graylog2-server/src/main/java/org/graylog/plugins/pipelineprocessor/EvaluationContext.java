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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.graylog.plugins.pipelineprocessor.ast.Rule;
import org.graylog.plugins.pipelineprocessor.ast.exceptions.FunctionEvaluationException;
import org.graylog.plugins.pipelineprocessor.ast.expressions.Expression;
import org.graylog.plugins.pipelineprocessor.ast.functions.FunctionDescriptor;
import org.graylog2.plugin.EmptyMessages;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.MessageCollection;
import org.graylog2.plugin.Messages;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.graylog2.shared.utilities.ExceptionUtils.getRootCause;
import static org.graylog2.shared.utilities.StringUtils.f;

/**
 * 一个评估/计算用的上下文对象
 */
public class EvaluationContext {

    private static final EvaluationContext EMPTY_CONTEXT = new EvaluationContext() {
        @Override
        public void addCreatedMessage(Message newMessage) {
            // cannot add messages to empty context
        }

        @Override
        public void define(String identifier, Class type, Object value) {
            // cannot define any variables in empty context
        }
    };

    /**
     * 描述被计算的消息体
     */
    @Nonnull
    private final Message message;
    /**
     * 描述某个类型以及数值
     */
    @Nullable
    private Map<String, TypedValue> ruleVars;
    @Nullable
    private List<Message> createdMessages;
    /**
     * 记录调用函数产生的各种错误
     */
    @Nullable
    private List<EvalError> evalErrors;
    @Nullable
    private Rule currentRule;

    public void setRule(Rule rule) {
        currentRule = rule;
    }

    public Rule getRule() {
        return currentRule;
    }

    private EvaluationContext() {
        this(new Message("__dummy", "__dummy", DateTime.parse("2010-07-30T16:03:25Z"))); // first Graylog release
    }

    public EvaluationContext(@Nonnull Message message) {
        this.message = message;
    }

    /**
     * 追加某个类型的参数
     * @param identifier
     * @param type
     * @param value
     */
    public void define(String identifier, Class type, Object value) {
        if (ruleVars == null) {
            ruleVars = Maps.newHashMap();
        }
        ruleVars.put(identifier, new TypedValue(type, value));
    }

    public Message currentMessage() {
        return message;
    }

    /**
     * 获取某个参数
     * @param identifier
     * @return
     */
    public TypedValue get(String identifier) {
        if (ruleVars == null) {
            throw new IllegalStateException("Use of undeclared variable " + identifier);
        }
        return ruleVars.get(identifier);
    }

    /**
     * 返回createdMessages   createdMessages存储了在计算过程中衍生出的各种消息  之前为message添加静态字段，字段抽取 只是为了后面做准备
     * @return
     */
    public Messages createdMessages() {
        if (createdMessages == null) {
            return new EmptyMessages();
        }
        return new MessageCollection(createdMessages);
    }

    /**
     * 追加一个message
     * @param newMessage
     */
    public void addCreatedMessage(Message newMessage) {
        if (createdMessages == null) {
            createdMessages = Lists.newArrayList();
        }
        createdMessages.add(newMessage);
    }

    /**
     * 清空创建的所有消息
     */
    public void clearCreatedMessages() {
        if (createdMessages != null) {
            createdMessages.clear();
        }
    }

    public static EvaluationContext emptyContext() {
        return EMPTY_CONTEXT;
    }

    /**
     * 记录失败函数作用的行列 以及函数的描述信息
     * @param line
     * @param charPositionInLine
     * @param descriptor
     * @param e
     */
    public void addEvaluationError(int line, int charPositionInLine, @Nullable FunctionDescriptor descriptor, Throwable e) {
        if (evalErrors == null) {
            evalErrors = Lists.newArrayList();
        }
        evalErrors.add(new EvalError(line, charPositionInLine, descriptor, e));
    }

    /**
     * 当在计算过程中产生异常时  触发该方法
     * @param exception
     * @param expression
     */
    public void onEvaluationException(Exception exception, Expression expression) {
        // 发现异常是 FunctionEvaluationException 类型的
        if (exception instanceof FunctionEvaluationException) {
            final FunctionEvaluationException fee = (FunctionEvaluationException) exception;
            addEvaluationError(fee.getStartToken().getLine(),
                    fee.getStartToken().getCharPositionInLine(),
                    fee.getFunctionExpression().getFunction().descriptor(),
                    getRootCause(fee));
        } else {
            // 普通异常 没有descriptor信息
            addEvaluationError(
                    expression.getStartToken().getLine(),
                    expression.getStartToken().getCharPositionInLine(),
                    null,
                    getRootCause(exception));
        }
    }

    public boolean hasEvaluationErrors() {
        return evalErrors != null;
    }

    public List<EvalError> evaluationErrors() {
        return evalErrors == null ? Collections.emptyList() : Collections.unmodifiableList(evalErrors);
    }

    /**
     * 获取最后一个计算异常信息
     * @return
     */
    @Nullable
    public EvalError lastEvaluationError() {
        return evalErrors == null || evalErrors.isEmpty() ? null
                : evalErrors.get(evalErrors.size() - 1);
    }

    public static class TypedValue {
        private final Class type;
        private final Object value;

        public TypedValue(Class type, Object value) {
            this.type = type;
            this.value = value;
        }

        public Class getType() {
            return type;
        }

        public Object getValue() {
            return value;
        }
    }

    public static class EvalError {

        // 描述第几行第几列
        private final int line;
        private final int charPositionInLine;

        /**
         * 描述调用的函数信息
         */
        @Nullable
        private final FunctionDescriptor descriptor;
        private final Throwable throwable;

        public EvalError(int line, int charPositionInLine, @Nullable FunctionDescriptor descriptor, Throwable throwable) {
            this.line = line;
            this.charPositionInLine = charPositionInLine;
            this.descriptor = descriptor;
            this.throwable = throwable;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            if (descriptor != null) {
                sb.append("In call to function '").append(descriptor.name()).append("' at ");
            } else {
                sb.append("At ");
            }
            return sb.append(line)
                    .append(":")
                    .append(charPositionInLine)
                    .append(" an exception was thrown: ")
                    .append(throwable.getMessage())
                    .toString();
        }
    }

    public String pipelineErrorMessage(String msg) {
        if (currentRule != null) {
            return f("Rule <%s> %s", currentRule.name(), msg);
        }
        return msg;
    }
}
