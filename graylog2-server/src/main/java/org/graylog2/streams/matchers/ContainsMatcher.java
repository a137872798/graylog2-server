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
package org.graylog2.streams.matchers;

import org.graylog2.plugin.Message;
import org.graylog2.plugin.streams.StreamRule;

public class ContainsMatcher implements StreamRuleMatcher {

    /**
     * 该规则要求 消息必须包含 rule所指的某个字段
     * @param msg
     * @param rule
     * @return
     */
    @Override
    public boolean match(Message msg, StreamRule rule) {
        final boolean inverted = rule.getInverted();
        final Object field = msg.getField(rule.getField());

        if (field != null) {
            final String value = field.toString();
            return inverted ^ value.contains(rule.getValue());
        } else {
            return inverted;
        }
    }
}
