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
package org.graylog2.plugin.buffers;

import org.graylog2.plugin.journal.RawMessage;

/**
 * 可以将原始数据插入到buffer中
 */
public interface InputBuffer extends EventBuffer {

    /**
     * 原始消息处理后 会设置到buffer中
     * @param message
     */
    void insert(RawMessage message);
}
