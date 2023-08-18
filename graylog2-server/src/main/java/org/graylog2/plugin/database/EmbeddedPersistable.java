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
package org.graylog2.plugin.database;

import java.util.Map;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 * 描述一个被内嵌到input的对象   比如 Extractor
 */
public interface EmbeddedPersistable {

    /**
     * 返回需要持久化的字段
     * @return
     */
    Map<String, Object> getPersistedFields();

}
