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

import org.graylog2.plugin.database.validators.Validator;

import java.util.Map;

/**
 * @author Dennis Oelkers <dennis@torch.sh>
 *     表示一个被持久化的对象
 */
public interface Persisted {
    /**
     * 该对象应该有唯一标识
     * @return
     */
    String getId();

    /**
     * 该对象应当有很多字段
     * @return
     */
    Map<String, Object> getFields();

    /**
     * 针对某些字段可能有校验器
     * @return
     */
    Map<String, Validator> getValidations();

    /**
     * 某个字段内可能还有内嵌的校验器
     * @param key
     * @return
     */
    Map<String, Validator> getEmbeddedValidations(String key);

    /**
     * 将结果以map的形式返回
     * @return
     */
    Map<String, Object> asMap();
}
