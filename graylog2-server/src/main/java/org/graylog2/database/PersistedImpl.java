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
package org.graylog2.database;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.bson.types.ObjectId;
import org.graylog2.plugin.database.Persisted;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 表示一个被持久化的对象   只是一个基类
 */
public abstract class PersistedImpl implements Persisted {
    private static final Logger LOG = LoggerFactory.getLogger(PersistedImpl.class);

    /**
     * 该对象包含的字段  以及唯一id
     */
    protected final Map<String, Object> fields;
    /**
     * 借助mongodb包的能力 生成唯一id
     */
    protected final ObjectId id;

    /**
     * 以16进制的方式存储id
     */
    private final AtomicReference<String> hexId = new AtomicReference<>(null);

    protected PersistedImpl(@Nullable final Map<String, Object> fields) {
        this(new ObjectId(), fields);
    }

    protected PersistedImpl(final ObjectId id, @Nullable final Map<String, Object> fields) {
        this.id = id;

        if (null != this.id) {
            hexId.set(this.id.toHexString());
        }

        if(fields == null) {
            this.fields = new HashMap<>();
        } else {
            this.fields = new HashMap<>(fields.size());

            // Transform all java.util.Date's to JodaTime because MongoDB gives back java.util.Date's. #lol
            for (Map.Entry<String, Object> field : fields.entrySet()) {
                final String key = field.getKey();
                final Object value = field.getValue();
                if (value instanceof Date) {
                    this.fields.put(key, new DateTime(value, DateTimeZone.UTC));
                } else {
                    this.fields.put(key, value);
                }
            }
        }
    }

    protected ObjectId getObjectId() {
        return this.id;
    }

    /**
     * 总是返回16进制的形式
     * @return
     */
    @Override
    public String getId() {
        // Performance - toHexString is expensive so we cache it.
        final String s = hexId.get();
        if (s == null && id != null) {
            final String hexString = getObjectId().toHexString();
            hexId.compareAndSet(null, hexString);
            return hexString;
        }

        return s;
    }

    @Override
    @JsonIgnore
    public Map<String, Object> getFields() {
        return fields;
    }

    // 同时比较id 和 field
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof PersistedImpl)) {
            return false;
        }

        final PersistedImpl other = (PersistedImpl) o;
        return Objects.equals(fields, other.fields) && Objects.equals(getObjectId(), other.getObjectId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getObjectId(), getFields());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{" +
                "fields=" + getFields() +
                ", id=" + getId() +
                '}';
    }

    /**
     * 将内部字段返回
     * @return
     */
    @Override
    public Map<String, Object> asMap() {
        final Map<String, Object> result = new HashMap<>();

        // 优先使用getXXX方法获取field
        for (Method method : this.getClass().getMethods()) {
            if (method.getName().startsWith("get") && method.getParameterTypes().length == 0) {
                final String fieldName = method.getName().substring(3).toLowerCase(Locale.ENGLISH);
                try {
                    // 通过反射 调用所有 getXXX
                    result.put(fieldName, method.invoke(this));
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.debug("Error while accessing field", e);
                }
            }
        }

        // 还会读取所有field
        for (Field field : this.getClass().getFields()) {
            if (!result.containsKey(field.getName())) {
                try {
                    result.put(field.getName(), field.get(this));
                } catch (IllegalAccessException e) {
                    LOG.debug("Error while accessing field", e);
                }
            }
        }

        return result;
    }
}
