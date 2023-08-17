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
package org.graylog2.grok;

import io.krakens.grok.api.GrokUtils;
import io.krakens.grok.api.exception.GrokException;
import org.graylog2.database.NotFoundException;
import org.graylog2.plugin.database.ValidationException;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * grok正则是logstash内部的正则语法
 */
public interface GrokPatternService {

    /**
     * 将正则对象导入到存储层时 描述存储策略的对象
     */
    enum ImportStrategy {
        ABORT_ON_CONFLICT, REPLACE_ON_CONFLICT, DROP_ALL_EXISTING
    }

    /**
     * 通过id从mongodb中加载正则对象
     * @param patternId
     * @return
     * @throws NotFoundException
     */
    GrokPattern load(String patternId) throws NotFoundException;

    Optional<GrokPattern> loadByName(String name);

    /**
     * 批量查询
     * @param patternIds
     * @return
     */
    Set<GrokPattern> bulkLoad(Collection<String> patternIds);

    /**
     * 返回所有正则
     * @return
     */
    Set<GrokPattern> loadAll();

    /**
     * 存储一个正则表达式对象
     * @param pattern
     * @return
     * @throws ValidationException
     */
    GrokPattern save(GrokPattern pattern) throws ValidationException;

    GrokPattern update(GrokPattern pattern) throws ValidationException;

    /**
     * 存储一组对象
     * @param patterns
     * @param importStrategy
     * @return
     * @throws ValidationException
     */
    List<GrokPattern> saveAll(Collection<GrokPattern> patterns, ImportStrategy importStrategy) throws ValidationException;

    /**
     * 判断某个数据是否满足正则条件
     * @param pattern
     * @param sampleData
     * @return
     * @throws GrokException
     */
    Map<String, Object> match(GrokPattern pattern, String sampleData) throws GrokException;

    /**
     * 校验正则表达式是否合法
     * @param pattern
     * @return
     * @throws GrokException
     */
    boolean validate(GrokPattern pattern) throws GrokException;

    /**
     * 批量校验
     * @param patterns
     * @return
     * @throws GrokException
     */
    boolean validateAll(Collection<GrokPattern> patterns) throws GrokException;

    /**
     * 删除某个表达式
     * @param patternId
     * @return
     */
    int delete(String patternId);

    int deleteAll();

    static Set<String> extractPatternNames(String namedPattern) {
        final Set<String> result = new HashSet<>();
        // We have to use java.util.Regex here to get the names because ".find()" on the "com.google.code.regexp.Matcher"
        // would run in an endless loop.
        final Set<String> namedGroups = GrokUtils.getNameGroups(GrokUtils.GROK_PATTERN.namedPattern());
        final Matcher matcher = Pattern.compile(GrokUtils.GROK_PATTERN.namedPattern()).matcher(namedPattern);

        // 抽取出多个名字 并返回
        while (matcher.find()) {
            final Map<String, String> group = namedGroups(matcher, namedGroups);
            final String patternName = group.get("pattern");
            result.add(patternName);
        }
        return result;
    }

    static Map<String, String> namedGroups(Matcher matcher, Set<String> groupNames) {
        Map<String, String> namedGroups = new LinkedHashMap<>();
        for (String groupName : groupNames) {
            String groupValue = matcher.group(groupName);
            namedGroups.put(groupName, groupValue);
        }
        return namedGroups;
    }
}
