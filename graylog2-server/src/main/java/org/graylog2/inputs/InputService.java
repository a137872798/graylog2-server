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
package org.graylog2.inputs;

import org.graylog2.database.NotFoundException;
import org.graylog2.plugin.database.Persisted;
import org.graylog2.plugin.database.PersistedService;
import org.graylog2.plugin.database.ValidationException;
import org.graylog2.plugin.inputs.Extractor;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.shared.inputs.NoSuchInputTypeException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * PersistedService 开放了一些存储和查询的api 这里进一步提供create/update
 */
public interface InputService extends PersistedService {

    /**
     * 返回所有input
     * @return
     */
    List<Input> all();

    /**
     * 返回某个节点关联的所有input
     * @param nodeId
     * @return
     */
    List<Input> allOfThisNode(String nodeId);

    Input create(String id, Map<String, Object> fields);

    Input create(Map<String, Object> fields);

    <T extends Persisted> String saveWithoutEvents(T model) throws ValidationException;

    /**
     * 更新某个model
     * @param model
     * @return
     * @throws ValidationException
     */
    String update(Input model) throws ValidationException;

    /**
     * 从持久层检索某个model
     * @param id
     * @return
     * @throws NotFoundException
     */
    Input find(String id) throws NotFoundException;

    /**
     * 通过type匹配input
     * @param type
     * @return
     */
    default List<Input> allByType(String type) {
        return all().stream().filter(input -> Objects.equals(input.getType(), type)).toList();
    }

    /**
     * 通过id匹配input
     * @param ids
     * @return
     */
    Set<Input> findByIds(Collection<String> ids);

    Input findForThisNode(String nodeId, String id) throws NotFoundException;

    /**
     * 从某个节点 或者全局查找
     * @param nodeId
     * @param id
     * @return
     * @throws NotFoundException
     */
    Input findForThisNodeOrGlobal(String nodeId, String id) throws NotFoundException;

    /**
     * 返回集群中的总量
     * @return the total number of inputs in the cluster (including global inputs).
     */
    long totalCount();

    /**
     * @return the number of global inputs in the cluster.
     */
    long globalCount();

    /**
     * @return the number of node-specific inputs in the cluster.
     */
    long localCount();

    /**
     * @return the total number of inputs in the cluster grouped by type.
     */
    Map<String, Long> totalCountByType();

    /**
     * @param nodeId the node ID to query
     * @return the number of inputs on the specified node
     */
    long localCountForNode(String nodeId);

    /**
     * @param nodeId the node ID to query
     * @return the number of inputs on the specified node (including global inputs)
     */
    long totalCountForNode(String nodeId);

    /**
     * @return the total number of extractors in the cluster (including global inputs).
     */
    long totalExtractorCount();

    /**
     * @return the total number of extractors in the cluster (including global inputs) grouped by type.
     */
    Map<Extractor.Type, Long> totalExtractorCountByType();

    void addExtractor(Input input, Extractor extractor) throws ValidationException;

    void addStaticField(Input input, String key, String value) throws ValidationException;

    List<Extractor> getExtractors(Input input);

    Extractor getExtractor(Input input, String extractorId) throws NotFoundException;

    void updateExtractor(Input input, Extractor extractor) throws ValidationException;

    void removeExtractor(Input input, String extractorId);

    void removeStaticField(Input input, String key);

    MessageInput getMessageInput(Input io) throws NoSuchInputTypeException;

    List<Map.Entry<String, String>> getStaticFields(Input input);
}
