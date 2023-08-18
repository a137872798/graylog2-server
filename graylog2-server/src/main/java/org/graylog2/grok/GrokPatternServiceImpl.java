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

import com.google.common.base.Strings;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.exception.GrokException;
import org.graylog2.plugin.database.ValidationException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.graylog2.grok.GrokPatternService.ImportStrategy.DROP_ALL_EXISTING;

/**
 * 正则服务模板
 */
abstract class GrokPatternServiceImpl implements GrokPatternService {


    @Override
    public Map<String, Object> match(GrokPattern pattern, String sampleData) throws GrokException {
        // 除了本次传入的 之前存储的所有也会尝试使用
        final Set<GrokPattern> patterns = loadAll();
        // 该对象可以将普通的正则表达式转换成 grok的正则对象
        final GrokCompiler grokCompiler = GrokCompiler.newInstance();
        for (GrokPattern storedPattern : patterns) {
            grokCompiler.register(storedPattern.name(), storedPattern.pattern());
        }
        grokCompiler.register(pattern.name(), pattern.pattern());
        Grok grok = grokCompiler.compile("%{" + pattern.name() + "}");
        return grok.match(sampleData).captureFlattened();
    }

    /**
     * 检验正则有效性
     * @param pattern
     * @return
     * @throws GrokException  报错代表表达式本身有问题
     */
    @Override
    public boolean validate(GrokPattern pattern) throws GrokException {
        checkNotNull(pattern, "A pattern must be given");
        checkArgument(!pattern.name().contains(" "), "Pattern name must not contain spaces");

        final Set<GrokPattern> patterns = loadAll();
        final boolean fieldsMissing = Strings.isNullOrEmpty(pattern.name()) || Strings.isNullOrEmpty(pattern.pattern());
        final GrokCompiler grokCompiler = GrokCompiler.newInstance();
        for (GrokPattern storedPattern : patterns) {
            grokCompiler.register(storedPattern.name(), storedPattern.pattern());
        }
        grokCompiler.register(pattern.name(), pattern.pattern());
        grokCompiler.compile("%{" + pattern.name() + "}");
        return !fieldsMissing;
    }

    @Override
    public boolean validateAll(Collection<GrokPattern> newPatterns) throws GrokException {
        final Set<GrokPattern> patterns = loadAll();
        final GrokCompiler grokCompiler = GrokCompiler.newInstance();

        for (GrokPattern newPattern : newPatterns) {
            final boolean fieldsMissing = Strings.isNullOrEmpty(newPattern.name()) || Strings.isNullOrEmpty(newPattern.pattern());
            if (fieldsMissing) {
                return false;
            }
            grokCompiler.register(newPattern.name(), newPattern.pattern());
        }
        for (GrokPattern storedPattern : patterns) {
            grokCompiler.register(storedPattern.name(), storedPattern.pattern());
        }
        for (GrokPattern newPattern : newPatterns) {
            grokCompiler.compile("%{" + newPattern.name() + "}");
        }
        return true;
    }

    protected void validateAllOrThrow(Collection<GrokPattern> newPatterns, ImportStrategy importStrategy) throws ValidationException {
        try {
            if (!validateAll(newPatterns)) {
                throw new ValidationException("Patterns invalid.");
            }
        } catch (GrokException | PatternSyntaxException e) {
            throw new ValidationException("Invalid patterns.\n" + e.getMessage());
        }

        if (importStrategy == DROP_ALL_EXISTING) {
            deleteAll();
        }
    }
}
