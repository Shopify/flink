/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.api.testing;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Response from compiling a test plan.
 *
 * <p>Contains everything the CLI needs to set up an execution session: mock specifications with
 * exact schemas, the ordered DDL statements to register between mocks and the target, and the query
 * to execute.
 */
public final class TestCompileResponse {

    private final String contractVersion;
    private final List<String> sessionStatements;
    private final String querySql;
    private final List<String> warnings;
    private final List<TestMockSpec> mocks;

    public TestCompileResponse(
            String contractVersion,
            List<String> sessionStatements,
            String querySql,
            List<String> warnings,
            List<TestMockSpec> mocks) {
        this.contractVersion = Objects.requireNonNull(contractVersion);
        this.sessionStatements =
                Collections.unmodifiableList(Objects.requireNonNull(sessionStatements));
        this.querySql = Objects.requireNonNull(querySql);
        this.warnings = Collections.unmodifiableList(Objects.requireNonNull(warnings));
        this.mocks = Collections.unmodifiableList(Objects.requireNonNull(mocks));
    }

    /** The testing API contract version. */
    public String getContractVersion() {
        return contractVersion;
    }

    /**
     * Ordered DDL statements the CLI should register after creating mocks. Includes view
     * definitions between mock boundaries and the target, plus any config/function statements.
     */
    public List<String> getSessionStatements() {
        return sessionStatements;
    }

    /** The final query to execute against the target. */
    public String getQuerySql() {
        return querySql;
    }

    /** Non-fatal warnings (e.g., temporal join detection). */
    public List<String> getWarnings() {
        return warnings;
    }

    /** Mock specifications with exact resolved schemas and materialization strategies. */
    public List<TestMockSpec> getMocks() {
        return mocks;
    }
}
