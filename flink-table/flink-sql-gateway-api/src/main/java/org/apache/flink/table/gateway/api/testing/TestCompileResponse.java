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
 * <p>Contains everything the CLI needs to set up an execution session. The execution order is:
 *
 * <ol>
 *   <li>{@code configStatements} — session config, functions, USE statements (before mocks)
 *   <li>Mock creation — CLI creates mocks using {@code mocks} specifications
 *   <li>{@code pipelineStatements} — view DDL between mock boundaries and the target (after mocks)
 *   <li>Execute {@code querySql}
 * </ol>
 */
public final class TestCompileResponse {

    private final String contractVersion;
    private final List<String> configStatements;
    private final List<String> pipelineStatements;
    private final String querySql;
    private final List<String> warnings;
    private final List<TestMockSpec> mocks;

    public TestCompileResponse(
            String contractVersion,
            List<String> configStatements,
            List<String> pipelineStatements,
            String querySql,
            List<String> warnings,
            List<TestMockSpec> mocks) {
        this.contractVersion = Objects.requireNonNull(contractVersion);
        this.configStatements =
                Collections.unmodifiableList(Objects.requireNonNull(configStatements));
        this.pipelineStatements =
                Collections.unmodifiableList(Objects.requireNonNull(pipelineStatements));
        this.querySql = Objects.requireNonNull(querySql);
        this.warnings = Collections.unmodifiableList(Objects.requireNonNull(warnings));
        this.mocks = Collections.unmodifiableList(Objects.requireNonNull(mocks));
    }

    /** The testing API contract version. */
    public String getContractVersion() {
        return contractVersion;
    }

    /**
     * Config/function statements to execute BEFORE creating mocks. Includes SET, CREATE FUNCTION,
     * USE CATALOG, etc.
     */
    public List<String> getConfigStatements() {
        return configStatements;
    }

    /**
     * Pipeline view DDL to execute AFTER creating mocks. These are the views between mock
     * boundaries and the target, in dependency order.
     */
    public List<String> getPipelineStatements() {
        return pipelineStatements;
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
