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
 * Request to compile a test plan for a pipeline target.
 *
 * <p>The CLI pre-splits the rendered pipeline SQL into individual statements and sends them in
 * pipeline order (tables before views). The gateway loads them into an analysis session to resolve
 * schemas and dependencies.
 */
public final class TestCompileRequest {

    private final List<String> statements;
    private final String target;
    private final String mode;
    private final List<String> mockTargets;

    public TestCompileRequest(
            List<String> statements, String target, String mode, List<String> mockTargets) {
        this.statements = Collections.unmodifiableList(Objects.requireNonNull(statements));
        this.target = Objects.requireNonNull(target);
        this.mode = Objects.requireNonNull(mode);
        this.mockTargets = Collections.unmodifiableList(Objects.requireNonNull(mockTargets));
    }

    /** Pre-split pipeline SQL statements in dependency order. */
    public List<String> getStatements() {
        return statements;
    }

    /** The top-level view or table to test. */
    public String getTarget() {
        return target;
    }

    /** Execution mode: {@code "batch"} or {@code "changelog"}. */
    public String getMode() {
        return mode;
    }

    /** Names of pipeline objects to be replaced with mock data. */
    public List<String> getMockTargets() {
        return mockTargets;
    }
}
