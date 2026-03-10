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

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Specification for a required mock in a compiled test plan.
 *
 * <p>The mock name matches the original pipeline object name so it can be created as a direct
 * replacement in the execution session. The schema contains the exact resolved column types from the
 * Flink catalog.
 */
public final class TestMockSpec {

    /** Create mock as a VALUES-based temporary view. Suitable for batch and non-windowed cases. */
    public static final String MATERIALIZATION_VIEW = "view";

    /**
     * Create mock as a filesystem-backed temporary table with watermarks. Required for windowed
     * streaming/changelog tests where watermark advancement is needed.
     */
    public static final String MATERIALIZATION_FILESYSTEM = "filesystem";

    private final String requestedName;
    private final String sessionObject;
    private final String materialization;
    private final List<TestSchemaColumn> schema;
    @Nullable private final String watermarkColumn;
    private final boolean includeSentinel;

    public TestMockSpec(
            String requestedName,
            String sessionObject,
            String materialization,
            List<TestSchemaColumn> schema,
            @Nullable String watermarkColumn,
            boolean includeSentinel) {
        this.requestedName = Objects.requireNonNull(requestedName, "requestedName");
        this.sessionObject = Objects.requireNonNull(sessionObject, "sessionObject");
        this.materialization = Objects.requireNonNull(materialization, "materialization");
        this.schema = Collections.unmodifiableList(Objects.requireNonNull(schema, "schema"));
        this.watermarkColumn = watermarkColumn;
        this.includeSentinel = includeSentinel;
    }

    /** The canonical name of the mock target as specified in the test suite. */
    public String getRequestedName() {
        return requestedName;
    }

    /**
     * The SQL identifier the toolkit should use when creating the mock object. For v1 (top-level
     * targets only), this matches {@code requestedName}. For v2 with CTE targeting, this may differ
     * — e.g., a CTE mock like {@code charges_view.base_deduped} would need a dot-free session
     * identifier.
     */
    public String getSessionObject() {
        return sessionObject;
    }

    /** The materialization strategy: {@code "view"} or {@code "filesystem"}. */
    public String getMaterialization() {
        return materialization;
    }

    /** The exact resolved schema columns with SQL type strings. */
    public List<TestSchemaColumn> getSchema() {
        return schema;
    }

    /** The watermark column name, if filesystem materialization requires watermarks. */
    @Nullable
    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    /** Whether a sentinel row should be appended to advance watermarks past all windows. */
    public boolean isIncludeSentinel() {
        return includeSentinel;
    }
}
