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

package org.apache.flink.table.gateway.rest.message.testing;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/** Response body for {@code POST /v1/testing/compile}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompileTestResponseBody implements ResponseBody {

    private static final String FIELD_CONTRACT_VERSION = "contract_version";
    private static final String FIELD_CONFIG_STATEMENTS = "config_statements";
    private static final String FIELD_PIPELINE_STATEMENTS = "pipeline_statements";
    private static final String FIELD_QUERY_SQL = "query_sql";
    private static final String FIELD_WARNINGS = "warnings";
    private static final String FIELD_MOCKS = "mocks";

    @JsonProperty(FIELD_CONTRACT_VERSION)
    private final String contractVersion;

    @JsonProperty(FIELD_CONFIG_STATEMENTS)
    private final List<String> configStatements;

    @JsonProperty(FIELD_PIPELINE_STATEMENTS)
    private final List<String> pipelineStatements;

    @JsonProperty(FIELD_QUERY_SQL)
    private final String querySql;

    @JsonProperty(FIELD_WARNINGS)
    private final List<String> warnings;

    @JsonProperty(FIELD_MOCKS)
    private final List<MockSpecBody> mocks;

    @JsonCreator
    public CompileTestResponseBody(
            @JsonProperty(FIELD_CONTRACT_VERSION) String contractVersion,
            @JsonProperty(FIELD_CONFIG_STATEMENTS) List<String> configStatements,
            @JsonProperty(FIELD_PIPELINE_STATEMENTS) List<String> pipelineStatements,
            @JsonProperty(FIELD_QUERY_SQL) String querySql,
            @JsonProperty(FIELD_WARNINGS) List<String> warnings,
            @JsonProperty(FIELD_MOCKS) List<MockSpecBody> mocks) {
        this.contractVersion = contractVersion;
        this.configStatements = configStatements;
        this.pipelineStatements = pipelineStatements;
        this.querySql = querySql;
        this.warnings = warnings;
        this.mocks = mocks;
    }

    public String getContractVersion() {
        return contractVersion;
    }

    public List<String> getConfigStatements() {
        return configStatements;
    }

    public List<String> getPipelineStatements() {
        return pipelineStatements;
    }

    public String getQuerySql() {
        return querySql;
    }

    public List<String> getWarnings() {
        return warnings;
    }

    public List<MockSpecBody> getMocks() {
        return mocks;
    }

    /** JSON representation of a mock specification. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class MockSpecBody {

        @JsonProperty("requested_name")
        private final String requestedName;

        @JsonProperty("session_object")
        private final String sessionObject;

        @JsonProperty("materialization")
        private final String materialization;

        @JsonProperty("schema")
        private final List<SchemaColumnBody> schema;

        @JsonProperty("watermark_column")
        @Nullable
        private final String watermarkColumn;

        @JsonProperty("include_sentinel")
        @Nullable
        private final Boolean includeSentinel;

        @JsonCreator
        public MockSpecBody(
                @JsonProperty("requested_name") String requestedName,
                @JsonProperty("session_object") String sessionObject,
                @JsonProperty("materialization") String materialization,
                @JsonProperty("schema") List<SchemaColumnBody> schema,
                @Nullable @JsonProperty("watermark_column") String watermarkColumn,
                @Nullable @JsonProperty("include_sentinel") Boolean includeSentinel) {
            this.requestedName = requestedName;
            this.sessionObject = sessionObject;
            this.materialization = materialization;
            this.schema = schema;
            this.watermarkColumn = watermarkColumn;
            this.includeSentinel = includeSentinel;
        }

        public String getRequestedName() {
            return requestedName;
        }

        public String getSessionObject() {
            return sessionObject;
        }

        public String getMaterialization() {
            return materialization;
        }

        public List<SchemaColumnBody> getSchema() {
            return schema;
        }

        @Nullable
        public String getWatermarkColumn() {
            return watermarkColumn;
        }

        @Nullable
        public Boolean getIncludeSentinel() {
            return includeSentinel;
        }
    }

    /** JSON representation of a schema column. */
    public static class SchemaColumnBody {

        @JsonProperty("name")
        private final String name;

        @JsonProperty("type")
        private final String type;

        @JsonCreator
        public SchemaColumnBody(
                @JsonProperty("name") String name, @JsonProperty("type") String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }
    }
}
