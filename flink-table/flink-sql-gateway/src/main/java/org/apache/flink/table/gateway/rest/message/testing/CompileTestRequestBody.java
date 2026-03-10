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

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Request body for {@code POST /v1/testing/compile}. */
public class CompileTestRequestBody implements RequestBody {

    private static final String FIELD_STATEMENTS = "statements";
    private static final String FIELD_TARGET = "target";
    private static final String FIELD_MODE = "mode";
    private static final String FIELD_MOCK_TARGETS = "mock_targets";

    @JsonProperty(FIELD_STATEMENTS)
    private final List<String> statements;

    @JsonProperty(FIELD_TARGET)
    private final String target;

    @JsonProperty(FIELD_MODE)
    private final String mode;

    @JsonProperty(FIELD_MOCK_TARGETS)
    private final List<String> mockTargets;

    @JsonCreator
    public CompileTestRequestBody(
            @JsonProperty(FIELD_STATEMENTS) List<String> statements,
            @JsonProperty(FIELD_TARGET) String target,
            @JsonProperty(FIELD_MODE) String mode,
            @JsonProperty(FIELD_MOCK_TARGETS) List<String> mockTargets) {
        this.statements = statements;
        this.target = target;
        this.mode = mode;
        this.mockTargets = mockTargets;
    }

    public List<String> getStatements() {
        return statements;
    }

    public String getTarget() {
        return target;
    }

    public String getMode() {
        return mode;
    }

    public List<String> getMockTargets() {
        return mockTargets;
    }
}
