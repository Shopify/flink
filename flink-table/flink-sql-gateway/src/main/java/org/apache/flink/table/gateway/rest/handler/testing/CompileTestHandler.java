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

package org.apache.flink.table.gateway.rest.handler.testing;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.testing.TestCompileRequest;
import org.apache.flink.table.gateway.api.testing.TestCompileResponse;
import org.apache.flink.table.gateway.api.testing.TestMockSpec;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestRequestBody;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestResponseBody;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestResponseBody.MockSpecBody;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestResponseBody.SchemaColumnBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Handler for {@code POST /v1/testing/compile}. */
public class CompileTestHandler
        extends AbstractSqlGatewayRestHandler<
                CompileTestRequestBody, CompileTestResponseBody, EmptyMessageParameters> {

    public CompileTestHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            CompileTestRequestBody,
                            CompileTestResponseBody,
                            EmptyMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<CompileTestResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<CompileTestRequestBody> request)
            throws RestHandlerException {

        CompileTestRequestBody body = request.getRequestBody();
        TestCompileRequest apiRequest =
                new TestCompileRequest(
                        body.getStatements() != null
                                ? body.getStatements()
                                : Collections.emptyList(),
                        body.getTarget(),
                        body.getMode(),
                        body.getMockTargets() != null
                                ? body.getMockTargets()
                                : Collections.emptyList());

        try {
            TestCompileResponse apiResponse = service.compileTestPlan(apiRequest);
            return CompletableFuture.completedFuture(toResponseBody(apiResponse));
        } catch (IllegalArgumentException e) {
            throw new RestHandlerException(e.getMessage(), HttpResponseStatus.BAD_REQUEST, e);
        } catch (Exception e) {
            throw new RestHandlerException(
                    e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    private static CompileTestResponseBody toResponseBody(TestCompileResponse response) {
        return new CompileTestResponseBody(
                response.getContractVersion(),
                response.getSessionStatements(),
                response.getQuerySql(),
                response.getWarnings(),
                response.getMocks().stream()
                        .map(CompileTestHandler::toMockSpecBody)
                        .collect(Collectors.toList()));
    }

    private static MockSpecBody toMockSpecBody(TestMockSpec spec) {
        return new MockSpecBody(
                spec.getRequestedName(),
                spec.getSessionObject(),
                spec.getMaterialization(),
                spec.getSchema().stream()
                        .map(col -> new SchemaColumnBody(col.getName(), col.getType()))
                        .collect(Collectors.toList()),
                spec.getWatermarkColumn(),
                spec.isIncludeSentinel() ? true : null);
    }
}
