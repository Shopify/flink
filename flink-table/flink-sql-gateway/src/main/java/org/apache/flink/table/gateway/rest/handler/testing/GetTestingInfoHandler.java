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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.testing.TestingApi;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.message.testing.TestingInfoResponseBody;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler for {@code GET /v1/testing/info}. */
public class GetTestingInfoHandler
        extends AbstractSqlGatewayRestHandler<
                EmptyRequestBody, TestingInfoResponseBody, EmptyMessageParameters> {

    public GetTestingInfoHandler(
            SqlGatewayService service,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, TestingInfoResponseBody, EmptyMessageParameters>
                    messageHeaders) {
        super(service, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<TestingInfoResponseBody> handleRequest(
            @Nullable SqlGatewayRestAPIVersion version,
            @Nonnull HandlerRequest<EmptyRequestBody> request) {
        return CompletableFuture.completedFuture(
                new TestingInfoResponseBody(TestingApi.CONTRACT_VERSION));
    }
}
