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

package org.apache.flink.table.gateway.rest.header.testing;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestRequestBody;
import org.apache.flink.table.gateway.rest.message.testing.CompileTestResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for {@code POST /v1/testing/compile}. */
public class CompileTestHeaders
        implements SqlGatewayMessageHeaders<
                CompileTestRequestBody, CompileTestResponseBody, EmptyMessageParameters> {

    private static final CompileTestHeaders INSTANCE = new CompileTestHeaders();

    @Override
    public Class<CompileTestResponseBody> getResponseClass() {
        return CompileTestResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Compile a test plan for a pipeline target.";
    }

    @Override
    public Class<CompileTestRequestBody> getRequestClass() {
        return CompileTestRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return "/testing/compile";
    }

    public static CompileTestHeaders getInstance() {
        return INSTANCE;
    }

    @Override
    public String operationId() {
        return "compileTestPlan";
    }
}
