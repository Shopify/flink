---
title: "REST API"
weight: 8
type: docs
aliases:
  - /ops/rest_api.html
  - /internals/monitoring_rest_api.html
  - /monitoring/rest_api.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# REST API

Flink has a monitoring API that can be used to query status and statistics of running jobs, as well as recent completed jobs.
This monitoring API is used by Flink's own dashboard, but is designed to be used also by custom monitoring tools.

The monitoring API is a REST-ful API that accepts HTTP requests and responds with JSON data.

## Overview

The monitoring API is backed by a web server that runs as part of the *JobManager*. By default, this server listens at port `8081`, which can be configured in [Flink configuration file]({{< ref "docs/deployment/config#flink-configuration-file" >}}) via `rest.port`. Note that the monitoring API web server and the web dashboard web server are currently the same and thus run together at the same port. They respond to different HTTP URLs, though.

In the case of multiple JobManagers (for high availability), each JobManager will run its own instance of the monitoring API, which offers information about completed and running job while that JobManager was elected the cluster leader.


## Developing

The REST API backend is in the `flink-runtime` project. The core class is `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint`, which sets up the server and the request routing.

We use *Netty* and the *Netty Router* library to handle REST requests and translate URLs. This choice was made because this combination has lightweight dependencies, and the performance of Netty HTTP is very good.

To add new requests, one needs to
* add a new `MessageHeaders` class which serves as an interface for the new request,
* add a new `AbstractRestHandler` class which handles the request according to the added `MessageHeaders` class,
* add the handler to `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint#initializeHandlers()`.

A good example is the `org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler` that uses the `org.apache.flink.runtime.rest.messages.JobExceptionsHeaders`.


## API

The REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. Prefixes are always of the form `v[version_number]`.
For example, to access version 1 of `/foo/bar` one would query `/v1/foo/bar`.

If no version is specified Flink will default to the *oldest* version supporting the request.

Querying unsupported/non-existing versions will return a 404 error.

There exist several async operations among these APIs, e.g. `trigger savepoint`, `rescale a job`. They would return a `triggerid` to identify the operation you just POST and then you need to use that `triggerid` to query for the status of the operation.

For (stop-with-)savepoint operations you can control this `triggerId` by setting it in the body of the request that triggers the operation.
This allow you to safely* retry such operations without triggering multiple savepoints.

{{< hint info >}}
The retry is only safe until the [async operation store duration]({{< ref "docs/deployment/config#rest-async-store-duration" >}}) has elapsed.
{{</ hint >}}

### JobManager

[OpenAPI specification]({{< ref_static "generated/rest_v1_dispatcher.yml" >}})

{{< hint warning >}}
The OpenAPI specification is still experimental.
{{< /hint >}}

#### API reference

{{< tabs "f00ed142-b05f-44f0-bafc-799080c1d40d" >}}
{{< tab "v1" >}}

{{< generated/rest_v1_dispatcher >}}

{{< /tab >}}
{{< /tabs >}}

