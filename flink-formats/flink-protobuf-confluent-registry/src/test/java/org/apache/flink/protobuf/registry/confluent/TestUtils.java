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

package org.apache.flink.protobuf.registry.confluent;

import com.google.protobuf.ByteString;

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtils {

    static public final String TEST_STRING = "test";
    static public final int TEST_INT = 42;
    static public final long TEST_LONG = 99L;
    static public final float TEST_FLOAT = 3.14f;
    static public final double TEST_DOUBLE = 2.71828;
    static public final boolean TEST_BOOL = true;
    static public final ByteString TEST_BYTES = ByteString.copyFrom(new byte[] {0x01, 0x02, 0x03, 0x04});

    public static final String STRING_FIELD = "string";
    public static final String INT_FIELD = "int";
    public static final String LONG_FIELD = "long";
    public static final String FLOAT_FIELD = "float";
    public static final String DOUBLE_FIELD = "double";
    public static final String BOOL_FIELD = "bool";
    public static final String BYTES_FIELD = "bytes";
    public static final String NESTED_FIELD = "nested";
    public static final String ARRAY_FIELD = "array";
    public static final String MAP_FIELD = "map";
    public static final String TIMESTAMP_FIELD = "ts";
    public static final String SECONDS_FIELD = "seconds";
    public static final String NANOS_FIELD = "nanos";

    static public final String DEFAULT_PACKAGE = "org.apache.flink.formats.protobuf.proto";
    static public final int DEFAULT_SCHEMA_ID = 1;
    static public final String DEFAULT_CLASS_SUFFIX = "123";
    static public final String DEFAULT_CLASS_NAME = "TestClass";

    public static RowType createRowType(RowType.RowField... fields) {
        List<RowType.RowField> fieldList = new ArrayList<>();
        fieldList.addAll(Arrays.asList(fields));
        return new RowType(fieldList);
    }

}
