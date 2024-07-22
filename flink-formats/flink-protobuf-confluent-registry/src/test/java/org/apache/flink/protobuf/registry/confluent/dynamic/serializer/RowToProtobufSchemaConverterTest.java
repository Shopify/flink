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

package org.apache.flink.protobuf.registry.confluent.dynamic.serializer;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RowToProtobufSchemaConverterTest {

    @Test
    public void testFlatRowType() throws Exception {
        RowType rowType = TestUtils.createRowType(
            new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
            new RowType.RowField(TestUtils.INT_FIELD, new IntType())
        );
        RowToProtobufSchemaConverter converter = new RowToProtobufSchemaConverter(
                TestUtils.DEFAULT_PACKAGE, TestUtils.DEFAULT_CLASS_NAME, rowType);
        ProtobufSchema actual = converter.convert();
        ProtobufSchema expected = new ProtobufSchema(
                sharedSchemaComponents() +
                "  string" + TestUtils.STRING_FIELD + "= 1;\n" +
                "  int32" + TestUtils.INT_FIELD + "= 2;\n" +
                "}\n");
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testGoogleTimestampField() throws Exception {
        String tsField = "ts";
        RowType rowType = TestUtils.createRowType(
                new RowType.RowField(tsField, new TimestampType())
        );
        RowToProtobufSchemaConverter converter = new RowToProtobufSchemaConverter(
                TestUtils.DEFAULT_PACKAGE, TestUtils.DEFAULT_CLASS_NAME, rowType);
        ProtobufSchema actual = converter.convert();
        ProtobufSchema expected = new ProtobufSchema(
                sharedSchemaComponents() +
                        "  google.protobuf.Timestamp " + tsField + "= 1;\n" +
                        "}\n");
        Assertions.assertEquals(expected, actual);
    }

    private static String sharedSchemaComponents() {
        return "syntax = \"proto3\";\n" +
                "package " + TestUtils.DEFAULT_PACKAGE + ";\n" +
                "import \"google/protobuf/timestamp.proto\";" +
                "message " + TestUtils.DEFAULT_CLASS_NAME + "{\n" ;
    }
}
