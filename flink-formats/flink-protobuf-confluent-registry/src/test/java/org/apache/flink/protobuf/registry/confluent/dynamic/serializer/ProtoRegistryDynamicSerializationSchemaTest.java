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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ProtoRegistryDynamicSerializationSchemaTest {
    private static final String CLASS_NAME = "TestClass";
    private static final String SUBJECT_NAME = "testSubject";
    private static final String SCHEMA_REGISTRY_URL = "http://registry:8081";

    private MockSchemaRegistryClient mockSchemaRegistryClient;

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Test
    public void testSerialize() throws Exception {
        RowType rowType = TestUtils.createRowType(
                new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
                new RowType.RowField(TestUtils.INT_FIELD, new IntType())
        );
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema = new ProtoRegistryDynamicSerializationSchema(
                TestUtils.DEFAULT_PACKAGE, CLASS_NAME, rowType, SUBJECT_NAME, mockSchemaRegistryClient, SCHEMA_REGISTRY_URL);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, StringData.fromString(TestUtils.TEST_STRING));
        rowData.setField(1, TestUtils.TEST_INT);

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        ProtobufSchema actualRegisteredSchema = (ProtobufSchema) mockSchemaRegistryClient.getSchemas(SUBJECT_NAME, false, true).get(0);
        ProtobufSchema expectedSchema = new ProtobufSchema(
                "syntax = \"proto3\";\n" +
                "package " + TestUtils.DEFAULT_PACKAGE + ";\n" +
                "option java_package = \"org.apache.flink.formats.protobuf.proto\";\n" +
                "option java_outer_classname = \"TestClass_OuterClass\";\n" +
                "option java_multiple_files = false;" +
                "message " + CLASS_NAME + "{\n" +
                "  string string = 1;\n" +
                "  int32 int = 2;\n" +
                "}\n");
        Assertions.assertEquals(expectedSchema, actualRegisteredSchema);

        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        KafkaProtobufDeserializer deser = new KafkaProtobufDeserializer(mockSchemaRegistryClient, opts);
        Message message = deser.deserialize(null, actualBytes);
        Descriptors.FieldDescriptor stringField = message.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);
        Descriptors.FieldDescriptor intField = message.getDescriptorForType().findFieldByName(TestUtils.INT_FIELD);

        Assertions.assertEquals(TestUtils.TEST_STRING, message.getField(stringField));
        Assertions.assertEquals(TestUtils.TEST_INT, message.getField(intField));

    }

}
