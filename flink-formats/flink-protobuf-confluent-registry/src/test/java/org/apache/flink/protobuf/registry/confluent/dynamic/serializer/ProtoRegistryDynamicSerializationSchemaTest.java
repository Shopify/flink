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
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ProtoRegistryDynamicSerializationSchemaTest {
    private static final String SUBJECT_NAME = "testSubject";
    private static final String SCHEMA_REGISTRY_URL = "http://registry:8081";

    private MockSchemaRegistryClient mockSchemaRegistryClient;

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
    }

    @Test
    public void testSerializePrimitiveTypes() throws Exception {
        RowType rowType = TestUtils.createRowType(
                new RowType.RowField(TestUtils.STRING_FIELD, new VarCharType()),
                new RowType.RowField(TestUtils.INT_FIELD, new IntType())
        );
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema = new ProtoRegistryDynamicSerializationSchema(
                TestUtils.DEFAULT_PACKAGE, TestUtils.DEFAULT_CLASS_NAME, rowType, SUBJECT_NAME, mockSchemaRegistryClient, SCHEMA_REGISTRY_URL);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(2);
        rowData.setField(0, StringData.fromString(TestUtils.TEST_STRING));
        rowData.setField(1, TestUtils.TEST_INT);

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes);
        Descriptors.FieldDescriptor stringField = message.getDescriptorForType().findFieldByName(TestUtils.STRING_FIELD);
        Descriptors.FieldDescriptor intField = message.getDescriptorForType().findFieldByName(TestUtils.INT_FIELD);

        Assertions.assertEquals(TestUtils.TEST_STRING, message.getField(stringField));
        Assertions.assertEquals(TestUtils.TEST_INT, message.getField(intField));

    }

    @Test
    public void testSerializeMap() throws Exception {
        RowType rowType = TestUtils.createRowType(
                new RowType.RowField(TestUtils.MAP_FIELD, new MapType(new VarCharType(), new IntType()))
        );
        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema = new ProtoRegistryDynamicSerializationSchema(
                TestUtils.DEFAULT_PACKAGE, TestUtils.DEFAULT_CLASS_NAME, rowType, SUBJECT_NAME, mockSchemaRegistryClient, SCHEMA_REGISTRY_URL);
        protoRegistryDynamicSerializationSchema.open(null);

        GenericRowData rowData = new GenericRowData(1);
        Map<String, Integer> mapContent = new HashMap<>();
        mapContent.put(TestUtils.TEST_STRING, TestUtils.TEST_INT);
        rowData.setField(0, new GenericMapData(mapContent));

        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);

        Message message = parseBytesToMessage(actualBytes);
        Descriptors.FieldDescriptor mapField = message.getDescriptorForType().findFieldByName(TestUtils.MAP_FIELD);

        Assertions.assertEquals(mapContent, message.getField(mapField));

    }

//    @Test
//    public void testSerializeGoogleTypes() throws Exception {
//        String tsField = "ts";
//        RowType rowType = TestUtils.createRowType(
//                new RowType.RowField(tsField, new TimestampType())
//        );
//        ProtoRegistryDynamicSerializationSchema protoRegistryDynamicSerializationSchema = new ProtoRegistryDynamicSerializationSchema(
//                TestUtils.DEFAULT_PACKAGE, TestUtils.DEFAULT_CLASS_NAME, rowType, SUBJECT_NAME, mockSchemaRegistryClient, SCHEMA_REGISTRY_URL);
//        protoRegistryDynamicSerializationSchema.open(null);
//
//        GenericRowData rowData = new GenericRowData(1);
//        rowData.setField(0, TimestampData.fromEpochMillis(123));
//
//        byte[] actualBytes = protoRegistryDynamicSerializationSchema.serialize(rowData);
//
//        ProtobufSchema actualRegisteredSchema = getRegisteredSchema();
//        ProtobufSchema expectedSchema = new ProtobufSchema(
//                sharedSchemaComponents() +
//                        "  google.protobuf.Timestamp" + tsField + "= 1;\n" +
//                        "}\n");
//        Assertions.assertEquals(expectedSchema, actualRegisteredSchema);
//
//        Message message = parseBytesToMessage(actualBytes);
//        Descriptors.FieldDescriptor tsFieldDescriptor = message.getDescriptorForType().findFieldByName(tsField);
//
//        Assertions.assertEquals(
//                Timestamp.newBuilder().setSeconds(123).setNanos(456).build(),
//                message.getField(tsFieldDescriptor)
//        );
//
//    }

    private static String sharedSchemaComponents() {
        return "syntax = \"proto3\";\n" +
                "package " + TestUtils.DEFAULT_PACKAGE + ";\n" +
                "option java_package = \"org.apache.flink.formats.protobuf.proto\";\n" +
                "option java_outer_classname = \"TestClass_OuterClass\";\n" +
                "option java_multiple_files = false;" +
                "import \"google/protobuf/timestamp.proto\";" +
                "message " + TestUtils.DEFAULT_CLASS_NAME + "{\n";
    }

    private ProtobufSchema getRegisteredSchema() throws Exception {
        return (ProtobufSchema) mockSchemaRegistryClient.getSchemas(SUBJECT_NAME, false, true).get(0);
    }

    private Message parseBytesToMessage(byte[] bytes) throws Exception {
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        KafkaProtobufDeserializer deser = new KafkaProtobufDeserializer(mockSchemaRegistryClient, opts);
        return deser.deserialize(null, bytes);
    }

}
