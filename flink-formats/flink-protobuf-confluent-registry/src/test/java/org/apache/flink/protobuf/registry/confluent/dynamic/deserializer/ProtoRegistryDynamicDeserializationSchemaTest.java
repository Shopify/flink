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

package org.apache.flink.protobuf.registry.confluent.dynamic.deserializer;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.flink.formats.protobuf.proto.FlatProto3OuterClass;

import org.apache.flink.formats.protobuf.proto.MapProto3;
import org.apache.flink.protobuf.registry.confluent.TestUtils;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ProtoRegistryDynamicDeserializationSchemaTest {

    private MockSchemaRegistryClient mockSchemaRegistryClient;
    private KafkaProtobufSerializer kafkaProtobufSerializer;
    private ProtoRegistryDynamicDeserializerFormatConfig formatConfig;
    private static final String DUMMY_SCHEMA_REGISTRY_URL = "http://registry:8081";
    public static final String FAKE_TOPIC = "fake-topic";

    @BeforeEach
    public void setup() {
        mockSchemaRegistryClient = new MockSchemaRegistryClient();
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", DUMMY_SCHEMA_REGISTRY_URL);
        kafkaProtobufSerializer = new KafkaProtobufSerializer(mockSchemaRegistryClient, opts);
        formatConfig = new ProtoRegistryDynamicDeserializerFormatConfig(DUMMY_SCHEMA_REGISTRY_URL, false, false);
    }

    @Test
    public void deserializerTest() throws Exception {
        FlatProto3OuterClass.FlatProto3 in = FlatProto3OuterClass.FlatProto3.newBuilder()
                .setString(TestUtils.TEST_STRING)
                .setInt(TestUtils.TEST_INT)
                .setLong(TestUtils.TEST_LONG)
                .setFloat(TestUtils.TEST_FLOAT)
                .setDouble(TestUtils.TEST_DOUBLE)
                .addInts(TestUtils.TEST_INT)
                .setBytes(TestUtils.TEST_BYTES)
                .setBool(TestUtils.TEST_BOOL)
                .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("string", new VarCharType()));
        fields.add(new RowType.RowField("int", new IntType()));
        fields.add(new RowType.RowField("long", new BigIntType()));

        RowType rowType = new RowType(fields);

        ProtoRegistryDynamicDeserializationSchema deser = new ProtoRegistryDynamicDeserializationSchema(
                mockSchemaRegistryClient, rowType, null, formatConfig
        );
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(3, actual.getArity());
        Assertions.assertEquals(TestUtils.TEST_STRING, actual.getString(0).toString());
        Assertions.assertEquals(TestUtils.TEST_INT, actual.getInt(1));
        Assertions.assertEquals(TestUtils.TEST_LONG, actual.getLong(2));
    }

    @Test
    public void mapDeserializerTest() throws Exception {
        MapProto3.Proto3Map in = MapProto3.Proto3Map.newBuilder()
                .putMap(TestUtils.TEST_STRING, TestUtils.TEST_STRING)
                .build();

        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);

        RowType rowType = TestUtils.createRowType(
                new RowType.RowField(TestUtils.MAP_FIELD, new MapType(new VarCharType(), new VarCharType()))
        );

        ProtoRegistryDynamicDeserializationSchema deser = new ProtoRegistryDynamicDeserializationSchema(
                mockSchemaRegistryClient, rowType, null, formatConfig
        );
        deser.open(null);

        RowData actual = deser.deserialize(inBytes);
        Assertions.assertEquals(1, actual.getArity());
        Map<BinaryStringData, BinaryStringData> expectedMap = new HashMap<>();
        BinaryStringData binaryString = BinaryStringData.fromString(TestUtils.TEST_STRING);
        expectedMap.put(binaryString, binaryString);
        Assertions.assertEquals(new GenericMapData(expectedMap), actual.getMap(0));
    }

//    @Test
//    public void googleTypeDeserializerTest() throws Exception {
//        TimestampProto3OuterClass.TimestampProto3 in = TimestampProto3OuterClass.TimestampProto3.newBuilder()
//                .setTs(
//                        com.google.protobuf.Timestamp.newBuilder()
//                                .setSeconds(123)
//                                .setNanos(0)
//                                .build()
//                )
//                .build();
//        byte[] inBytes = kafkaProtobufSerializer.serialize(FAKE_TOPIC, in);
//
//        RowType rowType = TestUtils.createRowType(
//                new RowType.RowField("ts", new TimestampType())
//        );
//
//        ProtoRegistryDynamicDeserializationSchema deser = new ProtoRegistryDynamicDeserializationSchema(
//                mockSchemaRegistryClient, rowType, null, formatConfig
//        );
//        deser.open(null);
//
//        RowData actual = deser.deserialize(inBytes);
//        System.out.println(actual);
//    }

}
