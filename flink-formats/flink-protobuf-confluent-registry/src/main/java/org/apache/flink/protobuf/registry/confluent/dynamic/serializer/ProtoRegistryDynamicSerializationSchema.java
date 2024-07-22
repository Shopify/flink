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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.serialize.MessageSerializer;
import org.apache.flink.formats.protobuf.serialize.RowToProtoConverter;
import org.apache.flink.protobuf.registry.confluent.dynamic.deserializer.ProtoCompiler;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashMap;
import java.util.Map;

public class ProtoRegistryDynamicSerializationSchema implements SerializationSchema<RowData> {

    private static final String PROTOBUF_OUTER_CLASS_NAME_SUFFIX = "OuterClass";

    private final String generatedPackageName;
    private final String generatedClassName;
    private final RowType rowType;
    private final String subjectName;
    private final SchemaRegistryClient schemaRegistryClient;
    private final String schemaRegistryUrl;

    private transient RowToProtoConverter rowToProtoConverter;

    public ProtoRegistryDynamicSerializationSchema(
            String generatedPackageName,
            String generatedClassName,
            RowType rowType,
            String subjectName,
            SchemaRegistryClient schemaRegistryClient,
            String schemaRegistryUrl) {
        this.generatedPackageName = generatedPackageName;
        this.generatedClassName = generatedClassName;
        this.rowType = rowType;
        this.subjectName = subjectName;
        this.schemaRegistryClient = schemaRegistryClient;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            return rowToProtoConverter.convertRowToProtoBinary(element);
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Class generatedClass = generateProtoClassForRowType();
        KafkaProtobufSerializer kafkaProtobufSerializer = createKafkaSerializer();
        MessageSerializer messageSerializer = new ConfluentMessageSerializer(kafkaProtobufSerializer, subjectName);
        // TODO: use a real config here
        PbFormatConfig formatConfig = new PbFormatConfig(generatedClass.getName(), false, true, null);
        rowToProtoConverter = new RowToProtoConverter(rowType, formatConfig, messageSerializer);
    }

    private Class generateProtoClassForRowType() throws Exception {
        RowToProtobufSchemaConverter rowToProtobufSchemaConverter = new RowToProtobufSchemaConverter(
                generatedPackageName, generatedClassName, rowType
        );

        ProtobufSchema protoSchema = rowToProtobufSchemaConverter.convert();
        ProtoCompiler protoCompiler = new ProtoCompiler(PROTOBUF_OUTER_CLASS_NAME_SUFFIX);
        return protoCompiler.generateMessageClass(protoSchema, null);
    }

    private KafkaProtobufSerializer createKafkaSerializer() throws Exception {
        Map<String, String> opts = new HashMap<>();
        opts.put("schema.registry.url", schemaRegistryUrl);
        opts.put("auto.register.schemas", "true");
        opts.put("value.subject.name.strategy", NoSuffixTopicNameStrategy.class.getName());
        boolean isKey = false;

        KafkaProtobufSerializer ser = new KafkaProtobufSerializer(schemaRegistryClient);
        ser.configure(opts, isKey);

        return ser;
    }
}
