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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;

import com.google.protobuf.GeneratedMessageV3;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.type.DecimalProto;

import org.apache.avro.LogicalTypes;

import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

public class RowToProtobufSchemaConverter {
    private final String packageName;
    private final String className;
    private final RowType rowType;

    public RowToProtobufSchemaConverter(String packageName, String className, RowType rowType) {
        this.packageName = packageName;
        this.className = className;
        this.rowType = rowType;
    }

    public ProtobufSchema convert() throws Descriptors.DescriptorValidationException {
        // Create a FileDescriptorProto builder
        DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        fileDescriptorProtoBuilder.setName(className);
        fileDescriptorProtoBuilder.setPackage(packageName);
        fileDescriptorProtoBuilder.setSyntax("proto3");
        fileDescriptorProtoBuilder.addDependency("confluent/type/decimal.proto");

        // Create a DescriptorProto builder for the message type
        DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        descriptorProtoBuilder.setName(className);

        // Convert each field in RowType to a FieldDescriptorProto
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();
            fieldDescriptorProtoBuilder.setName(field.getName());
            fieldDescriptorProtoBuilder.setNumber(i + 1);
            setProtoField(fieldDescriptorProtoBuilder, field.getType());
            descriptorProtoBuilder.addField(fieldDescriptorProtoBuilder);
        }

        // Add the message type to the file descriptor
        fileDescriptorProtoBuilder.addMessageType(descriptorProtoBuilder);

        // Build the FileDescriptor
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptorProtoBuilder.build();
        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[] {
                DecimalProto.getDescriptor().getFile()
        };
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);

        return new ProtobufSchema(fileDescriptor);
    }

    private static void setProtoField(DescriptorProtos.FieldDescriptorProto.Builder input, LogicalType rowFieldType) {
        switch (rowFieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                input.setType(Type.TYPE_STRING);
                return;
            case BOOLEAN:
                input.setType(Type.TYPE_BOOL);
                return;
            case BINARY:
            case VARBINARY:
                input.setType(Type.TYPE_BYTES);
                return;
            case INTEGER:
                input.setType(Type.TYPE_INT32);
                return;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) rowFieldType;
                input.setType(Type.TYPE_MESSAGE);
                MetaProto.Meta options = MetaProto.Meta.newBuilder()
                        .putParams("precision", String.valueOf(decimalType.getPrecision()))
                        .putParams("scale", String.valueOf(decimalType.getScale())).build();
                DescriptorProtos.FieldOptions.Builder optionsBuilder =
                        DescriptorProtos.FieldOptions.newBuilder();
                optionsBuilder.setExtension(MetaProto.fieldMeta, options);
                input.setOptions(optionsBuilder.build());
                input.setTypeName("confluent.type.Decimal");
                return;
//            case BIGINT:
//                return Type.TYPE_INT64;
//            case FLOAT:
//                return Type.TYPE_FLOAT;
//            case DOUBLE:
//                return Type.TYPE_DOUBLE;
            // Add more cases as needed
            default:
                throw new IllegalArgumentException("Unsupported logical type: " + rowFieldType);
        }
    }
}
