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

import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
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
        fileDescriptorProtoBuilder.addDependency("google/protobuf/timestamp.proto");

        // Create a DescriptorProto builder for the message type
        DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder = DescriptorProtos.DescriptorProto.newBuilder();
        descriptorProtoBuilder.setName(className);

        // Convert each field in RowType to a FieldDescriptorProto
        recurseRowType(rowType, descriptorProtoBuilder);

        // Add the message type to the file descriptor
        fileDescriptorProtoBuilder.addMessageType(descriptorProtoBuilder);

        // Build the FileDescriptor
        DescriptorProtos.FileDescriptorProto fileDescriptorProto = fileDescriptorProtoBuilder.build();
        Descriptors.FileDescriptor[] dependencies = new Descriptors.FileDescriptor[] {
                Timestamp.getDescriptor().getFile()
        };
        Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, dependencies);

        return new ProtobufSchema(fileDescriptor);
    }

    private void recurseRowType(RowType rowType, DescriptorProtos.DescriptorProto.Builder currentBuilder) {

        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder = DescriptorProtos.FieldDescriptorProto.newBuilder();
            fieldDescriptorProtoBuilder.setName(field.getName());
            fieldDescriptorProtoBuilder.setNumber(i + 1);
            if (field.getType() instanceof RowType) {
                DescriptorProtos.DescriptorProto.Builder newBuilder = DescriptorProtos.DescriptorProto.newBuilder();
                String nestedTypeName = nestedClassName(field.getName());
                newBuilder.setName(nestedTypeName);
                recurseRowType((RowType) field.getType(), newBuilder);
                currentBuilder.addNestedType(newBuilder);

                fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);

            } else if (field.getType() instanceof ArrayType) {
                if (((ArrayType) field.getType()).getElementType() instanceof RowType) {
                    DescriptorProtos.DescriptorProto.Builder newBuilder = DescriptorProtos.DescriptorProto.newBuilder();
                    String nestedTypeName = nestedClassName(field.getName());
                    newBuilder.setName(nestedTypeName);
                    recurseRowType(
                            (RowType) ((ArrayType) field.getType()).getElementType(),
                            newBuilder);
                    currentBuilder.addNestedType(newBuilder);

                    fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                    fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);
                } else {
                    setProtoField(fieldDescriptorProtoBuilder, ((ArrayType) field.getType()).getElementType());
                }
                fieldDescriptorProtoBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
            } else if (field.getType() instanceof MapType) {
                List<RowType.RowField> mapFields = new ArrayList<>();
                mapFields.add(new RowType.RowField(
                        "key",
                        ((MapType) field.getType()).getKeyType()));
                mapFields.add(new RowType.RowField(
                        "value",
                        ((MapType) field.getType()).getValueType()));

                DescriptorProtos.DescriptorProto.Builder newBuilder = DescriptorProtos.DescriptorProto.newBuilder();
                String nestedTypeName = nestedClassName(field.getName());
                newBuilder.setName(nestedTypeName);
                recurseRowType(new RowType(mapFields), newBuilder);
                newBuilder.setOptions(DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true).build());

                currentBuilder.addNestedType(newBuilder);

                fieldDescriptorProtoBuilder.setType(Type.TYPE_MESSAGE);
                fieldDescriptorProtoBuilder.setTypeName(nestedTypeName);
                fieldDescriptorProtoBuilder.setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
            } else {
                setProtoField(fieldDescriptorProtoBuilder, field.getType());
            }
            currentBuilder.addField(fieldDescriptorProtoBuilder.build());
        }

    }

    /**
     * Set the type of a FieldDescriptorProto based on the logical type of the Flink field.
     */
    private static void setProtoField(DescriptorProtos.FieldDescriptorProto.Builder input,
                                      LogicalType rowFieldType) {
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
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                input.setType(Type.TYPE_INT32);
                return;
            case BIGINT:
                input.setType(Type.TYPE_INT64);
                return;
            case FLOAT:
                input.setType(Type.TYPE_FLOAT);
                return;
            case DOUBLE:
                input.setType(Type.TYPE_DOUBLE);
                return;
            case DATE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                input.setType(Type.TYPE_MESSAGE);
                input.setTypeName("google.protobuf.Timestamp");
                return;
            default:
                throw new IllegalArgumentException("Unsupported logical type: " + rowFieldType);
        }
    }

    private String nestedClassName(String originalFieldName) {
        return originalFieldName + "Class";
    }
}
