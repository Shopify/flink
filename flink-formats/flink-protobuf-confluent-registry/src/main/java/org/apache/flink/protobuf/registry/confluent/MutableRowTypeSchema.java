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

import org.apache.flink.table.types.logical.RowType;

public interface MutableRowTypeSchema {
    /**
     * Sets the row type information for the deserialization schema.
     * While the "vanilla" implementations of this interface will never need to call this method,
     * it is useful for the debezium format, which needs to decorate the user's row type to match
     * the debezium payload format.
     *
     * @param rowType The row type information.
     */
    void setRowType(RowType rowType);

    RowType getRowType();
}
