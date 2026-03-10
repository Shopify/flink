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

package org.apache.flink.table.gateway.service.testing;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TestPlanService} utility methods. */
class TestPlanServiceTest {

    // -------------------------------------------------------------------------
    //  sqlReferencesObject
    // -------------------------------------------------------------------------

    @Test
    void sqlReferencesObjectFindsSimpleReference() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM payment_events", "payment_events")).isTrue();
    }

    @Test
    void sqlReferencesObjectRespectsWordBoundaries() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM payment_events_enriched", "payment_events")).isFalse();
    }

    @Test
    void sqlReferencesObjectSkipsStringLiterals() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM t WHERE name = 'payment_events'", "payment_events")).isFalse();
    }

    @Test
    void sqlReferencesObjectHandlesDoubledQuoteEscape() {
        // SQL escapes quotes by doubling: 'O''Brien'
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM t WHERE name = 'it''s payment_events here'", "payment_events"))
                .isFalse();
    }

    @Test
    void sqlReferencesObjectSkipsLineComments() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM t -- references payment_events\nWHERE 1=1", "payment_events"))
                .isFalse();
    }

    @Test
    void sqlReferencesObjectSkipsBlockComments() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM t /* payment_events */ WHERE 1=1", "payment_events")).isFalse();
    }

    @Test
    void sqlReferencesObjectMatchesAfterComment() {
        assertThat(TestPlanService.sqlReferencesObject(
                "-- comment\nSELECT * FROM payment_events", "payment_events")).isTrue();
    }

    @Test
    void sqlReferencesObjectCaseInsensitive() {
        assertThat(TestPlanService.sqlReferencesObject(
                "SELECT * FROM PAYMENT_EVENTS", "payment_events")).isTrue();
    }

    // -------------------------------------------------------------------------
    //  insideStringOrComment
    // -------------------------------------------------------------------------

    @Test
    void insideStringOrCommentDetectsSimpleString() {
        String sql = "SELECT 'hello world' FROM t";
        // Position of 'w' in 'world' — inside string
        int pos = sql.indexOf("world");
        assertThat(TestPlanService.insideStringOrComment(sql, pos)).isTrue();
    }

    @Test
    void insideStringOrCommentOutsideString() {
        String sql = "SELECT 'hello' FROM t";
        int pos = sql.indexOf("FROM");
        assertThat(TestPlanService.insideStringOrComment(sql, pos)).isFalse();
    }

    @Test
    void insideStringOrCommentHandlesDoubledQuotes() {
        // 'O''Brien' — the second quote is escaped, not a boundary
        String sql = "SELECT * FROM t WHERE name = 'O''Brien' AND id = 1";
        int pos = sql.indexOf("AND");
        assertThat(TestPlanService.insideStringOrComment(sql, pos)).isFalse();
    }

    @Test
    void insideStringOrCommentSkipsLineComment() {
        String sql = "SELECT * -- comment with 'quote\nFROM t";
        int pos = sql.indexOf("FROM");
        assertThat(TestPlanService.insideStringOrComment(sql, pos)).isFalse();
    }

    @Test
    void insideStringOrCommentSkipsBlockComment() {
        String sql = "SELECT * /* comment with 'quote */ FROM t";
        int pos = sql.indexOf("FROM");
        assertThat(TestPlanService.insideStringOrComment(sql, pos)).isFalse();
    }

    // -------------------------------------------------------------------------
    //  cleanType
    // -------------------------------------------------------------------------

    @Test
    void cleanTypeRemovesRowtime() {
        assertThat(TestPlanService.cleanType("TIMESTAMP(3) *ROWTIME*")).isEqualTo("TIMESTAMP(3)");
    }

    @Test
    void cleanTypeRemovesNotNull() {
        assertThat(TestPlanService.cleanType("INT NOT NULL")).isEqualTo("INT");
    }

    @Test
    void cleanTypeRemovesMetadata() {
        assertThat(TestPlanService.cleanType("BIGINT METADATA FROM 'offset'")).isEqualTo("BIGINT");
    }

    @Test
    void cleanTypeHandlesCombination() {
        assertThat(TestPlanService.cleanType("TIMESTAMP(3) *ROWTIME* NOT NULL"))
                .isEqualTo("TIMESTAMP(3)");
    }

    @Test
    void cleanTypeLeavesSimpleTypeAlone() {
        assertThat(TestPlanService.cleanType("STRING")).isEqualTo("STRING");
    }

    @Test
    void cleanTypeHandlesDecimal() {
        assertThat(TestPlanService.cleanType("DECIMAL(10, 2)")).isEqualTo("DECIMAL(10, 2)");
    }

    // -------------------------------------------------------------------------
    //  normalizeId
    // -------------------------------------------------------------------------

    @Test
    void normalizeIdLowercases() {
        assertThat(TestPlanService.normalizeId("Payment_Events")).isEqualTo("payment_events");
    }

    @Test
    void normalizeIdStripsBackticks() {
        assertThat(TestPlanService.normalizeId("`payment_events`")).isEqualTo("payment_events");
    }

    @Test
    void normalizeIdStripsDoubleQuotes() {
        assertThat(TestPlanService.normalizeId("\"payment_events\"")).isEqualTo("payment_events");
    }

    @Test
    void normalizeIdTrims() {
        assertThat(TestPlanService.normalizeId("  payment_events  ")).isEqualTo("payment_events");
    }
}
