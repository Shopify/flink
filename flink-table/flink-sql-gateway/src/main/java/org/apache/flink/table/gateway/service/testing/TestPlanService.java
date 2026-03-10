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

import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.api.testing.TestCompileRequest;
import org.apache.flink.table.gateway.api.testing.TestCompileResponse;
import org.apache.flink.table.gateway.api.testing.TestMockSpec;
import org.apache.flink.table.gateway.api.testing.TestSchemaColumn;
import org.apache.flink.table.gateway.api.testing.TestingApi;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.session.Session;
import org.apache.flink.table.gateway.service.session.SessionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compiles a test plan by analyzing a pipeline in an isolated session.
 *
 * <p>The service loads all pipeline statements into an analysis session, uses {@code DESCRIBE} to
 * resolve mock schemas, {@code EXPLAIN} to detect window functions and temporal joins, and computes
 * the minimal set of views needed between mock boundaries and the test target.
 *
 * <p>The compiled plan is returned to the CLI, which creates mock objects and registers views in a
 * separate execution session using the standard SQL Gateway v3 API.
 */
public class TestPlanService {

    private static final Logger LOG = LoggerFactory.getLogger(TestPlanService.class);

    private static final String MODE_BATCH = "batch";
    private static final String MODE_CHANGELOG = "changelog";

    private final SessionManager sessionManager;

    public TestPlanService(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    /** Compile a test plan for the given request. */
    public TestCompileResponse compile(TestCompileRequest request) {
        validateRequest(request);
        String mode = normalizeMode(request.getMode());
        Set<String> mockTargetSet =
                request.getMockTargets().stream()
                        .map(TestPlanService::normalizeId)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        // Open an isolated analysis session.
        Session session =
                sessionManager.openSession(
                        SessionEnvironment.newBuilder()
                                .setSessionName("test-compile-analysis")
                                .setSessionEndpointVersion(new EndpointVersion() {})
                                .build());
        SessionHandle sessionHandle = session.getSessionHandle();
        try {
            // Phase 1: Load all pipeline statements in order.
            PipelineInfo pipeline = loadPipeline(session, request.getStatements());

            // Phase 2: Validate the target exists.
            String normalizedTarget = normalizeId(request.getTarget());
            if (!pipeline.allObjects.containsKey(normalizedTarget)) {
                throw new IllegalArgumentException(
                        "Target '"
                                + request.getTarget()
                                + "' not found in pipeline. Available: "
                                + String.join(", ", pipeline.allObjects.keySet()));
            }

            // Phase 3: DESCRIBE each mock target to get exact schemas.
            Map<String, MockInfo> resolvedMocks = new LinkedHashMap<>();
            for (String mockName : mockTargetSet) {
                if (!pipeline.allObjects.containsKey(mockName)) {
                    throw new IllegalArgumentException(
                            "Mock target '"
                                    + mockName
                                    + "' not found in pipeline. Available: "
                                    + String.join(", ", pipeline.allObjects.keySet()));
                }
                resolvedMocks.put(mockName, describeMock(session, mockName));
            }

            // Phase 4: EXPLAIN the target to detect windows and temporal joins.
            String explainPlan = explain(session, normalizedTarget);
            boolean isWindowed = containsWindowFunction(explainPlan);
            boolean hasTemporal = containsTemporalJoin(explainPlan);

            // Phase 5: Determine needed views (between mocks and target).
            List<String> neededStatements =
                    computeNeededStatements(normalizedTarget, mockTargetSet, pipeline);

            // Phase 6: Build mock specs.
            List<TestMockSpec> mocks = new ArrayList<>();
            for (Map.Entry<String, MockInfo> entry : resolvedMocks.entrySet()) {
                String mockName = entry.getKey();
                MockInfo info = entry.getValue();
                String materialization;
                String watermarkColumn = null;
                boolean includeSentinel = false;

                if (MODE_CHANGELOG.equals(mode) && isWindowed && info.watermarkColumn != null) {
                    materialization = "filesystem";
                    watermarkColumn = info.watermarkColumn;
                    includeSentinel = true;
                } else {
                    materialization = "view";
                }

                mocks.add(
                        new TestMockSpec(
                                mockName,
                                mockName, // session object = original name
                                materialization,
                                info.columns,
                                watermarkColumn,
                                includeSentinel));
            }

            // Phase 7: Build warnings.
            List<String> warnings = new ArrayList<>();
            if (hasTemporal) {
                warnings.add(
                        "Temporal joins detected in the target's execution plan. "
                                + "Temporal join semantics are unsupported/unstable in v1 test "
                                + "mocking; prefer an integration test or mock downstream of "
                                + "the temporal join.");
            }

            // Build response.
            return new TestCompileResponse(
                    TestingApi.CONTRACT_VERSION,
                    neededStatements,
                    "SELECT * FROM " + quoteId(normalizedTarget),
                    warnings,
                    mocks);
        } finally {
            try {
                sessionManager.closeSession(sessionHandle);
            } catch (Exception e) {
                LOG.warn("Failed to close analysis session.", e);
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Pipeline loading
    // -------------------------------------------------------------------------

    /**
     * Loads all pipeline statements into the session and classifies them using Flink's own catalog
     * introspection. No SQL text parsing is needed for classification — SHOW TABLES and SHOW VIEWS
     * tell us exactly what Flink created.
     */
    private PipelineInfo loadPipeline(Session session, List<String> statements) {
        List<String> allStatements = new ArrayList<>();

        // Execute all statements in pipeline order.
        for (String sql : statements) {
            String trimmed = sql.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            configureSession(session, trimmed);
            allStatements.add(trimmed);
        }

        // Query the catalog to see what was created.
        Set<String> catalogTables = fetchNames(session, "SHOW TABLES");
        Set<String> catalogViews = fetchNames(session, "SHOW VIEWS");

        // Match each statement to a catalog object by extracting its name.
        Map<String, String> allObjects = new LinkedHashMap<>();
        Map<String, String> tables = new LinkedHashMap<>();
        Map<String, String> views = new LinkedHashMap<>();
        List<String> configStatements = new ArrayList<>();

        for (String stmt : allStatements) {
            String name = extractObjectName(stmt);
            if (name != null) {
                String normalized = normalizeId(name);
                if (catalogViews.contains(normalized)) {
                    views.put(normalized, stmt);
                    allObjects.put(normalized, stmt);
                } else if (catalogTables.contains(normalized)) {
                    tables.put(normalized, stmt);
                    allObjects.put(normalized, stmt);
                } else {
                    configStatements.add(stmt);
                }
            } else {
                configStatements.add(stmt);
            }
        }

        return new PipelineInfo(allObjects, tables, views, configStatements);
    }

    /** Execute a SHOW command and return the names as a set. */
    private Set<String> fetchNames(Session session, String showCommand) {
        Set<String> names = new LinkedHashSet<>();
        ResultSet result = executeAndFetch(session, showCommand);
        for (org.apache.flink.table.data.RowData row : result.getData()) {
            names.add(normalizeId(row.getString(0).toString()));
        }
        return names;
    }

    // -------------------------------------------------------------------------
    //  Schema resolution via DESCRIBE
    // -------------------------------------------------------------------------

    /** DESCRIBE a pipeline object to get its exact schema and watermark info. */
    private MockInfo describeMock(Session session, String objectName) {
        List<TestSchemaColumn> columns = new ArrayList<>();
        String watermarkColumn = null;

        ResultSet result = executeAndFetch(session, "DESCRIBE " + quoteId(objectName));
        for (org.apache.flink.table.data.RowData row : result.getData()) {
            String colName = row.getString(0).toString();
            String colType = cleanType(row.getString(1).toString());
            String watermark = row.isNullAt(5) ? null : row.getString(5).toString();

            columns.add(new TestSchemaColumn(colName, colType));
            if (watermark != null && !watermark.isEmpty()) {
                watermarkColumn = colName;
            }
        }

        return new MockInfo(columns, watermarkColumn);
    }

    // -------------------------------------------------------------------------
    //  Window and temporal join detection via EXPLAIN
    // -------------------------------------------------------------------------

    /** EXPLAIN the target query and return the plan text. */
    private String explain(Session session, String target) {
        ResultSet result =
                executeAndFetch(session, "EXPLAIN SELECT * FROM " + quoteId(target));
        if (result.getData().isEmpty()) {
            return "";
        }
        return result.getData().get(0).getString(0).toString();
    }

    /** Check if the EXPLAIN plan contains window table function calls. */
    private static boolean containsWindowFunction(String plan) {
        String upper = plan.toUpperCase();
        return upper.contains("TUMBLE(") || upper.contains("HOP(") || upper.contains("CUMULATE(")
                || upper.contains("SESSION(");
    }

    /** Check if the EXPLAIN plan contains temporal join patterns. */
    private static boolean containsTemporalJoin(String plan) {
        return plan.toUpperCase().contains("FOR SYSTEM_TIME AS OF")
                || plan.contains("TemporalJoin")
                || plan.contains("LogicalSnapshot");
    }

    // -------------------------------------------------------------------------
    //  Needed statement computation
    // -------------------------------------------------------------------------

    /**
     * Computes the ordered list of DDL statements the CLI needs to register after creating mocks.
     * This includes config statements and view DDL for views between mock boundaries and the target.
     */
    private List<String> computeNeededStatements(
            String target, Set<String> mockTargets, PipelineInfo pipeline) {
        // Collect views needed between mocks and target.
        Set<String> neededViews = new LinkedHashSet<>();
        collectNeededViews(target, mockTargets, pipeline, neededViews, new LinkedHashSet<>());

        // Build ordered list: config statements first, then needed view DDL in pipeline order.
        List<String> result = new ArrayList<>(pipeline.configStatements);
        for (Map.Entry<String, String> entry : pipeline.views.entrySet()) {
            if (neededViews.contains(entry.getKey())) {
                result.add(entry.getValue());
            }
        }
        return result;
    }

    /**
     * Recursively collects views that are needed between mock boundaries and the target. Stops at
     * mock boundaries and at tables (which are either mocked or real sources).
     */
    private void collectNeededViews(
            String viewName,
            Set<String> mockTargets,
            PipelineInfo pipeline,
            Set<String> neededViews,
            Set<String> visited) {
        if (visited.contains(viewName) || mockTargets.contains(viewName)) {
            return;
        }
        visited.add(viewName);

        if (!pipeline.views.containsKey(viewName)) {
            return; // It's a table or unknown — not a view to register.
        }

        neededViews.add(viewName);

        // Find what this view references by checking which known objects appear in its DDL.
        String ddl = pipeline.views.get(viewName);
        for (String candidate : pipeline.allObjects.keySet()) {
            if (candidate.equals(viewName)) {
                continue;
            }
            if (sqlReferencesObject(ddl, candidate)) {
                collectNeededViews(candidate, mockTargets, pipeline, neededViews, visited);
            }
        }
    }

    /**
     * Checks if a SQL statement references an object name using word-boundary matching. Skips
     * matches inside single-quoted string literals.
     */
    static boolean sqlReferencesObject(String sql, String objectName) {
        String lower = sql.toLowerCase();
        String target = objectName.toLowerCase();
        int idx = 0;
        while ((idx = lower.indexOf(target, idx)) >= 0) {
            int end = idx + target.length();
            boolean leftOk =
                    idx == 0
                            || (!Character.isLetterOrDigit(lower.charAt(idx - 1))
                                    && lower.charAt(idx - 1) != '_');
            boolean rightOk =
                    end >= lower.length()
                            || (!Character.isLetterOrDigit(lower.charAt(end))
                                    && lower.charAt(end) != '_');
            if (leftOk && rightOk && !insideStringLiteral(lower, idx)) {
                return true;
            }
            idx = end;
        }
        return false;
    }

    /** Check if position is inside a single-quoted string literal. */
    private static boolean insideStringLiteral(String sql, int position) {
        int quotes = 0;
        for (int i = 0; i < position; i++) {
            if (sql.charAt(i) == '\'' && (i == 0 || sql.charAt(i - 1) != '\\')) {
                quotes++;
            }
        }
        return quotes % 2 != 0;
    }

    // -------------------------------------------------------------------------
    //  Session helpers
    // -------------------------------------------------------------------------

    /** Execute a statement via configureSession (synchronous). */
    private void configureSession(Session session, String statement) {
        try {
            OperationManager opManager = session.getOperationManager();
            org.apache.flink.table.gateway.api.operation.OperationHandle handle =
                    opManager.submitOperation(
                            h -> session.createExecutor().configureSession(h, statement));
            opManager.awaitOperationTermination(handle);
            opManager.closeOperation(handle);
        } catch (Exception e) {
            throw new RuntimeException("Failed to configure session: " + statement, e);
        }
    }

    /** Execute a statement and fetch all results synchronously. */
    private ResultSet executeAndFetch(Session session, String statement) {
        try {
            OperationManager opManager = session.getOperationManager();
            org.apache.flink.table.gateway.api.operation.OperationHandle handle =
                    opManager.submitOperation(
                            h -> session.createExecutor().executeStatement(h, statement));
            opManager.awaitOperationTermination(handle);
            ResultSet result = opManager.fetchResults(handle, 0L, Integer.MAX_VALUE);
            opManager.closeOperation(handle);
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute: " + statement, e);
        }
    }

    // -------------------------------------------------------------------------
    //  Validation and normalization
    // -------------------------------------------------------------------------

    private static void validateRequest(TestCompileRequest request) {
        if (request.getStatements() == null || request.getStatements().isEmpty()) {
            throw new IllegalArgumentException("statements must not be empty");
        }
        if (request.getTarget() == null || request.getTarget().trim().isEmpty()) {
            throw new IllegalArgumentException("target must not be empty");
        }
        if (request.getMode() == null || request.getMode().trim().isEmpty()) {
            throw new IllegalArgumentException("mode must not be empty");
        }
    }

    private static String normalizeMode(String mode) {
        String lower = mode.trim().toLowerCase();
        if (MODE_BATCH.equals(lower) || MODE_CHANGELOG.equals(lower)) {
            return lower;
        }
        throw new IllegalArgumentException(
                "Invalid mode '" + mode + "'. Must be 'batch' or 'changelog'.");
    }

    /** Normalize an identifier to lowercase, removing backtick/double-quote wrapping. */
    static String normalizeId(String name) {
        if (name == null) {
            return "";
        }
        String trimmed = name.trim();
        if ((trimmed.startsWith("`") && trimmed.endsWith("`"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed.toLowerCase();
    }

    private static String quoteId(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    /**
     * Clean a Flink type string from DESCRIBE output for use in CAST expressions. Removes
     * annotations like {@code *ROWTIME*}, {@code NOT NULL}, and {@code METADATA FROM}.
     */
    static String cleanType(String type) {
        String cleaned = type;
        // Remove *ROWTIME* watermark marker.
        cleaned = cleaned.replace("*ROWTIME*", "").trim();
        // Remove METADATA FROM clause.
        int metaIdx = cleaned.toUpperCase().indexOf(" METADATA");
        if (metaIdx > 0) {
            cleaned = cleaned.substring(0, metaIdx).trim();
        }
        // Remove NOT NULL constraint (mocks allow nulls).
        int notNullIdx = cleaned.toUpperCase().indexOf(" NOT NULL");
        if (notNullIdx > 0) {
            cleaned = cleaned.substring(0, notNullIdx).trim();
        }
        return cleaned;
    }

    /**
     * Extract the object name from a CREATE TABLE/VIEW statement. Handles backtick-quoted and
     * plain identifiers, including fully-qualified names (extracts the last part).
     *
     * <p>Uses specific patterns to find the keyword position, avoiding false matches on TABLE/VIEW
     * keywords that appear in the query body (e.g., FROM TABLE(TUMBLE(...))).
     */
    static String extractObjectName(String ddl) {
        String upper = ddl.toUpperCase().trim();
        // Search for CREATE [TEMPORARY] TABLE or CREATE [TEMPORARY] VIEW.
        int pos = -1;
        for (String pattern :
                new String[] {
                    "CREATE TEMPORARY VIEW ",
                    "CREATE TEMPORARY TABLE ",
                    "CREATE VIEW ",
                    "CREATE TABLE "
                }) {
            int idx = upper.indexOf(pattern);
            if (idx >= 0) {
                pos = idx + pattern.length();
                break;
            }
        }
        if (pos < 0) {
            return null;
        }

        // Skip whitespace and optional IF NOT EXISTS / IF EXISTS.
        String rest = ddl.substring(pos).trim();
        if (rest.toUpperCase().startsWith("IF NOT EXISTS")) {
            rest = rest.substring("IF NOT EXISTS".length()).trim();
        } else if (rest.toUpperCase().startsWith("IF EXISTS")) {
            rest = rest.substring("IF EXISTS".length()).trim();
        }

        // Extract the identifier (possibly dot-separated, possibly quoted).
        StringBuilder name = new StringBuilder();
        int i = 0;
        while (i < rest.length()) {
            char c = rest.charAt(i);
            if (c == '`') {
                // Backtick-quoted identifier.
                i++;
                while (i < rest.length() && rest.charAt(i) != '`') {
                    name.append(rest.charAt(i));
                    i++;
                }
                i++; // skip closing backtick
            } else if (c == '"') {
                i++;
                while (i < rest.length() && rest.charAt(i) != '"') {
                    name.append(rest.charAt(i));
                    i++;
                }
                i++;
            } else if (c == '.') {
                // Dot separator in qualified name — reset to take the last part.
                name.setLength(0);
                i++;
            } else if (Character.isLetterOrDigit(c) || c == '_' || c == '-') {
                name.append(c);
                i++;
            } else {
                break; // Hit space, paren, or other delimiter.
            }
        }
        return name.length() > 0 ? name.toString() : null;
    }

    // -------------------------------------------------------------------------
    //  Internal data classes
    // -------------------------------------------------------------------------

    private static class PipelineInfo {
        final Map<String, String> allObjects; // normalized name → DDL
        final Map<String, String> tables;
        final Map<String, String> views;
        final List<String> configStatements;

        PipelineInfo(
                Map<String, String> allObjects,
                Map<String, String> tables,
                Map<String, String> views,
                List<String> configStatements) {
            this.allObjects = allObjects;
            this.tables = tables;
            this.views = views;
            this.configStatements = configStatements;
        }
    }

    private static class MockInfo {
        final List<TestSchemaColumn> columns;
        final String watermarkColumn;

        MockInfo(List<TestSchemaColumn> columns, String watermarkColumn) {
            this.columns = columns;
            this.watermarkColumn = watermarkColumn;
        }
    }
}
