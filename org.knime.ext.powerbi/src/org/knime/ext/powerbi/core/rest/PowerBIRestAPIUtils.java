/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   Oct 4, 2019 (benjamin): created
 */
package org.knime.ext.powerbi.core.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;
import org.knime.core.util.ThreadLocalHTTPAuthenticator.AuthenticationCloseable;
import org.knime.ext.powerbi.core.rest.bindings.Column;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.core.rest.bindings.ErrorResponse;
import org.knime.ext.powerbi.core.rest.bindings.Groups;
import org.knime.ext.powerbi.core.rest.bindings.QueryErrorResponse;
import org.knime.ext.powerbi.core.rest.bindings.QueryResults;
import org.knime.ext.powerbi.core.rest.bindings.QueryResults.Result;
import org.knime.ext.powerbi.core.rest.bindings.Relationship;
import org.knime.ext.powerbi.core.rest.bindings.Table;
import org.knime.ext.powerbi.core.rest.bindings.Tables;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status.Family;
import jakarta.ws.rs.core.Response.StatusType;
import jakarta.ws.rs.core.UriBuilder;

/**
 * Utility class to make PowerBI API calls.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public final class PowerBIRestAPIUtils {

    private static final long CONNECTION_TIMEOUT = 30000;

    private static final long RECEIVE_TIMEOUT = 60000;

    private static final String GET_DATASETS_URI = "https://api.powerbi.com/v1.0/myorg/datasets";

    private static final String GET_DATASETS_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets";

    private static final String POST_DATASET_URI = "https://api.powerbi.com/v1.0/myorg/datasets";

    private static final String POST_DATASET_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets";

    private static final String POST_ROWS_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables/{tableName}/rows";

    private static final String POST_ROWS_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/tables/{tableName}/rows";

    private static final String DELETE_DATASET_URI = "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}";

    private static final String DELETE_DATASET_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}";

    private static final String GET_TABLES_URI = "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables";

    private static final String GET_TABLES_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/tables";

    private static final String PUT_TABLE_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables/{tableName}";

    private static final String PUT_TABLE_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/tables/{tableName}";

    private static final String GET_GROUPS_URI = "https://api.powerbi.com/v1.0/myorg/groups";

    private static final String DELETE_ROWS_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables/{tableName}/rows";

    private static final String DELETE_ROWS_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/tables/{tableName}/rows";

    private static final String EXECUTE_QUERY_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/executeQueries";

    private static final String EXECUTE_QUERY_IN_GROUP_URI =
        "https://api.powerbi.com/v1.0/myorg/groups/{groupId}/datasets/{datasetId}/executeQueries";

    private static final Gson GSON = new Gson();

    private PowerBIRestAPIUtils() {
        // Utility class
    }

    /**
     * Calls "Datasets - Get Datasets" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return a {@link Datasets} object which contains a list of datasets
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Datasets getDatasets(final AuthTokenProvider auth, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        return get(GET_DATASETS_URI, Datasets.class, auth, exec);
    }

    /**
     * Calls "Datasets - Get Datasets In Group" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return a {@link Datasets} object which contains a list of datasets
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Datasets getDatasets(final AuthTokenProvider auth, final String groupId, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            return getDatasets(auth, exec);
        }
        final String uri = UriBuilder.fromPath(GET_DATASETS_IN_GROUP_URI).build(groupId).toString();
        return get(uri, Datasets.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PostDataset" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @param relationships nullable array of column relationships
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Dataset postDataset(final AuthTokenProvider auth, final String datasetName, final String defaultMode,
        final Table[] tables, final Relationship[] relationships, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        if (relationships != null && relationships.length > 0) {
            body.put("relationships", relationships);
        }
        return post(POST_DATASET_URI, Dataset.class, GSON.toJson(body), auth, exec);
    }

    /**
     * This is the frozen version used by the deprecated node. Calls "Push Datasets - Datasets PostDatasetInGroup" from
     * the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    @Deprecated
    public static Dataset postDataset(final AuthTokenProvider auth, final String groupId, final String datasetName,
        final String defaultMode, final Table[] tables, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        String uri = groupId == null ? POST_DATASET_URI
            : UriBuilder.fromPath(POST_DATASET_IN_GROUP_URI).build(groupId).toString();
        return post(uri, Dataset.class, GSON.toJson(body), auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PostDatasetInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @param relationships nullable array of PowerBI relationship entitites
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Dataset postDataset(final AuthTokenProvider auth, final String groupId, final String datasetName,
        final String defaultMode, final Table[] tables, final Relationship[] relationships, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            return postDataset(auth, datasetName, defaultMode, tables, relationships, exec);
        }
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        if (relationships != null && relationships.length > 0) {
            body.put("relationships", relationships);
        }
        final String uri = UriBuilder.fromPath(POST_DATASET_IN_GROUP_URI).build(groupId).toString();
        return post(uri, Dataset.class, GSON.toJson(body), auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PostRows" from the Power BI REST API. Add rows to an existing Power BI dataset
     * and table.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param rows the rows to add
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void postRows(final AuthTokenProvider auth, final String datasetId, final String tableName,
        final String rows, final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final String uri = UriBuilder.fromPath(POST_ROWS_URI).build(datasetId, tableName).toString();
        post(uri, Void.class, rows, auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PostRowsInGroup" from the Power BI REST API. Add rows to an existing Power BI
     * dataset and table.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param rows the rows to add
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void postRows(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final String tableName, final String rows, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            postRows(auth, datasetId, tableName, rows, exec);
            return;
        }
        final String uri = UriBuilder.fromPath(POST_ROWS_IN_GROUP_URI).build(groupId, datasetId, tableName).toString();
        post(uri, Void.class, rows, auth, exec);
    }

    /**
     * Calls "Datasets - Delete Dataset" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void deleteDataset(final AuthTokenProvider auth, final String datasetId, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final String uri = UriBuilder.fromPath(DELETE_DATASET_URI).build(datasetId).toString();
        delete(uri, Void.class, auth, exec);
    }

    /**
     * Calls "Datasets - Delete DatasetInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void deleteDataset(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            deleteDataset(auth, datasetId, exec);
            return;
        }
        final String uri = UriBuilder.fromPath(DELETE_DATASET_IN_GROUP_URI).build(groupId, datasetId).toString();
        delete(uri, Void.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets GetTables" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the tables
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Tables getTables(final AuthTokenProvider auth, final String datasetId, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final String uri = UriBuilder.fromPath(GET_TABLES_URI).build(datasetId).toString();
        return get(uri, Tables.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets GetTablesInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the tables
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Tables getTables(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            return getTables(auth, datasetId, exec);
        }
        final String uri = UriBuilder.fromPath(GET_TABLES_IN_GROUP_URI).build(groupId, datasetId).toString();
        return get(uri, Tables.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PutTable" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param columns the columns of the table
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void putTable(final AuthTokenProvider auth, final String datasetId, final String tableName,
        final Column[] columns, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", tableName);
        body.put("columns", columns);
        final String uri = UriBuilder.fromPath(PUT_TABLE_URI).build(datasetId, tableName).toString();
        put(uri, Void.class, GSON.toJson(body), auth, exec);
    }

    /**
     * Calls "Push Datasets - Datasets PutTableInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param columns the columns of the table
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void putTable(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final String tableName, final Column[] columns, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            putTable(auth, datasetId, tableName, columns, exec);
            return;
        }
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", tableName);
        body.put("columns", columns);
        final String uri = UriBuilder.fromPath(PUT_TABLE_IN_GROUP_URI).build(groupId, datasetId, tableName).toString();
        put(uri, Void.class, GSON.toJson(body), auth, exec);
    }

    /**
     * Calls "Groups - Get Groups" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the groups the user has access to
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Groups getGroups(final AuthTokenProvider auth, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        return get(GET_GROUPS_URI, Groups.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Dataset DeleteRows" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void deleteRows(final AuthTokenProvider auth, final String datasetId, final String tableName,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final String uri = UriBuilder.fromPath(DELETE_ROWS_URI).build(datasetId, tableName).toString();
        delete(uri, Void.class, auth, exec);
    }

    /**
     * Calls "Push Datasets - Dataset DeleteRowsInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static void deleteRows(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final String tableName, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            deleteRows(auth, datasetId, tableName, exec);
            return;
        }
        final String uri =
            UriBuilder.fromPath(DELETE_ROWS_IN_GROUP_URI).build(groupId, datasetId, tableName).toString();
        delete(uri, Void.class, auth, exec);
    }

    /**
     * Calls "Datasets - Execute Queries" from the Power BI REST API. Execute a DAX Query which results in a table.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param datasetId the identifier of the dataset
     * @param query the query to execute
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the query result which may contain table and columns
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Result executeDAXQuery(final AuthTokenProvider auth, final String datasetId, final String query,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final String uri = UriBuilder.fromPath(EXECUTE_QUERY_URI).build(datasetId).toString();
        return executeDAXQuery(uri, auth, query, exec);
    }

    /**
     * Calls "Datasets - Execute Queries" from the Power BI REST API. Execute a DAX Query which results in a table.
     *
     * @param auth the authentication to use (the access token is refreshed if necessary)
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param query the query to execute
     * @param exec the execution context used to notify the user about the waiting period when waiting. The message will
     *            be restored. Can be {@code null} in which case no message will be set.
     * @return the query result which may contain table and columns
     * @throws PowerBIResponseException if an error was returned by the REST API
     * @throws CanceledExecutionException if the request or any of its retries was canceled
     */
    public static Result executeDAXQuery(final AuthTokenProvider auth, final String groupId, final String datasetId,
        final String query, final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        if (groupId == null) {
            return executeDAXQuery(auth, datasetId, query, exec);
        }
        final String uri = UriBuilder.fromPath(EXECUTE_QUERY_IN_GROUP_URI).build(groupId, datasetId).toString();
        return executeDAXQuery(uri, auth, query, exec);
    }

    private static Result executeDAXQuery(final String uri, final AuthTokenProvider auth, final String query,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final var body = Map.of("queries", List.of(Map.of("query", query)));
        final var post = post(uri, QueryResults.class, GSON.toJson(body), auth, exec);
        if (post == null || post.results() == null || post.results().length == 0) {
            return null;
        }
        return post.results()[0];
    }

    /** Make a GET request */
    private static <T> T get(final String uri, final Class<T> responseType, final AuthTokenProvider auth,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        try (final AuthenticationCloseable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final var response = RetryUtil.withRetry(client::get, exec)) {
            return checkResponse(response, responseType);
        }
    }

    /** Make a POST request */
    private static <T> T post(final String uri, final Class<T> responseType, final String body,
        final AuthTokenProvider auth, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        client.type(MediaType.APPLICATION_JSON);
        try (final AuthenticationCloseable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response response = RetryUtil.withRetry(() -> client.post(body), exec)) {
            return checkResponse(response, responseType);
        }
    }

    /** Make a DELETE request */
    private static <T> T delete(final String uri, final Class<T> responseType, final AuthTokenProvider auth,
        final ExecutionContext exec) throws PowerBIResponseException, CanceledExecutionException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        try (final AuthenticationCloseable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response response = RetryUtil.withRetry(client::delete, exec)) {
            return checkResponse(response, responseType);
        }
    }

    /** Make a PUT request */
    private static <T> T put(final String uri, final Class<T> responseType, final String body,
        final AuthTokenProvider auth, final ExecutionContext exec)
        throws PowerBIResponseException, CanceledExecutionException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        client.type(MediaType.APPLICATION_JSON);
        try (final AuthenticationCloseable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups();
                final Response response = RetryUtil.withRetry(() -> client.put(body), exec)) {
            return checkResponse(response, responseType);
        }
    }

    /**
     * Check the response of a call to the Power BI REST API. Reads the body if successful or throws an exception if
     * unsuccessful.
     */
    private static <T> T checkResponse(final Response response, final Class<T> responseType)
        throws PowerBIResponseException {
        final StatusType statusInfo = response.getStatusInfo();
        if (statusInfo.getFamily() != Family.SUCCESSFUL) {
            String message;
            try {
                final var str = response.readEntity(String.class);
                if (StringUtils.contains(str, "pbi.error")) {
                    final var queryError = GSON.fromJson(str, QueryErrorResponse.class);
                    message = queryError.toString();
                } else {
                    final var error = GSON.fromJson(str, ErrorResponse.class);
                    message = error == null ? "Unknown reason." : error.toString();
                }
            } catch (final JsonSyntaxException | ProcessingException e) {
                message = "Error occurred during communicating with Power BI: " + statusInfo.getReasonPhrase()
                    + " (Error Code: " + statusInfo.getStatusCode() + ")";
            }
            throw new PowerBIResponseException(message);
        }
        try {
            return GSON.fromJson(response.readEntity(String.class), responseType);
        } catch (final JsonSyntaxException e) {
            throw new PowerBIResponseException("Invalid response from Power BI.", e);
        }
    }

    /** Get a web client that accesses the given url with the given authentication */
    private static WebClient getClient(final String url, final AuthTokenProvider auth) throws PowerBIResponseException {
        final WebClient client = WebClient.create(url);

        // Set the timeout
        final HTTPConduit httpConduit = WebClient.getConfig(client).getHttpConduit();
        httpConduit.getClient().setConnectionTimeout(CONNECTION_TIMEOUT);
        httpConduit.getClient().setReceiveTimeout(RECEIVE_TIMEOUT);

        // Set the auth token
        client.authorization(getAuthenticationHeader(auth));
        return client;
    }

    private static String getAuthenticationHeader(final AuthTokenProvider auth) throws PowerBIResponseException {
        try {
            return "Bearer " + auth.getToken();
        } catch (final IOException ex) {
            throw new PowerBIResponseException(ex.getMessage(), ex);
        }
    }

    /** A interface for everything that can provide an Bearer token for Power BI */
    @FunctionalInterface
    public static interface AuthTokenProvider {
        /**
         * @return the Bearer token
         * @throws IOException if no token can be returned
         */
        String getToken() throws IOException;
    }

    /**
     * An exception that is thrown if an error occurs during the communication with the Power BI REST API.
     */
    public static class PowerBIResponseException extends Exception {

        private static final long serialVersionUID = 1L;

        private PowerBIResponseException(final String message, final Throwable cause) {
            super(message, cause);
        }

        private PowerBIResponseException(final String message) {
            super(message);
        }
    }
}
