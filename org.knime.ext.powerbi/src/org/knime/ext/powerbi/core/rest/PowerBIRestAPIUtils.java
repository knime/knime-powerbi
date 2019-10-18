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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;
import javax.ws.rs.core.Response.StatusType;
import javax.ws.rs.core.UriBuilder;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.http.HTTPConduit;
import org.knime.ext.azuread.auth.AzureADAuthentication;
import org.knime.ext.powerbi.core.rest.bindings.Column;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.core.rest.bindings.ErrorResponse;
import org.knime.ext.powerbi.core.rest.bindings.Groups;
import org.knime.ext.powerbi.core.rest.bindings.Table;
import org.knime.ext.powerbi.core.rest.bindings.Tables;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

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

    private static final Gson GSON = new Gson();

    private PowerBIRestAPIUtils() {
        // Utility class
    }

    /**
     * Calls "Datasets - Get Datasets" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @return a {@link Datasets} object which contains a list of datasets
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Datasets getDatasets(final AzureADAuthentication auth) throws PowerBIResponseException {
        return get(GET_DATASETS_URI, Datasets.class, auth);
    }

    /**
     * Calls "Datasets - Get Datasets In Group" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @return a {@link Datasets} object which contains a list of datasets
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Datasets getDatasets(final AzureADAuthentication auth, final String groupId)
        throws PowerBIResponseException {
        if (groupId == null) {
            return getDatasets(auth);
        }
        final String uri = UriBuilder.fromPath(GET_DATASETS_IN_GROUP_URI).build(groupId).toString();
        return get(uri, Datasets.class, auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostDataset" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Dataset postDataset(final AzureADAuthentication auth, final String datasetName,
        final String defaultMode, final Table[] tables) throws PowerBIResponseException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        return post(POST_DATASET_URI, Dataset.class, GSON.toJson(body), auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostDatasetInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Dataset postDataset(final AzureADAuthentication auth, final String groupId, final String datasetName,
        final String defaultMode, final Table[] tables) throws PowerBIResponseException {
        if (groupId == null) {
            return postDataset(auth, datasetName, defaultMode, tables);
        }
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        final String uri = UriBuilder.fromPath(POST_DATASET_IN_GROUP_URI).build(groupId).toString();
        return post(uri, Dataset.class, GSON.toJson(body), auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostRows" from the Power BI REST API. Add rows to an existing Power BI dataset
     * and table.
     *
     * @param auth the authentication to use
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param rows the rows to add
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void postRows(final AzureADAuthentication auth, final String datasetId, final String tableName,
        final String rows) throws PowerBIResponseException {
        final String uri = UriBuilder.fromPath(POST_ROWS_URI).build(datasetId, tableName).toString();
        post(uri, Void.class, rows, auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostRowsInGroup" from the Power BI REST API. Add rows to an existing Power BI
     * dataset and table.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param rows the rows to add
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void postRows(final AzureADAuthentication auth, final String groupId, final String datasetId,
        final String tableName, final String rows) throws PowerBIResponseException {
        if (groupId == null) {
            postRows(auth, datasetId, tableName, rows);
            return;
        }
        final String uri = UriBuilder.fromPath(POST_ROWS_IN_GROUP_URI).build(groupId, datasetId, tableName).toString();
        post(uri, Void.class, rows, auth);
    }

    /**
     * Calls "Datasets - Delete Dataset" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param datasetId the identifier of the dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void deleteDataset(final AzureADAuthentication auth, final String datasetId)
        throws PowerBIResponseException {
        final String uri = UriBuilder.fromPath(DELETE_DATASET_URI).build(datasetId).toString();
        delete(uri, Void.class, auth);
    }

    /**
     * Calls "Datasets - Delete DatasetInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void deleteDataset(final AzureADAuthentication auth, final String groupId, final String datasetId)
        throws PowerBIResponseException {
        if (groupId == null) {
            deleteDataset(auth, datasetId);
            return;
        }
        final String uri = UriBuilder.fromPath(DELETE_DATASET_IN_GROUP_URI).build(groupId, datasetId).toString();
        delete(uri, Void.class, auth);
    }

    /**
     * Calls "Push Datasets - Datasets GetTables" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param datasetId the identifier of the dataset
     * @return the tables
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Tables getTables(final AzureADAuthentication auth, final String datasetId)
        throws PowerBIResponseException {
        final String uri = UriBuilder.fromPath(GET_TABLES_URI).build(datasetId).toString();
        return get(uri, Tables.class, auth);
    }

    /**
     * Calls "Push Datasets - Datasets GetTablesInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @return the tables
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Tables getTables(final AzureADAuthentication auth, final String groupId, final String datasetId)
        throws PowerBIResponseException {
        if (groupId == null) {
            return getTables(auth, datasetId);
        }
        final String uri = UriBuilder.fromPath(GET_TABLES_IN_GROUP_URI).build(groupId, datasetId).toString();
        return get(uri, Tables.class, auth);
    }

    /**
     * Calls "Push Datasets - Datasets PutTable" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param columns the columns of the table
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void putTable(final AzureADAuthentication auth, final String datasetId, final String tableName,
        final Column[] columns) throws PowerBIResponseException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", tableName);
        body.put("columns", columns);
        final String uri = UriBuilder.fromPath(PUT_TABLE_URI).build(datasetId, tableName).toString();
        put(uri, Void.class, GSON.toJson(body), auth);
    }

    /**
     * Calls "Push Datasets - Datasets PutTableInGroup" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @param groupId the workspace id (Can be <code>null</code> for "My Workspace")
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param columns the columns of the table
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void putTable(final AzureADAuthentication auth, final String groupId, final String datasetId,
        final String tableName, final Column[] columns) throws PowerBIResponseException {
        if (groupId == null) {
            putTable(auth, datasetId, tableName, columns);
            return;
        }
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", tableName);
        body.put("columns", columns);
        final String uri = UriBuilder.fromPath(PUT_TABLE_IN_GROUP_URI).build(groupId, datasetId, tableName).toString();
        put(uri, Void.class, GSON.toJson(body), auth);
    }

    /**
     * Calls "Groups - Get Groups" from the Power BI REST API.
     *
     * @param auth the authentication to use
     * @return the groups the user has access to
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Groups getGroups(final AzureADAuthentication auth) throws PowerBIResponseException {
        return get(GET_GROUPS_URI, Groups.class, auth);
    }

    /** Make a GET request */
    private static <T> T get(final String uri, final Class<T> responseType, final AzureADAuthentication auth)
        throws PowerBIResponseException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        final Response response = client.get();
        return checkResponse(response, responseType);
    }

    /** Make a POST request */
    private static <T> T post(final String uri, final Class<T> responseType, final String body,
        final AzureADAuthentication auth) throws PowerBIResponseException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        client.type(MediaType.APPLICATION_JSON);
        final Response response = client.post(body);
        return checkResponse(response, responseType);
    }

    /** Make a DELETE request */
    private static <T> T delete(final String uri, final Class<T> responseType, final AzureADAuthentication auth)
        throws PowerBIResponseException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        final Response response = client.delete();
        return checkResponse(response, responseType);
    }

    /** Make a PUT request */
    private static <T> T put(final String uri, final Class<T> responseType, final String body,
        final AzureADAuthentication auth) throws PowerBIResponseException {
        final WebClient client = getClient(uri, auth);
        client.accept(MediaType.APPLICATION_JSON);
        client.type(MediaType.APPLICATION_JSON);
        final Response response = client.put(body);
        return checkResponse(response, responseType);
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
                final ErrorResponse error = GSON.fromJson(response.readEntity(String.class), ErrorResponse.class);
                message = error.toString();
            } catch (final JsonSyntaxException | ProcessingException e) {
                message = "Error occured during communicating with Power BI: " + statusInfo.getReasonPhrase()
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
    private static WebClient getClient(final String url, final AzureADAuthentication auth) {
        final WebClient client = WebClient.create(url);

        // Set the timeout
        final HTTPConduit httpConduit = WebClient.getConfig(client).getHttpConduit();
        httpConduit.getClient().setConnectionTimeout(CONNECTION_TIMEOUT);
        httpConduit.getClient().setReceiveTimeout(RECEIVE_TIMEOUT);

        // Set the auth token
        client.authorization(getAuthenticationHeader(auth));
        return client;
    }

    private static String getAuthenticationHeader(final AzureADAuthentication auth) {
        return "Bearer " + auth.getAccessToken();
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
