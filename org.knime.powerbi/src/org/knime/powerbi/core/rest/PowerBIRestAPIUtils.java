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
package org.knime.powerbi.core.rest;

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
import org.knime.azuread.auth.AzureADAuthentication;
import org.knime.powerbi.core.rest.bindings.Column;
import org.knime.powerbi.core.rest.bindings.Dataset;
import org.knime.powerbi.core.rest.bindings.Datasets;
import org.knime.powerbi.core.rest.bindings.ErrorResponse;
import org.knime.powerbi.core.rest.bindings.Table;
import org.knime.powerbi.core.rest.bindings.Tables;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class PowerBIRestAPIUtils {

    private static final long CONNECTION_TIMEOUT = 30000;

    private static final long RECEIVE_TIMEOUT = 60000;

    private static final String GET_DATASETS_URI = "https://api.powerbi.com/v1.0/myorg/datasets";

    private static final String CREATE_DATASET_URI = "https://api.powerbi.com/v1.0/myorg/datasets";

    private static final String ADD_ROWS_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables/{tableName}/rows";

    private static final String DELETE_DATASET_URI = "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}";

    private static final String GET_TABLES_URI = "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables";

    private static final String PUT_TABLE_URI =
        "https://api.powerbi.com/v1.0/myorg/datasets/{datasetId}/tables/{tableName}";

    private static final Gson GSON = new Gson();

    private PowerBIRestAPIUtils() {
        // Utility class
    }

    /**
     * Calls "Datasets - Get Datasets" from the PowerBI REST API.
     *
     * @param auth the authentication to use
     * @return a {@link Datasets} object which contains a list of datasets
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Datasets getDatasets(final AzureADAuthentication auth) throws PowerBIResponseException {
        return get(GET_DATASETS_URI, Datasets.class, auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostDataset" from the PowerBI REST API.
     *
     * @param auth the authentication to use
     * @param datasetName the name of the dataset
     * @param defaultMode the mode of the dataset
     * @param tables the table definitions of the dataset
     * @return the created dataset
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static Dataset createDataset(final AzureADAuthentication auth, final String datasetName,
        final String defaultMode, final Table[] tables) throws PowerBIResponseException {
        final Map<String, Object> body = new HashMap<>(2);
        body.put("name", datasetName);
        body.put("defaultMode", defaultMode);
        body.put("tables", tables);
        return post(CREATE_DATASET_URI, Dataset.class, GSON.toJson(body), auth);
    }

    /**
     * Calls "Push Datasets - Datasets PostRows" from the PowerBI REST API. Add rows to an existing PowerBI dataset and
     * table.
     *
     * @param auth the authentication to use
     * @param datasetId the identifier of the dataset
     * @param tableName the name of the table
     * @param rows the rows to add
     * @throws PowerBIResponseException if an error was returned by the REST API
     */
    public static void addRows(final AzureADAuthentication auth, final String datasetId, final String tableName,
        final String rows) throws PowerBIResponseException {
        final String uri = UriBuilder.fromPath(ADD_ROWS_URI).build(datasetId, tableName).toString();
        post(uri, Void.class, rows, auth);
    }

    /**
     * Calls "Datasets - Delete Dataset" from the PowerBI REST API.
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
     * Calls "Push Datasets - Datasets GetTables" from the PowerBI REST API.
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
     * Calls "Push Datasets - Datasets PutTable" from the PowerBI REST API.
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
     * Check the response of a call to the PowerBI REST API. Reads the body if successful or throws an exception if
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
                message = "Error occured during communicating with PowerBI: " + statusInfo.getReasonPhrase()
                    + " (Error Code: " + statusInfo.getStatusCode() + ")";
            }
            throw new PowerBIResponseException(message);
        }
        try {
            return GSON.fromJson(response.readEntity(String.class), responseType);
        } catch (final JsonSyntaxException e) {
            throw new PowerBIResponseException("Invalid response from PowerBI.", e);
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
     * An exception that is thrown if an error occurs during the communication with the PowerBI REST API.
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
