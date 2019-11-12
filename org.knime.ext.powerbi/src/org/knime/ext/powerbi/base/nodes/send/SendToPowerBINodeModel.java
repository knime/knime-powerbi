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
package org.knime.ext.powerbi.base.nodes.send;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.ConvenienceMethods;
import org.knime.ext.azuread.auth.AzureADAuthentication;
import org.knime.ext.azuread.auth.AzureADAuthenticationUtils;
import org.knime.ext.azuread.auth.AzureADAuthenticationUtils.AuthenticationException;
import org.knime.ext.azuread.auth.DefaultOAuth20Scope;
import org.knime.ext.azuread.auth.OAuth20Scope;
import org.knime.ext.powerbi.base.nodes.send.SendToPowerBINodeSettings.OverwritePolicy;
import org.knime.ext.powerbi.core.PowerBIDataTypeUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.ext.powerbi.core.rest.bindings.Column;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.core.rest.bindings.Group;
import org.knime.ext.powerbi.core.rest.bindings.Groups;
import org.knime.ext.powerbi.core.rest.bindings.Table;
import org.knime.ext.powerbi.core.rest.bindings.Tables;

/**
 * Send to Power BI node model.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeModel extends NodeModel {

    private static final double PROGRESS_PREPARE = 0.3;

    private static final double PROGRESS_SEND_ROWS = 1 - PROGRESS_PREPARE;

    private static final int POWERBI_MAX_COLUMNS = 75;

    private static final String POWERBI_DATASET_MODE = "Push";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeModel.class);

    static final OAuth20Scope OAUTH_POWERBI_SCOPE = new DefaultOAuth20Scope( //
        "offline_access", // Required for the refresh token
        "https://analysis.windows.net/powerbi/api/Dataset.Read.All", // Required to list datasets
        "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All", // Required to upload datasets
        "https://analysis.windows.net/powerbi/api/Workspace.Read.All" // Required to get the workspaces
    );

    private final SendToPowerBINodeSettings m_settings;

    SendToPowerBINodeModel() {
        super(1, 0);
        m_settings = new SendToPowerBINodeSettings();
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        // Check if the user is authenticated
        if (m_settings.getAuthentication() == null) {
            throw new InvalidSettingsException("Not authenticated. Please authenticate in the Node Configuration.");
        }

        // Check if there is an column with a compatible type
        final Map<String, Integer> columnIndexMap = getColumnIndexMap(inSpecs[0]);
        if (columnIndexMap.isEmpty()) {
            throw new InvalidSettingsException("No column with a compatible datatype is available."
                + " See node description for the list of supported datatypes.");
        }
        if (columnIndexMap.size() > POWERBI_MAX_COLUMNS) {
            throw new InvalidSettingsException(
                "The table contains more columns (" + columnIndexMap.size() + ") than supported by the Power BI API ("
                    + POWERBI_MAX_COLUMNS + "). " + " Please filter out unneeded columns.");
        }

        return new DataTableSpec[0];
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        final ExecutionMonitor execPrepare = exec.createSubProgress(PROGRESS_PREPARE);
        execPrepare.setMessage("Authenticating with Microsoft Power BI");

        // Get the input and some information
        final BufferedDataTable inTable = inData[0];
        final double rowCount = inTable.size();
        final DataTableSpec inSpec = inTable.getDataTableSpec();

        // TODO make sure to keep this in mind:
        // https://docs.microsoft.com/en-us/power-bi/developer/api-rest-api-limitations?redirectedfrom=MSDN
        /*
         * Refresh the access token
         * Note: This is not best practice but works for us now. In future versions we should only request a new
         * access token if the old one is not valid anymore and check this for every API call
         */
        final AzureADAuthentication auth;
        try {
            auth = AzureADAuthenticationUtils.refreshToken(m_settings.getAuthentication());
        } catch (final AuthenticationException e) {
            // If the host is unknown it's most likely because the user has no internet (but maybe Microsoft is down)
            if (e.getCause() instanceof UnknownHostException) {
                throw new IllegalStateException(
                    "Cannot connect to Microsoft. Please make sure to have an active internet connection.", e);
            }
            throw e;
        }

        execPrepare.setMessage("Checking for exisiting datasets");

        // Get the settings
        final String tableName = m_settings.getTableName();
        final String datasetName = m_settings.getDatasetName();
        final String workspace = m_settings.getWorkspace();
        final OverwritePolicy overwritePolicy = m_settings.getOverwritePolicy();

        // Get the workspace id (can be null)
        final String workspaceId = getWorkspaceId(auth, workspace);

        // Check if the dataset already exists and get its id
        final Dataset dataset = getDataset(auth, workspaceId, datasetName);
        String datasetId = dataset == null ? null : dataset.getId();

        if (dataset != null) {

            switch (overwritePolicy) {
                case ABORT:
                    throw new InvalidSettingsException(
                        "The dataset with the name \"" + datasetName + "\" already exists.");

                case OVERWRITE:
                    PowerBIRestAPIUtils.deleteDataset(auth, workspaceId, datasetId);
                    datasetId = null;
                    break;

                case APPEND:
                    if (!dataset.isAddRowsAPIEnabled()) {
                        throw new InvalidSettingsException("The dataset with the name \"" + datasetName
                            + "\" already exists and does not support adding rows.");
                    }
                    final Tables tables = PowerBIRestAPIUtils.getTables(auth, workspaceId, datasetId);
                    final Table table = getTableWithName(tables, tableName);
                    if (table == null) {
                        throw new InvalidSettingsException("The dataset with the name \"" + datasetName
                            + "\" already exists but has no table with the name \"" + tableName + "\".");
                    }
                    break;

                default:
                    // Cannot happen
                    break;
            }
        }

        if (datasetId == null) {
            // Create the dataset
            final Table pbiTable = createTableDef(tableName, inSpec);
            final Dataset pbiDataset = PowerBIRestAPIUtils.postDataset(auth, workspaceId, datasetName,
                POWERBI_DATASET_MODE, new Table[]{pbiTable});
            datasetId = pbiDataset.getId();
        }

        // Finish the prepare step
        execPrepare.setProgress(1);

        // Send the table
        final RowsBuilder rowBuilder = new RowsBuilder(getColumnIndexMap(inSpec));
        long rowIdx = 0;
        final ExecutionMonitor execSendRows = exec.createSubProgress(PROGRESS_SEND_ROWS);
        execSendRows.setProgress(0);
        for (final DataRow row : inTable) {
            if (!rowBuilder.acceptsRows()) {
                try {
                    // Send to Power BI
                    PowerBIRestAPIUtils.postRows(auth, workspaceId, datasetId, tableName, rowBuilder.toString());
                    rowBuilder.reset();
                } catch (PowerBIResponseException ex) {
                    throw new RuntimeException("Error while sending data to PowerBI. See log for details.", ex);
                }
            }
            rowBuilder.addRow(row);
            execSendRows.setProgress(rowIdx / rowCount, "Sending row " + rowIdx + " of " + (long)rowCount);
            rowIdx++;
            // TODO can we delete the dataset that is uploaded half way?
            exec.checkCanceled();
        }
        // Send the last rows
        PowerBIRestAPIUtils.postRows(auth, workspaceId, datasetId, tableName, rowBuilder.toString());
        execSendRows.setProgress(1);

        return new BufferedDataTable[0];
    }

    /** Get the table with the given name */
    private static Table getTableWithName(final Tables tables, final String tableName) {
        for (final Table table : tables.getValue()) {
            if (tableName.equals(table.getName())) {
                return table;
            }
        }
        return null;
    }

    /** Get the dataset with the given name */
    private static Dataset getDataset(final AzureADAuthentication auth, final String workspaceId,
        final String datasetName) throws PowerBIResponseException {
        final Datasets datasets = PowerBIRestAPIUtils.getDatasets(auth, workspaceId);
        for (final Dataset dataset : datasets.getValue()) {
            if (datasetName.equals(dataset.getName())) {
                return dataset;
            }
        }
        return null;
    }

    /** Get the id of the workspace if it is not empty. If it does not exist an InvalidSettingsException is thrown. */
    private static String getWorkspaceId(final AzureADAuthentication auth, final String workspace)
        throws PowerBIResponseException, InvalidSettingsException {
        if (workspace == null || workspace.trim().isEmpty()) {
            return null;
        }
        // Find the workspace/group
        final Groups groups = PowerBIRestAPIUtils.getGroups(auth);
        for (final Group g : groups.getValue()) {
            if (workspace.equals(g.getName())) {
                return g.getId();
            }
        }
        throw new InvalidSettingsException("The workspace with the name \"" + workspace + "\" does not exist.");
    }

    /** Creates a Power BI table definition given a KNIME DataTableSpec and a name */
    private static Table createTableDef(final String name, final DataTableSpec tableSpec) {
        final Column[] columns = createColumnsDef(tableSpec);
        return new Table(name, columns);
    }

    /** Creates Power BI column definitions given a KNIME DataTableSpec */
    private static Column[] createColumnsDef(final DataTableSpec tableSpec) {
        final List<Column> columns = new ArrayList<>();
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec columnSpec = tableSpec.getColumnSpec(i);
            final Optional<String> dataType = PowerBIDataTypeUtils.powerBITypeForKNIMEType(columnSpec.getType());
            if (dataType.isPresent()) {
                columns.add(new Column(columnSpec.getName(), dataType.get()));
            }
        }
        return columns.toArray(new Column[0]);
    }

    /** Get a map of compatible columns and their index in the input table */
    private Map<String, Integer> getColumnIndexMap(final DataTableSpec tableSpec) {
        final Map<String, Integer> columns = new HashMap<>();
        final List<String> incompatibleColumns = new ArrayList<String>();
        boolean hasIncompatibleColumns = false;
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec columnSpec = tableSpec.getColumnSpec(i);
            if (PowerBIDataTypeUtils.powerBITypeForKNIMEType(columnSpec.getType()).isPresent()) {
                columns.put(tableSpec.getColumnNames()[i], i);
            } else {
                hasIncompatibleColumns = true;
                incompatibleColumns.add(columnSpec.getName());
            }
        }
        if (hasIncompatibleColumns) {
            LOGGER.warn("The table contains " + incompatibleColumns.size() + " incompatible columns which will be ignored. "
                + "See node description for the list of supported datatypes.");
            String message = "Incompatible columns: ";
            message += ConvenienceMethods.getShortStringFrom(incompatibleColumns, 4);
            setWarningMessage(message);
        }
        return columns;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        try {
            m_settings.saveSettingsTo(settings);
        } catch (IOException | InvalidSettingsException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        SendToPowerBINodeSettings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        try {
            m_settings.loadValidatedSettingsFrom(settings);
        } catch (IOException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do

    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do
    }

    @Override
    protected void reset() {
        // Nothing to do
    }

    /** A builder that takes KNIME rows and adds them to a JSON string */
    private static class RowsBuilder {

        private static final String ROWS_JSON_START = "{\"rows\":[";

        private static final String ROWS_JSON_END = "]}";

        // TODO use smarter metrics to decide if more rows get accepted
        private static final int MAX_ROW_COUNT = 400;

        private final Map<String, Integer> m_columnNameAndIndex;

        private StringBuilder m_builder;

        private long m_rowCount;

        private RowsBuilder(final Map<String, Integer> columnNameAndIndex) {
            m_columnNameAndIndex = columnNameAndIndex;
            reset();
        }

        private void addRow(final DataRow row) {
            boolean firstCol = true;
            m_builder.append(m_rowCount == 0 ? "{" : ",{");
            for (final Entry<String, Integer> colNameIndex : m_columnNameAndIndex.entrySet()) {
                final Optional<String> value =
                    PowerBIDataTypeUtils.powerBIValueForKNIMEValue(row.getCell(colNameIndex.getValue()));
                if (value.isPresent()) {
                    m_builder.append((firstCol ? "\"" : ",\"") + colNameIndex.getKey() + "\":");
                    m_builder.append(value.get());
                    firstCol = false;
                }
            }
            m_builder.append("}");
            m_rowCount++;
        }

        private boolean acceptsRows() {
            return m_rowCount < MAX_ROW_COUNT;
        }

        private void reset() {
            m_builder = new StringBuilder();
            m_builder.append(ROWS_JSON_START);
            m_rowCount = 0;
        }

        @Override
        public String toString() {
            return m_builder.append(ROWS_JSON_END).toString();
        }
    }
}
