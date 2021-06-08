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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.util.ConvenienceMethods;
import org.knime.ext.azuread.auth.DefaultOAuth20Scope;
import org.knime.ext.azuread.auth.OAuth20Scope;
import org.knime.ext.powerbi.core.PowerBIDataTypeUtils;
import org.knime.ext.powerbi.core.PowerBIDataTypeUtils.PowerBIIllegalValueException;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.ext.powerbi.core.rest.bindings.Column;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.core.rest.bindings.Table;
import org.knime.ext.powerbi.core.rest.bindings.Tables;

import com.google.gson.Gson;

/**
 * Send to Power BI node model.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 * @deprecated
 */
@Deprecated
final class SendToPowerBINodeModel extends NodeModel {

    private static final Gson GSON = new Gson();

    private static final double PROGRESS_PREPARE = 0.3;

    private static final double PROGRESS_SEND_ROWS = 1 - PROGRESS_PREPARE;

    private static final int POWERBI_MAX_COLUMNS = 75;

    private static final int POWERBI_MAX_TABLES = 75;

    private static final int POWERBI_MAX_ROWS_PER_HOUR = 1000000;

    private static final int POWERBI_MAX_ROWS_NONE_RETENTION = 5000000;

    /** 10000 rows per request are allowed */
    private static final int REQUEST_MAX_ROW_COUNT = 10000;

    /** Limit around 8MB (64MB is the limit of the server but this seemed a bit high) */
    private static final int REQUEST_MAX_BODY_LENGTH = 8 * 1024 * 1024;

    private static final String POWERBI_DATASET_MODE = "Push";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeModel.class);

    static final OAuth20Scope OAUTH_POWERBI_SCOPE = new DefaultOAuth20Scope( //
        "offline_access", // Required for the refresh token
        "https://analysis.windows.net/powerbi/api/Dataset.Read.All", // Required to list datasets
        "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All", // Required to upload datasets
        "https://analysis.windows.net/powerbi/api/Workspace.Read.All" // Required to get the workspaces
    );

    private final SendToPowerBINodeSettings m_settings;

    SendToPowerBINodeModel(final PortsConfiguration portsConfiguration) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_settings = new SendToPowerBINodeSettings();
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        // Check if the user is authenticated
        if (m_settings.getAuthentication() == null) {
            throw new InvalidSettingsException("Not authenticated. Please authenticate in the Node Configuration.");
        }
        // Check if a table name for each table is configured
        if (inSpecs.length > m_settings.getTableNames().length) {
            throw new InvalidSettingsException("Not all table names are configured. Please reconfigure the node.");
        }

        // Check that there are no more than 75 tables
        if (inSpecs.length > POWERBI_MAX_TABLES) {
            throw new InvalidSettingsException("Too many table inputs are configured (" + inSpecs.length
                + "). The Power BI API only allows " + POWERBI_MAX_TABLES + " tables per dataset.");
        }

        // Check if there is an column with a compatible type in each table
        for (int i = 0; i < inSpecs.length; i++) {
            final Map<String, Integer> columnIndexMap = getColumnIndexMap(inSpecs[i]);
            if (columnIndexMap.isEmpty()) {
                throw new InvalidSettingsException("No column with a compatible datatype is available in table "
                    + (i + 1) + ". See the node description for the list of supported datatypes.");
            }
            // Check that there are no more than 75 columns
            if (columnIndexMap.size() > POWERBI_MAX_COLUMNS) {
                throw new InvalidSettingsException("Table " + (i + 1) + " contains more columns ("
                    + columnIndexMap.size() + ") than supported by the Power BI API (" + POWERBI_MAX_COLUMNS + "). "
                    + " Please filter out unneeded columns.");
            }
        }

        return new DataTableSpec[0];
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        final ExecutionMonitor execPrepare = exec.createSubProgress(PROGRESS_PREPARE);
        execPrepare.setMessage("Checking for exisiting datasets");

        checkTableSize(inData);

        // Get the settings
        final AuthTokenProvider auth = new AzureADAuthTokenSupplier(m_settings.getAuthentication());
        final String[] tableNames = m_settings.getTableNames();
        final String datasetName = m_settings.getDatasetName();
        final String workspaceId = m_settings.getWorkspace().isEmpty() ? null : m_settings.getWorkspace();
        final boolean createNewDataset = m_settings.isCreateNewDataset();
        final boolean allowOverwrite = m_settings.isAllowOverwrite();
        final boolean appendToExisting = m_settings.isAppendRows();

        // Get the workspace id (can be null)

        // Check if the dataset already exists and get its id
        final Dataset dataset = getDataset(auth, workspaceId, datasetName);
        String datasetId = dataset == null ? null : dataset.getId();

        if (createNewDataset && dataset != null) {
            if (allowOverwrite) {
                // Delete the dataset
                PowerBIRestAPIUtils.deleteDataset(auth, workspaceId, datasetId);
                datasetId = null;
            } else {
                // Fail because the dataset exists
                throw new InvalidSettingsException("The dataset with the name \"" + datasetName + "\" already exists.");
            }
        } else if (!createNewDataset) {
            if (dataset == null) {
                // Fail because the dataset does not exist
                throw new InvalidSettingsException(
                    "The selected dataset with the name \"" + datasetName + "\" does not exist anymore.");
            } else {
                // Check if the dataset works for appending
                if (!dataset.isAddRowsAPIEnabled()) {
                    throw new InvalidSettingsException("The dataset with the name \"" + datasetName
                        + "\" already exists and does not support adding rows.");
                }
                final Tables tables = PowerBIRestAPIUtils.getTables(auth, workspaceId, datasetId);
                checkTablesExist(tables, tableNames);
                // If refreshing we need to delete the selected tables
                if (!appendToExisting) {
                    deleteRowsFromTables(auth, workspaceId, datasetId, tableNames);
                }
            }
        }

        if (datasetId == null) {
            // Create the dataset
            final Table[] tables = new Table[inData.length];
            for (int i = 0; i < inData.length; i++) {
                tables[i] = createTableDef(tableNames[i], inData[i].getDataTableSpec());
            }
            final Dataset pbiDataset =
                PowerBIRestAPIUtils.postDataset(auth, workspaceId, datasetName, POWERBI_DATASET_MODE, tables);
            datasetId = pbiDataset.getId();
        }

        // Finish the prepare step
        execPrepare.setProgress(1);

        // Send the tables
        for (int i = 0; i < inData.length; i++) {
            final ExecutionMonitor execSendRows = exec.createSubProgress(PROGRESS_SEND_ROWS / inData.length);
            sendTable(inData[i], execSendRows, auth, workspaceId, datasetId, tableNames[i]);
        }

        return new BufferedDataTable[0];
    }

    private void sendTable(final BufferedDataTable table, final ExecutionMonitor exec, final AuthTokenProvider auth,
        final String workspaceId, final String datasetId, final String tableName)
        throws CanceledExecutionException, PowerBIResponseException, PowerBIIllegalValueException {
        final RowsBuilder rowBuilder = new RowsBuilder(getColumnIndexMap(table.getDataTableSpec()));
        long rowIdx = 0;
        final double rowCount = table.size();
        exec.setProgress(0);
        for (final DataRow row : table) {
            if (!rowBuilder.acceptsRows()) {
                // Send to Power BI
                PowerBIRestAPIUtils.postRows(auth, workspaceId, datasetId, tableName, rowBuilder.toString());
                rowBuilder.reset();
            }
            rowBuilder.addRow(row);
            exec.setProgress(rowIdx / rowCount, "Sending row " + rowIdx + " of " + (long)rowCount);
            rowIdx++;
            // TODO can we delete the dataset that is uploaded half way?
            exec.checkCanceled();
        }
        // Send the last rows
        PowerBIRestAPIUtils.postRows(auth, workspaceId, datasetId, tableName, rowBuilder.toString());
        exec.setProgress(1);
    }

    /** Deletes all rows from the given tables from the given dataset */
    private static void deleteRowsFromTables(final AuthTokenProvider auth, final String workspaceId,
        final String datasetId, final String[] tableNames) throws PowerBIResponseException {
        for (final String t : tableNames) {
            PowerBIRestAPIUtils.deleteRows(auth, workspaceId, datasetId, t);
        }
    }

    /** Checks the size of the given tables. Sets a warning if > 1M rows and throws exception if > 5M rows */
    private void checkTableSize(final BufferedDataTable[] inData) throws InvalidSettingsException {
        // Check the size of the tables
        long totalNumRows = 0;
        for (int i = 0; i < inData.length; i++) {
            if (inData[i].size() > POWERBI_MAX_ROWS_NONE_RETENTION) {
                throw new InvalidSettingsException("Table " + (i + 1) + " contains more than "
                    + POWERBI_MAX_ROWS_NONE_RETENTION + " rows which is not supported by Power BI. Note that only "
                    + POWERBI_MAX_ROWS_PER_HOUR + " can be uploaded per hour.");
            }
            totalNumRows += inData[i].size();
        }
        if (totalNumRows > POWERBI_MAX_ROWS_PER_HOUR) {
            final String prefix = inData.length == 1 ? "The input table contains " : "The input tables contain ";
            setWarningMessage(prefix + "more rows than can be uploaded to Power BI per hour. See log for details.");
            LOGGER.warn(prefix + totalNumRows
                + " rows in total which is more than the maximum amount that can be uploaded to Power BI in one hour ("
                + POWERBI_MAX_ROWS_PER_HOUR + "). " + "The node will probably fail because of this. "
                + "Consider filtering the rows and sending only " + POWERBI_MAX_ROWS_PER_HOUR + " rows per hour.");
        }
    }

    /** Get the table with the given name */
    private static void checkTablesExist(final Tables tables, final String[] tableNames)
        throws InvalidSettingsException {
        final Set<String> availableTables =
            Arrays.stream(tables.getValue()).map(Table::getName).collect(Collectors.toSet());
        for (final String t : tableNames) {
            if (!availableTables.contains(t)) {
                throw new InvalidSettingsException(
                    "The dataset contains no table with the name \"" + t + "\". Please reconfigure the node.");
            }
        }
    }

    /** Get the dataset with the given name */
    private static Dataset getDataset(final AuthTokenProvider auth, final String workspaceId, final String datasetName)
        throws PowerBIResponseException {
        final Datasets datasets = PowerBIRestAPIUtils.getDatasets(auth, workspaceId);
        for (final Dataset dataset : datasets.getValue()) {
            if (datasetName.equals(dataset.getName())) {
                return dataset;
            }
        }
        return null;
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

    /** Get a map of compatible columns (as JSON strings) and their index in the input table */
    private Map<String, Integer> getColumnIndexMap(final DataTableSpec tableSpec) {
        final Map<String, Integer> columns = new HashMap<>();
        final List<String> incompatibleColumns = new ArrayList<>();
        boolean hasIncompatibleColumns = false;
        for (int i = 0; i < tableSpec.getNumColumns(); i++) {
            final DataColumnSpec columnSpec = tableSpec.getColumnSpec(i);
            if (PowerBIDataTypeUtils.powerBITypeForKNIMEType(columnSpec.getType()).isPresent()) {
                final String columnName = GSON.toJson(tableSpec.getColumnNames()[i]);
                columns.put(columnName, i);
            } else {
                hasIncompatibleColumns = true;
                incompatibleColumns.add(columnSpec.getName());
            }
        }
        if (hasIncompatibleColumns) {
            LOGGER.warn(
                "The table contains " + incompatibleColumns.size() + " incompatible columns which will be ignored. "
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
        /* Note:
        * Do not check if the number of table names is correct.
        * It could be incorrect after adding a port and we still want to load the settings
        */
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
    private static final class RowsBuilder {

        private static final String ROWS_JSON_START = "{\"rows\":[";

        private static final String ROWS_JSON_END = "]}";

        private final Map<String, Integer> m_columnNameAndIndex;

        private StringBuilder m_builder;

        private long m_rowCount;

        private RowsBuilder(final Map<String, Integer> columnNameAndIndex) {
            m_columnNameAndIndex = columnNameAndIndex;
            reset();
        }

        private void addRow(final DataRow row) throws PowerBIIllegalValueException {
            boolean firstCol = true;
            m_builder.append(m_rowCount == 0 ? "{" : ",{");
            for (final Entry<String, Integer> colNameIndex : m_columnNameAndIndex.entrySet()) {
                final Optional<String> value =
                    PowerBIDataTypeUtils.powerBIValueForKNIMEValue(row.getCell(colNameIndex.getValue()));
                if (value.isPresent()) {
                    if (!firstCol) {
                        m_builder.append(",");
                    }
                    m_builder.append(colNameIndex.getKey() + ":");
                    m_builder.append(value.get());
                    firstCol = false;
                }
            }
            m_builder.append("}");
            m_rowCount++;
        }

        private boolean acceptsRows() {
            return m_rowCount < REQUEST_MAX_ROW_COUNT //
                && m_builder.length() < REQUEST_MAX_BODY_LENGTH - 2;
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
