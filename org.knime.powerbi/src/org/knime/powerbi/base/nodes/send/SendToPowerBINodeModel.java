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
package org.knime.powerbi.base.nodes.send;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.knime.azuread.auth.AzureADAuthentication;
import org.knime.azuread.auth.AzureADAuthenticationUtils;
import org.knime.azuread.auth.DefaultOAuth20Scope;
import org.knime.azuread.auth.OAuth20Scope;
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
import org.knime.powerbi.base.nodes.send.SendToPowerBINodeSettings.OverwritePolicy;
import org.knime.powerbi.core.PowerBIDataTypeUtils;
import org.knime.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.powerbi.core.rest.bindings.Column;
import org.knime.powerbi.core.rest.bindings.Dataset;
import org.knime.powerbi.core.rest.bindings.Datasets;
import org.knime.powerbi.core.rest.bindings.Table;
import org.knime.powerbi.core.rest.bindings.Tables;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
class SendToPowerBINodeModel extends NodeModel {

    private static final String POWERBI_DATASET_MODE = "Push";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeModel.class);

    static final OAuth20Scope OAUTH_POWERBI_SCOPE = new DefaultOAuth20Scope(new String[]{ //
        "offline_access", // Required for the refresh token
        "https://analysis.windows.net/powerbi/api/Dataset.Read.All", // Required to list datasets
        "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All" // Required to upload datasets
    });

    private final SendToPowerBINodeSettings m_settings;

    SendToPowerBINodeModel() {
        super(1, 0);
        m_settings = new SendToPowerBINodeSettings();
    }

    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        if (m_settings.getAuthentication() == null) {
            throw new InvalidSettingsException("Not authenticated. Please authenticate in the Node Configuration.");
        }
        return new DataTableSpec[0];
    }

    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        // Get the input and some information
        final BufferedDataTable inTable = inData[0];
        final double rowCount = inTable.size();
        final DataTableSpec inSpec = inTable.getDataTableSpec();
        final String[] columnNames = inSpec.getColumnNames();

        // TODO make sure to keep this in mind: https://docs.microsoft.com/en-us/power-bi/developer/api-rest-api-limitations?redirectedfrom=MSDN
        // TODO datasets in groups?

        // Refresh the access token
        // Note: This is not best practice but works for us now.
        // In future versions we should only request a new access token if the old one is not valid anymore and check this for every API call
        final AzureADAuthentication auth = AzureADAuthenticationUtils.refreshToken(m_settings.getAuthentication());

        // Get the settings
        final String tableName = m_settings.getTableName();
        final String datasetName = m_settings.getDatasetName();
        final OverwritePolicy overwritePolicy = m_settings.getOverwritePolicy();

        // Check if the dataset already exists and get its id
        String datasetId = getDatasetId(auth, datasetName);

        if (datasetId != null) {

            switch (overwritePolicy) {
                case ABORT:
                    // TODO throw other exception?
                    throw new IllegalStateException(
                        "The dataset with the name \"" + datasetName + "\" already exists.");

                case OVERWRITE:
                    PowerBIRestAPIUtils.deleteDataset(auth, datasetId);
                    datasetId = null;
                    break;

                case APPEND:
                    final Tables tables = PowerBIRestAPIUtils.getTables(auth, datasetId);
                    final Table table = getTableWithName(tables, tableName);
                    if (table == null) {
                        // Add the table to the existing dataset
                        final Column[] columns = createColumnsDef(inSpec);
                        PowerBIRestAPIUtils.putTable(auth, datasetId, tableName, columns);
                    } // else {
                      // Check that the table is compatible with the input table
                      // if (!isCompatible(table, inSpec)) {
                      //    throw new IllegalStateException("The dataset with the name \"" + datasetName
                      //        + "\" has a table with the name \"" + tableName
                      //        + "\". However, the existing table is not compatible with the input table.");
                      // }
                      // If it is compatible we will just add rows
                      // }
                    break;

                default:
                    // Cannot happen
                    break;
            }
        }

        if (datasetId == null) {
            // Create the dataset
            final Table pbiTable = createTableDef(tableName, inSpec);
            final Dataset pbiDataset =
                PowerBIRestAPIUtils.createDataset(auth, datasetName, POWERBI_DATASET_MODE, new Table[]{pbiTable});
            datasetId = pbiDataset.getId();
        }

        // Send the table
        RowsBuilder rowBuilder = new RowsBuilder();
        long rowIdx = 0;
        exec.setProgress(0);
        for (final DataRow row : inTable) {
            rowBuilder.addRow(columnNames, row);
            if (!rowBuilder.acceptsRows()) {
                // Send to PowerBI
                PowerBIRestAPIUtils.addRows(auth, datasetId, tableName, rowBuilder.toString());
                rowBuilder = new RowsBuilder();
            }
            exec.setProgress(++rowIdx / rowCount);
            // TODO can we delete the dataset that is uploaded half way?
            exec.checkCanceled();
        }
        // Send the last rows
        PowerBIRestAPIUtils.addRows(auth, datasetId, tableName, rowBuilder.toString());

        return new BufferedDataTable[0];
    }

    /** Checks if the PowerBI table is compatible with the given table spec */
    private static boolean isCompatible(final Table pbiTable, final DataTableSpec inSpec) {
        final Table inputTable = createTableDef(pbiTable.getName(), inSpec);
        final Column[] inColumns = inputTable.getColumns();
        final Column[] pbiColumns = pbiTable.getColumns();

        // TODO ignore column order?
        // TODO move to Table#equals?

        // Same number of columns
        if (inColumns.length != pbiColumns.length) {
            return false;
        }

        // Loop over columns
        for (int i = 0; i < inColumns.length; i++) {
            final Column inCol = inColumns[i];
            final Column pbiCol = pbiColumns[i];

            // Column name and type
            if (!inCol.getName().equals(pbiCol.getName()) || //
                !inCol.getDataType().equals(pbiCol.getDataType())) {
                return false;
            }
        }

        // All checks passed
        return true;
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

    /** Check if a dataset exists. Returns the dataset id or null */
    private static String getDatasetId(final AzureADAuthentication auth, final String datasetName)
        throws PowerBIResponseException {
        final Datasets datasets = PowerBIRestAPIUtils.getDatasets(auth);
        for (final Dataset dataset : datasets.getValue()) {
            if (datasetName.equals(dataset.getName())) {
                return dataset.getId();
            }
        }
        return null;
    }

    /** Creates a PowerBI table definition given a KNIME DataTableSpec and a name */
    private static Table createTableDef(final String name, final DataTableSpec tableSpec) {
        final Column[] columns = createColumnsDef(tableSpec);
        return new Table(name, columns);
    }

    /** Creates PowerBI column definitions given a KNIME DataTableSpec */
    private static Column[] createColumnsDef(final DataTableSpec tableSpec) {
        final Column[] columns = new Column[tableSpec.getNumColumns()];
        for (int i = 0; i < columns.length; i++) {
            final DataColumnSpec columnSpec = tableSpec.getColumnSpec(i);
            final Optional<String> dataType = PowerBIDataTypeUtils.powerBITypeForKNIMEType(columnSpec.getType());
            // TODO document in the node description that only compatible columns are transfered
            // TODO warn for incomptablible column
            if (dataType.isPresent()) {
                columns[i] = new Column(columnSpec.getName(), dataType.get());
            }
        }
        return columns;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
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

        private final StringBuilder builder;

        private long rowCount;

        private RowsBuilder() {
            builder = new StringBuilder();
            builder.append(ROWS_JSON_START);
            rowCount = 0;
        }

        private void addRow(final String[] columnNames, final DataRow row) {
            builder.append(rowCount == 0 ? "{" : ",{");
            for (int i = 0; i < columnNames.length; i++) {
                final Optional<String> value = PowerBIDataTypeUtils.powerBIValueForKNIMEValue(row.getCell(i));
                if (value.isPresent()) {
                    builder.append((i == 0 ? "\"" : ",\"") + columnNames[i] + "\":");
                    builder.append(value.get());
                }
            }
            builder.append("}");
            rowCount++;
        }

        private boolean acceptsRows() {
            return rowCount < MAX_ROW_COUNT;
        }

        @Override
        public String toString() {
            return builder.append(ROWS_JSON_END).toString();
        }
    }
}
