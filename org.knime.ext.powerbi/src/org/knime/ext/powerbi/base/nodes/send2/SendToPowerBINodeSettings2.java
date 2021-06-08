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
 *   Oct 9, 2019 (benjamin): created
 */
package org.knime.ext.powerbi.base.nodes.send2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings store managing all configurations required to send to data to PowerBI.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeSettings2 {

    private static final String CFG_KEY_WORKSPACE = "workspace";

    private static final String CFG_KEY_DATASET_NAME = "dataset_name";

    private static final String CFG_KEY_TABLE_NAMES = "table_name";

    private static final String CFG_KEY_DATASET_NAME_DIALOG = "dataset_name_dialog";

    private static final String CFG_KEY_TABLE_NAMES_DIALOG = "table_name_dialog";

    private static final String CFG_KEY_CREATE_NEW_DATASET = "create_new_dataset";

    private static final String CFG_KEY_ALLOW_OVERWRITE = "allow_overwrite";

    private static final String CFG_KEY_APPEND_ROWS = "append_rows";

    private String m_workspace = "";

    private String m_datasetName = "";

    private String[] m_tableNames = {""};

    /** The unused dataset name in the unselected dialog option */
    private String m_datasetNameDialog = "";

    /** The unused table names in the unselected dialog option */
    private String[] m_tableNamesDialog = {""};

    private boolean m_createNewDataset = true;

    private boolean m_allowOverwrite = false;

    private boolean m_appendRows = true;

    /**
     * @return the workspace
     */
    String getWorkspace() {
        return m_workspace;
    }

    /**
     * @param workspace the workspace to set
     */
    void setWorkspace(final String workspace) {
        m_workspace = workspace;
    }

    /**
     * @return the datasetName
     */
    String getDatasetName() {
        return m_datasetName;
    }

    /**
     * @param datasetName the datasetName to set
     */
    void setDatasetName(final String datasetName) {
        m_datasetName = datasetName;
    }

    /**
     * @return the tableNames
     */
    String[] getTableNames() {
        return m_tableNames;
    }

    /**
     * @param tableNames the tableNames to set
     */
    void setTableNames(final String[] tableNames) {
        m_tableNames = tableNames;
    }

    /**
     * @return the datasetNameDialog
     */
    String getDatasetNameDialog() {
        return m_datasetNameDialog;
    }

    /**
     * @param datasetNameDialog the datasetNameDialog to set
     */
    void setDatasetNameDialog(final String datasetNameDialog) {
        m_datasetNameDialog = datasetNameDialog;
    }

    /**
     * @return the tableNamesDialog
     */
    String[] getTableNamesDialog() {
        return m_tableNamesDialog;
    }

    /**
     * @param tableNamesDialog the tableNamesDialog to set
     */
    void setTableNamesDialog(final String[] tableNamesDialog) {
        m_tableNamesDialog = tableNamesDialog;
    }

    /**
     * @return the createNewDataset
     */
    boolean isCreateNewDataset() {
        return m_createNewDataset;
    }

    /**
     * @param createNewDataset the createNewDataset to set
     */
    void setCreateNewDataset(final boolean createNewDataset) {
        m_createNewDataset = createNewDataset;
    }

    /**
     * @return the allowOverwrite
     */
    boolean isAllowOverwrite() {
        return m_allowOverwrite;
    }

    /**
     * @param allowOverwrite the allowOverwrite to set
     */
    void setAllowOverwrite(final boolean allowOverwrite) {
        m_allowOverwrite = allowOverwrite;
    }

    /**
     * @return the appendRows
     */
    boolean isAppendRows() {
        return m_appendRows;
    }

    /**
     * @param appendRows the appendRows to set
     */
    void setAppendRows(final boolean appendRows) {
        m_appendRows = appendRows;
    }

    void saveSettingsTo(final NodeSettingsWO settings) throws IOException, InvalidSettingsException {
        settings.addString(CFG_KEY_WORKSPACE, getWorkspace());
        settings.addString(CFG_KEY_DATASET_NAME, getDatasetName());
        settings.addStringArray(CFG_KEY_TABLE_NAMES, getTableNames());
        settings.addString(CFG_KEY_DATASET_NAME_DIALOG, getDatasetNameDialog());
        settings.addStringArray(CFG_KEY_TABLE_NAMES_DIALOG, getTableNamesDialog());
        settings.addBoolean(CFG_KEY_CREATE_NEW_DATASET, m_createNewDataset);
        settings.addBoolean(CFG_KEY_ALLOW_OVERWRITE, m_allowOverwrite);
        settings.addBoolean(CFG_KEY_APPEND_ROWS, m_appendRows);
    }

    static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Note that the workspace config can be empty
        settings.getString(CFG_KEY_WORKSPACE);

        // Check the dataset name
        final String datasetName = settings.getString(CFG_KEY_DATASET_NAME);
        if (datasetName == null || datasetName.trim().isEmpty()
            || datasetName.equals(SendToPowerBINodeDialog2.DATASET_PLACEHOLDER)) {
            throw new InvalidSettingsException("The dataset name must be set.");
        }

        // Check the table names
        final String[] tableNames = settings.getStringArray(CFG_KEY_TABLE_NAMES);
        checkTableNamesValid(tableNames);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException, IOException {
        setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
        setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
        setTableNames(settings.getStringArray(CFG_KEY_TABLE_NAMES));
        setDatasetNameDialog(settings.getString(CFG_KEY_DATASET_NAME_DIALOG, ""));
        setTableNamesDialog(settings.getStringArray(CFG_KEY_TABLE_NAMES_DIALOG, ""));

        setCreateNewDataset(settings.getBoolean(CFG_KEY_CREATE_NEW_DATASET));
        setAllowOverwrite(settings.getBoolean(CFG_KEY_ALLOW_OVERWRITE));
        setAppendRows(settings.getBoolean(CFG_KEY_APPEND_ROWS, true));
    }

    /** Checks that no table name are valid. All set and none twice. */
    private static void checkTableNamesValid(final String[] tableNames) throws InvalidSettingsException {
        // Loop over names and check
        final Set<String> allNames = new HashSet<>();
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i] == null || tableNames[i].trim().isEmpty()
                || tableNames[i].equals(SendToPowerBINodeDialog2.TABLE_PLACEHOLDER)) {
                if (i == 0 && tableNames.length == 1) {
                    throw new InvalidSettingsException("Table name must not be empty.");
                } else {
                    throw new InvalidSettingsException("Table name for table " + (i + 1) + " must not be empty.");
                }
            }
            allNames.add(tableNames[i]);
        }

        // Check if none of them are equal
        if (allNames.size() != tableNames.length) {
            throw new InvalidSettingsException("Please use unique table names for each table.");
        }
    }
}
