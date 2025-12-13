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
 * ------------------------------------------------------------------------
 */
package org.knime.ext.powerbi.base.nodes.send2;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.internal.persistence.ArrayPersistor;
import org.knime.ext.powerbi.base.nodes.send2.SendToPowerBINodeParameters.DatasetMode;
import org.knime.ext.powerbi.base.nodes.send2.SendToPowerBINodeParameters.TableName;

/**
 * Persistor for table names array that transforms between the old string array format and the new object-based format
 * using ArrayPersistor interface.
 *
 * <p>
 * Note regarding node settings storage (which has been grown historically): The settings store two fields (arrays for
 * table names):
 * <ul>
 * <li>One that holds the names of the tables, which is then also used during execution.</li>
 * <li>Another field ("_DIALOG") that holds the names for the non-used mode to populate the dialog (only)</li>
 * </ul>
 *
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
final class TableNamesArrayPersistor implements ArrayPersistor<Integer, TableName> {

    @Override
    public int getArrayLength(final NodeSettingsRO nodeSettings) throws InvalidSettingsException {
        final var tableNames =
            nodeSettings.getStringArray(SendToPowerBINodeSettings2.CFG_KEY_TABLE_NAMES, ArrayUtils.EMPTY_STRING_ARRAY);
        return tableNames.length;
    }

    @Override
    public Integer createElementLoadContext(final int index) {
        return index;
    }

    @Override
    public TableName createElementSaveDTO(final int index) {
        return new TableName();
    }

    @Override
    public void save(final List<TableName> tableNames, final NodeSettingsWO settings) {
        final var tableNamesArray1 = new String[tableNames.size()];
        final var tableNamesArray2 = new String[tableNames.size()];
        boolean isUsingCreateMode = false;

        for (int i = 0; i < tableNames.size(); i++) {
            final var tableName = tableNames.get(i);
            isUsingCreateMode = tableName.m_datasetMode == DatasetMode.CREATE_NEW;
            tableNamesArray1[i] = tableName.m_tableNameCreate;
            tableNamesArray2[i] = tableName.m_tableNameSelect;
        }

        final var tableNamesArray = isUsingCreateMode ? tableNamesArray1 : tableNamesArray2;
        final var tableNamesArrayDialog = isUsingCreateMode ? tableNamesArray2 : tableNamesArray1;

        settings.addStringArray(SendToPowerBINodeSettings2.CFG_KEY_TABLE_NAMES, tableNamesArray);
        settings.addStringArray(SendToPowerBINodeSettings2.CFG_KEY_TABLE_NAMES_DIALOG, tableNamesArrayDialog);
    }
}
