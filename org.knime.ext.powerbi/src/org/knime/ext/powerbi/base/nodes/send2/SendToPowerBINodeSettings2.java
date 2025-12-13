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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.ext.powerbi.core.rest.bindings.Relationship;

import com.google.common.base.Objects;

/**
 * Settings store managing all configurations required to send to data to PowerBI.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeSettings2 {

    static final String CFG_KEY_WORKSPACE = "workspace";

    static final String CFG_KEY_DATASET_NAME = "dataset_name";

    static final String CFG_KEY_TABLE_NAMES = "table_name";

    static final String CFG_KEY_DATASET_NAME_DIALOG = "dataset_name_dialog";

    static final String CFG_KEY_TABLE_NAMES_DIALOG = "table_name_dialog";

    static final String CFG_KEY_RELATIONSHIP_FROMTABLES = "relationship_fromtables";

    static final String CFG_KEY_RELATIONSHIP_FROMCOLUMNS = "relationship_fromcolumns";

    static final String CFG_KEY_RELATIONSHIP_TOTABLES = "relationship_totables";

    static final String CFG_KEY_RELATIONSHIP_TOCOLUMNS = "relationship_tocolumns";

    static final String CFG_KEY_RELATIONSHIP_CROSSFILTERBEHAVIORS = "relationship_crossfilterbehaviors";

    static final String CFG_KEY_CREATE_NEW_DATASET = "create_new_dataset";

    static final String CFG_KEY_ALLOW_OVERWRITE = "allow_overwrite";

    static final String CFG_KEY_APPEND_ROWS = "append_rows";

    private String m_workspace = "";

    private String m_datasetName = "";

    private String[] m_tableNames = {""};

    /** The unused dataset name in the unselected dialog option */
    private String m_datasetNameDialog = "";

    /** The unused table names in the unselected dialog option */
    private String[] m_tableNamesDialog = {""};

    private String[] m_relationshipFromTables = {};

    private String[] m_relationshipFromColumns = {};

    private String[] m_relationshipToTables = {};

    private String[] m_relationshipToColumns = {};

    private String[] m_relationshipCrossfilterBehaviors = {};

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

    void setRelationshipCrossfilterBehaviors(final String[] stringArray) {
        m_relationshipCrossfilterBehaviors = stringArray;

    }

    void setRelationshipToColumns(final String[] stringArray) {
        m_relationshipToColumns = stringArray;

    }

    void setRelationshipToTables(final String[] stringArray) {
        m_relationshipToTables = stringArray;

    }

    void setRelationshipFromColumns(final String[] stringArray) {
        m_relationshipFromColumns = stringArray;

    }

    void setRelationshipFromTables(final String[] stringArray) {
        m_relationshipFromTables = stringArray;

    }

    String[] getRelationshipCrossfilterBehaviors() {
        return m_relationshipCrossfilterBehaviors;
    }

    String[] getRelationshipToColumns() {
        return m_relationshipToColumns;
    }

    String[] getRelationshipToTables() {
        return m_relationshipToTables;
    }

    String[] getRelationshipFromColumns() {
        return m_relationshipFromColumns;
    }

    String[] getRelationshipFromTables() {
        return m_relationshipFromTables;
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

        settings.addStringArray(CFG_KEY_RELATIONSHIP_FROMTABLES, getRelationshipFromTables());
        settings.addStringArray(CFG_KEY_RELATIONSHIP_FROMCOLUMNS, getRelationshipFromColumns());
        settings.addStringArray(CFG_KEY_RELATIONSHIP_TOTABLES, getRelationshipToTables());
        settings.addStringArray(CFG_KEY_RELATIONSHIP_TOCOLUMNS, getRelationshipToColumns());
        settings.addStringArray(CFG_KEY_RELATIONSHIP_CROSSFILTERBEHAVIORS, getRelationshipCrossfilterBehaviors());

        settings.addBoolean(CFG_KEY_CREATE_NEW_DATASET, m_createNewDataset);
        settings.addBoolean(CFG_KEY_ALLOW_OVERWRITE, m_allowOverwrite);
        settings.addBoolean(CFG_KEY_APPEND_ROWS, m_appendRows);
    }

    static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Note that the workspace config can be empty
        settings.getString(CFG_KEY_WORKSPACE);

        // Check the dataset name
        final String datasetName = settings.getString(CFG_KEY_DATASET_NAME);
        if (StringUtils.isBlank(datasetName)) {
            throw new InvalidSettingsException("The dataset name must be set.");
        }

        // Check the table names
        final String[] tableNames = settings.getStringArray(CFG_KEY_TABLE_NAMES);
        checkTableNamesValid(tableNames);

        String[] fromTables = settings.getStringArray(CFG_KEY_RELATIONSHIP_FROMTABLES);
        String[] fromColumns = settings.getStringArray(CFG_KEY_RELATIONSHIP_FROMCOLUMNS);
        String[] toTables = settings.getStringArray(CFG_KEY_RELATIONSHIP_TOTABLES);
        String[] toColumns = settings.getStringArray(CFG_KEY_RELATIONSHIP_TOCOLUMNS);
        checkRelationshipsValid(fromTables, fromColumns, toTables, toColumns);
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException, IOException {
        setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
        setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
        setTableNames(settings.getStringArray(CFG_KEY_TABLE_NAMES));
        setDatasetNameDialog(settings.getString(CFG_KEY_DATASET_NAME_DIALOG, ""));
        setTableNamesDialog(settings.getStringArray(CFG_KEY_TABLE_NAMES_DIALOG, ""));

        setRelationshipFromTables(settings.getStringArray(CFG_KEY_RELATIONSHIP_FROMTABLES));
        setRelationshipFromColumns(settings.getStringArray(CFG_KEY_RELATIONSHIP_FROMCOLUMNS));
        setRelationshipToTables(settings.getStringArray(CFG_KEY_RELATIONSHIP_TOTABLES));
        setRelationshipToColumns(settings.getStringArray(CFG_KEY_RELATIONSHIP_TOCOLUMNS));
        setRelationshipCrossfilterBehaviors(settings.getStringArray(CFG_KEY_RELATIONSHIP_CROSSFILTERBEHAVIORS));

        setCreateNewDataset(settings.getBoolean(CFG_KEY_CREATE_NEW_DATASET));
        setAllowOverwrite(settings.getBoolean(CFG_KEY_ALLOW_OVERWRITE));
        setAppendRows(settings.getBoolean(CFG_KEY_APPEND_ROWS, true));
    }

    /** Checks that no table name are valid. All set and none twice. */
    private static void checkTableNamesValid(final String[] tableNames) throws InvalidSettingsException {
        // Loop over names and check
        final Set<String> allNames = new HashSet<>();
        for (int i = 0; i < tableNames.length; i++) {
            if (StringUtils.isBlank(tableNames[i])) {
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

    /**
     * @param fromTables names of the source tables (per relationship)
     * @param fromColumns names of the source columns (per relationship)
     * @param toTables names of the target tables (per relationship)
     * @param toColumns names of the target columns (per relationship)
     * @throws InvalidSettingsException a) if a relationship has identical source and target table or b) two
     *             relationships define the same source table+column and target table+column
     */
    private static void checkRelationshipsValid(final String[] fromTables, final String[] fromColumns,
        final String[] toTables, final String[] toColumns) throws InvalidSettingsException {
        Map<Relationship, Integer> rs = new HashMap<>();
        for (int i = 0; i < fromTables.length; i++) {
            int relationshipNumber = i + 1;
            // check for self-referential relationships
            if (Objects.equal(fromTables[i], toTables[i])) {
                throw new InvalidSettingsException(String.format("Relationship %s relates table \"%s\" to itself. "
                    + "Change the source or target of that relationship.", relationshipNumber, fromTables[i]));
            }
            // check for duplicate relationships
            Relationship rel = new Relationship("", fromTables[i], fromColumns[i], toTables[i], toColumns[i], "");
            if (rs.containsKey(rel)) {
                throw new InvalidSettingsException(String.format(
                    "Relationship %s defines the same source and target tables and columns as Relationship %s. "
                        + "Remove or change either relationship.",
                    relationshipNumber, rs.get(rel), fromTables[i], fromColumns[i], toTables[i], toColumns[i]));
            }
            rs.put(rel, relationshipNumber);
        }
    }

    /**
     * Checks that the column names in the relationships appear in the corresponding tables. Note: it is not necessary
     * to check that the ports (tables) referenced by a relationship still exist because those will be filtered out
     * during execute anyways.
     *
     * @param inSpecs
     * @throws InvalidSettingsException
     */
    void validateAgainst(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {

        // table names state the user selected names for the tables at port 1, 2, ... (port 0 is for authentication)
        String[] tableNames = getTableNames();
        Map<String, Integer> nameToPort = new HashMap<>();
        for (int i = 0; i < tableNames.length; i++) {
            nameToPort.put(tableNames[i], i + 1);
        }

        // validate only those relationships referring to tables at still existing ports (because only those are sent)
        Relationship[] toValidate = getRelationships(Arrays.copyOf(tableNames, inSpecs.length - 1));

        // make sure every referenced column is still a valid column identifier
        for (int i = 0; i < toValidate.length; i++) {

            Relationship v = toValidate[i];

            int fromPort = nameToPort.get(v.getFromTable());
            DataTableSpec fromTable = (DataTableSpec)inSpecs[fromPort];
            if (!fromTable.containsName(v.getFromColumn())) {
                throw new InvalidSettingsException(String.format(
                    "PowerBI relationship %s references source column %s in table %s (port %s) which does not exist.",
                    i + 1, v.getFromColumn(), v.getFromTable(), fromPort));
            }

            int toPort = nameToPort.get(v.getToTable());
            DataTableSpec toTable = (DataTableSpec)inSpecs[toPort];
            if (!toTable.containsName(v.getToColumn())) {
                throw new InvalidSettingsException(String.format(
                    "PowerBI relationship %s references target column %s in table %s (port %s) which does not exist.",
                    i + 1, v.getToColumn(), v.getToTable(), toPort));
            }
        }

    }

    /**
     * @param filterTableNames relationships referring to table names not in this set are not returned.
     * @return combine the string fields {@link #getRelationshipFromTables()}, {@link #getRelationshipFromColumns()}
     *         etc. into {@link Relationship} objects.
     */
    Relationship[] getRelationships(final String[] filterTableNames) {

        Set<String> fTN = Set.of(filterTableNames);

        final String[] relationshipFromTables = getRelationshipFromTables();
        final String[] relationshipFromColumns = getRelationshipFromColumns();
        final String[] relationshipToTables = getRelationshipToTables();
        final String[] relationshipToColumns = getRelationshipToColumns();
        final String[] relationshipCrossfilterBehaviors = getRelationshipCrossfilterBehaviors();

        List<Relationship> relationships = new ArrayList<>();

        for (int i = 0; i < relationshipFromTables.length; i++) {

            if (!fTN.contains(relationshipFromTables[i]) || !fTN.contains(relationshipToTables[i])) {
                continue;
            }

            // generate a unique identifier for the PowerBI API
            final String name = "rel" + Integer.toString(i);
            relationships.add(new Relationship(name, relationshipFromTables[i], relationshipFromColumns[i],
                relationshipToTables[i], relationshipToColumns[i], relationshipCrossfilterBehaviors[i]));
        }
        return relationships.toArray(Relationship[]::new);
    }
}
