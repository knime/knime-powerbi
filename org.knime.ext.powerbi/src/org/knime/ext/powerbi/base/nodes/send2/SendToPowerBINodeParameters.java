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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.webui.node.dialog.defaultdialog.internal.persistence.ElementFieldPersistor;
import org.knime.core.webui.node.dialog.defaultdialog.internal.persistence.PersistArray;
import org.knime.core.webui.node.dialog.defaultdialog.internal.persistence.PersistArrayElement;
import org.knime.core.webui.node.dialog.defaultdialog.internal.widget.OverwriteDialogTitleInternal;
import org.knime.core.webui.node.dialog.defaultdialog.internal.widget.WidgetInternal;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.util.PowerBICredentialUtil;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.migration.LoadDefaultsForAbsentFields;
import org.knime.node.parameters.persistence.NodeParametersPersistor;
import org.knime.node.parameters.persistence.Persist;
import org.knime.node.parameters.persistence.Persistor;
import org.knime.node.parameters.persistence.legacy.EnumBooleanPersistor;
import org.knime.node.parameters.updates.Effect;
import org.knime.node.parameters.updates.Effect.EffectType;
import org.knime.node.parameters.updates.EffectPredicate;
import org.knime.node.parameters.updates.EffectPredicateProvider;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.ColumnChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.choices.RadioButtonsWidget;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.choices.TypedStringChoice;
import org.knime.node.parameters.widget.choices.ValueSwitchWidget;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.message.TextMessage.MessageType;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation;

/**
 * Node parameters for Send to Power BI.
 *
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
@LoadDefaultsForAbsentFields
final class SendToPowerBINodeParameters implements NodeParameters {

    private static final String DEFAULT_WORKSPACE = "default";

    @TextMessage(WorkspaceInfoMessageProvider.class)
    Void m_workspaceInfo;

    @Widget(title = "Workspace", description = """
            Select the workspace to use. Select "default" to use the default workspace which is called
            "My workspace" in the Power BI user interface.
            <br/>
            <b>NOTE:</b> Authenticate using the <tt>Microsoft Authenticator</tt> node to see available workspaces.
            """)
    @ChoicesProvider(WorkspaceChoicesProvider.class)
    @ValueReference(WorkspaceRef.class)
    @Persistor(WorkspacePersistor.class)
    String m_workspace = DEFAULT_WORKSPACE; // UI value for default workspace (but persisted as empty string)

    @Widget(title = "Dataset mode", description = """
            Choose whether to create a new dataset or select an existing one. When selecting an existing
            dataset, you can append rows to existing tables or overwrite all rows.
            <br/>
            <b>Note:</b> Only "Push datasets" (datasets that support streaming or programmatic data upload) can be
            selected. Regular Power BI datasets are not supported.
            """)
    @ValueSwitchWidget
    @ValueReference(DatasetModeRef.class)
    @Persistor(DataSetModePersistor.class)
    DatasetMode m_datasetMode = DatasetMode.CREATE_NEW;

    @ValueReference(DatasetNameRef.class)
    @Persistor(DatasetNamePersistor.class)
    DatasetName m_datasetName = new DatasetName();

    @Widget(title = "Table name", description = """
            The name of the table(s) in the dataset to upload. Each table must have a unique name.
            """)
    @ArrayWidget(addButtonText = "Add table", hasFixedSize = true, elementTitle = "Table name")
    @ValueReference(TableNamesRef.class)
    @ValueProvider(TableNamesProvider.class)
    @PersistArray(TableNamesArrayPersistor.class)
    TableName[] m_tableNames = {};

    @Widget(title = "Delete and recreate if exists", description = """
            If enabled, deletes a dataset with the same name before creating a new one.
            If disabled and a dataset with the configured name already exists, the node will fail.
            <br/>
            <b>WARNING:</b> Deleting a dataset will permanently remove it along with ALL associated reports and
            dashboard tiles. This action cannot be undone.
            """)
    @Effect(predicate = IsCreateNewDataset.class, type = EffectType.SHOW)
    @Persist(configKey = SendToPowerBINodeSettings2.CFG_KEY_ALLOW_OVERWRITE)
    boolean m_allowOverwrite;

    @Widget(title = "Table operation", description = """
            Select whether to append new rows to existing tables or replace all rows with the input data.
            Overwriting will delete all current data in the selected tables.
            """)
    @RadioButtonsWidget
    @Effect(predicate = IsCreateNewDataset.class, type = EffectType.HIDE)
    @Persistor(TableOperationPersistor.class)
    TableOperation m_tableOperation = TableOperation.APPEND;

    @TextMessage(RelationshipsWarningProvider.class)
    @Effect(predicate = CannotDefineRelationships.class, type = EffectType.SHOW)
    @Advanced
    Void m_relationshipsWarning;

    @Widget(title = "Relationships", description = """
            Define relationships between tables to enable cross-table filtering in Power BI. Each relationship
            connects a column from one table (source) to a column in another table (target). Columns must be in
            different input tables and typically share common values (e.g., foreign keys).
            """, advanced = true)
    @ArrayWidget(addButtonText = "Add relationship",
        elementDefaultValueProvider = RelationshipDefaultValueProvider.class)
    @PersistArray(RelationshipsArrayPersistor.class)
    @Effect(predicate = CannotDefineRelationships.class, type = EffectType.HIDE)
    Relationship[] m_relationships = new Relationship[0];

    enum DatasetMode {
            @Label("Create new dataset")
            CREATE_NEW,

            @Label("Select existing dataset")
            SELECT_EXISTING
    }

    enum TableOperation {
            @Label("Append rows")
            APPEND,

            @Label("Overwrite rows")
            OVERWRITE
    }

    enum CrossFilterBehavior {
            @Label(value = "Automatic", description = "Let Power BI determine the best filtering behavior")
            AUTOMATIC("Automatic"),

            @Label(value = "One direction",
                description = "Filters will be applied to the table where values are aggregated")
            ONE_DIRECTION("OneDirection"),

            @Label(value = "Both directions",
                description = "Filtering is applied as if both tables were joined into a single table")
            BOTH_DIRECTIONS("BothDirections");

        private final String m_serialized;

        CrossFilterBehavior(final String serialized) {
            m_serialized = serialized;
        }

        String getSerialized() {
            return m_serialized;
        }

        static CrossFilterBehavior parseFromSerialized(final String value) {
            return Stream.of(CrossFilterBehavior.values()) //
                .filter(behavior -> behavior.m_serialized.equals(value)) //
                .findFirst() //
                .orElse(AUTOMATIC); // default
        }
    }

    static class DatasetName implements NodeParameters {

        @ValueProvider(DatasetModeValueProvider.class)
        @ValueReference(DatasetModeRefInDatasetName.class)
        DatasetMode m_datasetMode; // derived from top-level, not persisted

        @Widget(title = "Dataset name", description = "The name of the dataset to create in the workspace.")
        @TextInputWidget(patternValidation = PatternValidation.IsNotBlankValidation.class)
        @Effect(predicate = IsCreateNewDatasetInDatasetName.class, type = EffectType.SHOW)
        String m_datasetNameCreate = "";

        @Widget(title = "Dataset name", description = "Select the name of an existing dataset.")
        @ChoicesProvider(DatasetNameChoicesProvider.class)
        @WidgetInternal(hideControlInNodeDescription = "duplicated information from above")
        @Effect(predicate = IsCreateNewDatasetInDatasetName.class, type = EffectType.HIDE)
        String m_datasetNameSelect = "";

        static final class DatasetModeRefInDatasetName implements ParameterReference<DatasetMode> {
        }

        static final class IsCreateNewDatasetInDatasetName implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(DatasetModeRefInDatasetName.class).isOneOf(DatasetMode.CREATE_NEW);
            }
        }
    }

    static final class DatasetNamePersistor implements NodeParametersPersistor<DatasetName> {

        @Override
        public DatasetName load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var dn = new DatasetName();

            final var createNewDataset = settings.getBoolean(SendToPowerBINodeSettings2.CFG_KEY_CREATE_NEW_DATASET);
            dn.m_datasetMode = createNewDataset ? DatasetMode.CREATE_NEW : DatasetMode.SELECT_EXISTING;

            final var datasetName = settings.getString(SendToPowerBINodeSettings2.CFG_KEY_DATASET_NAME);
            final var datasetDialog = settings.getString(SendToPowerBINodeSettings2.CFG_KEY_DATASET_NAME_DIALOG, "");

            dn.m_datasetNameCreate = createNewDataset ? datasetName : datasetDialog;
            dn.m_datasetNameSelect = createNewDataset ? datasetDialog : datasetName;
            return dn;
        }

        @Override
        public void save(final DatasetName obj, final NodeSettingsWO settings) {
            final String datasetName = (obj.m_datasetMode == DatasetMode.SELECT_EXISTING) //
                ? obj.m_datasetNameSelect : obj.m_datasetNameCreate;
            final String datasetNameDialog = (obj.m_datasetMode == DatasetMode.CREATE_NEW) //
                ? obj.m_datasetNameSelect : obj.m_datasetNameCreate;
            settings.addString(SendToPowerBINodeSettings2.CFG_KEY_DATASET_NAME, datasetName);
            settings.addString(SendToPowerBINodeSettings2.CFG_KEY_DATASET_NAME_DIALOG, datasetNameDialog);
        }

        @Override
        public String[][] getConfigPaths() {
            // Expose dataset name key for flow variable support ("_DIALOG" key is of no practical use for the user)
            return new String[][]{{SendToPowerBINodeSettings2.CFG_KEY_DATASET_NAME}};
        }
    }

    static final class WorkspacePersistor implements NodeParametersPersistor<String> {

        @Override
        public String load(final NodeSettingsRO settings) throws InvalidSettingsException {
            final var workspaceId = settings.getString(SendToPowerBINodeSettings2.CFG_KEY_WORKSPACE);
            // Map empty string from settings to "default" for the UI
            return StringUtils.defaultIfEmpty(workspaceId, DEFAULT_WORKSPACE);
        }

        @Override
        public void save(final String obj, final NodeSettingsWO settings) {
            final var workspaceId = DEFAULT_WORKSPACE.equals(obj) ? "" : obj;
            settings.addString(SendToPowerBINodeSettings2.CFG_KEY_WORKSPACE, workspaceId);
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[][]{{SendToPowerBINodeSettings2.CFG_KEY_WORKSPACE}};
        }
    }

    static class TableName implements NodeParameters {

        @ValueProvider(DatasetModeValueProvider.class)
        @ValueReference(DatasetModeRefInTableName.class)
        @PersistArrayElement(DoNotPersistDatasetMode.class)
        DatasetMode m_datasetMode; // For use in modifications

        @Widget(title = "Table name", description = "The name of the table to create in the new dataset.")
        @WidgetInternal(hideControlInNodeDescription = "individual fields don't need to be documented")
        @TextInputWidget(patternValidation = PatternValidation.IsNotBlankValidation.class)
        @OverwriteDialogTitleInternal("")
        @Effect(predicate = IsCreateNewDatasetInTableName.class, type = EffectType.SHOW)
        @PersistArrayElement(TableNameFieldPersistors.TableNameCreatePersistor.class)
        String m_tableNameCreate = "";

        @Widget(title = "Table name",
            description = "Select an existing table from the dataset to append to or overwrite.")
        @WidgetInternal(hideControlInNodeDescription = "individual fields don't need to be documented")
        @OverwriteDialogTitleInternal("")
        @ChoicesProvider(TableNameChoicesProvider.class)
        @Effect(predicate = IsCreateNewDatasetInTableName.class, type = EffectType.HIDE)
        @PersistArrayElement(TableNameFieldPersistors.TableNameSelectPersistor.class)
        String m_tableNameSelect = "";

        String getTableName() {
            return (m_datasetMode == DatasetMode.CREATE_NEW) ? m_tableNameCreate : m_tableNameSelect;
        }

        static final class IsCreateNewDatasetInTableName implements EffectPredicateProvider {
            @Override
            public EffectPredicate init(final PredicateInitializer i) {
                return i.getEnum(DatasetModeRefInTableName.class).isOneOf(DatasetMode.CREATE_NEW);
            }
        }

        static final class DatasetModeRefInTableName implements ParameterReference<DatasetMode> {
        }
    }

    static class Relationship implements NodeParameters {

        @Widget(title = "From table", description = "Select the table that contains the source column.")
        @ChoicesProvider(FromTableChoicesProvider.class)
        @ValueReference(FromTableRef.class)
        @ValueProvider(FromTableValueProvider.class)
        @PersistArrayElement(RelationshipFieldPersistors.FromTablePersistor.class)
        String m_fromTable = "";

        @Widget(title = "From column", description = "Select the column that serves as the source column.")
        @ChoicesProvider(FromColumnChoicesProvider.class)
        @ValueReference(FromColumnRef.class)
        @ValueProvider(FromColumnValueProvider.class)
        @PersistArrayElement(RelationshipFieldPersistors.FromColumnPersistor.class)
        String m_fromColumn = "";

        @Widget(title = "To table", description = "Select the table that contains the target column.")
        @ChoicesProvider(ToTableChoicesProvider.class)
        @ValueReference(ToTableRef.class)
        @ValueProvider(ToTableValueProvider.class)
        @PersistArrayElement(RelationshipFieldPersistors.ToTablePersistor.class)
        String m_toTable = "";

        @Widget(title = "To column", description = "Select the column that serves as the target column.")
        @ChoicesProvider(ToColumnChoicesProvider.class)
        @ValueReference(ToColumnRef.class)
        @ValueProvider(ToColumnValueProvider.class)
        @PersistArrayElement(RelationshipFieldPersistors.ToColumnPersistor.class)
        String m_toColumn = "";

        @Widget(title = "Cross filtering", description = "Control how filters propagate across related tables.")
        @RadioButtonsWidget
        @PersistArrayElement(RelationshipFieldPersistors.CrossFilterBehaviorPersistor.class)
        CrossFilterBehavior m_crossFilterBehavior = CrossFilterBehavior.AUTOMATIC;

        // References for the table selections
        static final class FromTableRef implements ParameterReference<String> {
        }

        static final class ToTableRef implements ParameterReference<String> {
        }

        static final class FromColumnRef implements ParameterReference<String> {
        }

        static final class ToColumnRef implements ParameterReference<String> {
        }

    }

    // Choice providers for relationship table selections
    static final class FromTableChoicesProvider implements StringChoicesProvider {

        private Supplier<TableName[]> m_tableNamesSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            return computeTableChoices(m_tableNamesSupplier.get(), name -> true);
        }
    }

    static final class ToTableChoicesProvider implements StringChoicesProvider {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_fromTableSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_fromTableSupplier = initializer.computeFromValueSupplier(Relationship.FromTableRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            final var fromTable = m_fromTableSupplier.get();
            return computeTableChoices(m_tableNamesSupplier.get(), name -> !name.equals(fromTable));
        }
    }

    /**
     * Helper method to compute table name choices from the table names array.
     *
     * @param tableNames The array of table names to process
     * @param additionalPredicate A predicate to filter table names (e.g., to exclude a specific table)
     * @return A list of StringChoice objects representing the available table names
     */
    private static List<StringChoice> computeTableChoices(final TableName[] tableNames,
        final Predicate<String> additionalPredicate) {

        if (ArrayUtils.isEmpty(tableNames)) {
            return List.of();
        }

        return IntStream.range(0, tableNames.length) //
            .mapToObj(index -> {
                final var name = tableNames[index].getTableName();
                return StringUtils.isNotEmpty(name) && additionalPredicate.test(name) //
                    ? new StringChoice(name, String.format("Table `%s` (Port %d)", name, (index + 1))) //
                    : null;
            }) //
            .filter(Objects::nonNull) //
            .toList();
    }

    // Choice providers for relationship column selections
    static final class FromColumnChoicesProvider implements ColumnChoicesProvider {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_fromTableSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_fromTableSupplier = initializer.computeFromValueSupplier(Relationship.FromTableRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<TypedStringChoice> computeState(final NodeParametersInput context) {
            return getColumnChoicesForTable(context, m_tableNamesSupplier.get(), m_fromTableSupplier.get());
        }
    }

    static final class ToColumnChoicesProvider implements ColumnChoicesProvider {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_toTableSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_toTableSupplier = initializer.computeFromValueSupplier(Relationship.ToTableRef.class);
            initializer.computeBeforeOpenDialog();
        }

        @Override
        public List<TypedStringChoice> computeState(final NodeParametersInput context) {
            return getColumnChoicesForTable(context, m_tableNamesSupplier.get(), m_toTableSupplier.get());
        }
    }

    /**
     * Helper method to get column choices for a given table name. Finds the table index in the tableNames array,
     * calculates the corresponding port index (accounting for credential port at index 0), and returns the columns from
     * that port.
     */
    private static List<TypedStringChoice> getColumnChoicesForTable(final NodeParametersInput context,
        final TableName[] tableNames, final String tableName) {

        // Find the index of the table in the tableNames array
        int tableIndex = IntStream.range(0, ArrayUtils.getLength(tableNames)) //
            .filter(i -> Objects.equals(tableName, tableNames[i].getTableName())) //
            .findFirst() //
            .orElse(-1);

        if (tableIndex < 0) {
            return List.of();
        }

        // accounting for credential port at index 0
        final int portIndex = tableIndex + 1;

        // Get the table spec for the corresponding port
        return context.getInTableSpec(portIndex) //
            .map(spec -> spec.stream() //
                .map(TypedStringChoice::fromColSpec) //
                .toList()) //
            .orElse(List.of());
    }

    /**
     * Helper method to fail the default value computation in a state provider if the current value is already present.
     */
    private static void failComputationIfPresent(final Supplier<String> currentValueSupplier)
        throws StateComputationFailureException {
        if (StringUtils.isNotEmpty(currentValueSupplier.get())) {
            throw new StateComputationFailureException();
        }
    }

    // Value providers for relationship defaults
    static final class FromTableValueProvider implements StateProvider<String> {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_currentFromTableSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            // Get the current (persisted) m_fromTable value to check if it's already set
            m_currentFromTableSupplier = initializer.getValueSupplier(Relationship.FromTableRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput input) throws StateComputationFailureException {
            failComputationIfPresent(m_currentFromTableSupplier);
            return getDefaultRelationship(input, m_tableNamesSupplier.get()).fromTable();
        }
    }

    static final class FromColumnValueProvider implements StateProvider<String> {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_currentFromColumnSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_currentFromColumnSupplier = initializer.getValueSupplier(Relationship.FromColumnRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput input) throws StateComputationFailureException {
            failComputationIfPresent(m_currentFromColumnSupplier);
            return getDefaultRelationship(input, m_tableNamesSupplier.get()).fromColumn();
        }
    }

    static final class ToTableValueProvider implements StateProvider<String> {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_currentToTableSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_currentToTableSupplier = initializer.getValueSupplier(Relationship.ToTableRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput input) throws StateComputationFailureException {
            failComputationIfPresent(m_currentToTableSupplier);
            return getDefaultRelationship(input, m_tableNamesSupplier.get()).toTable();
        }
    }

    static final class ToColumnValueProvider implements StateProvider<String> {

        private Supplier<TableName[]> m_tableNamesSupplier;

        private Supplier<String> m_currentToColumnSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
            m_currentToColumnSupplier = initializer.getValueSupplier(Relationship.ToColumnRef.class);
        }

        @Override
        public String computeState(final NodeParametersInput input) throws StateComputationFailureException {
            failComputationIfPresent(m_currentToColumnSupplier);
            return getDefaultRelationship(input, m_tableNamesSupplier.get()).toColumn();
        }
    }

    static final class DataSetModePersistor extends EnumBooleanPersistor<DatasetMode> {

        DataSetModePersistor() {
            super(SendToPowerBINodeSettings2.CFG_KEY_CREATE_NEW_DATASET, DatasetMode.class, DatasetMode.CREATE_NEW);
        }
    }

    static final class DoNotPersistDatasetMode implements ElementFieldPersistor<DatasetMode, Integer, TableName> {

        @Override
        public DatasetMode load(final NodeSettingsRO settings, final Integer loadContext)
            throws InvalidSettingsException {
            return null;
        }

        @Override
        public void save(final DatasetMode obj, final TableName saveDTO) {
            // not persisted in settings.xml (or similar), but used to determine which create/select field to save as
            // "primary" - the format the settings are stored is questionable (has always been; and not
            // reviewed/refactored as part of the WebUI migration)
            saveDTO.m_datasetMode = obj;
        }

        @Override
        public String[][] getConfigPaths() {
            return new String[0][0];
        }
    }

    static final class DatasetModeRef implements ParameterReference<DatasetMode> {
    }

    static final class DatasetModeValueProvider implements StateProvider<DatasetMode> {

        private Supplier<DatasetMode> m_computeFromValueSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
            m_computeFromValueSupplier = initializer.computeFromValueSupplier(DatasetModeRef.class);
        }

        @Override
        public DatasetMode computeState(final NodeParametersInput input) {
            return m_computeFromValueSupplier.get();
        }
    }

    static final class WorkspaceRef implements ParameterReference<String> {
    }

    static final class DatasetNameRef implements ParameterReference<DatasetName> {
    }

    static final class IsCreateNewDataset implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            return i.getEnum(DatasetModeRef.class).isOneOf(DatasetMode.CREATE_NEW);
        }
    }

    /** Return type of a state provider that provides a message and some data. */
    private record MessageAndData<T>(TextMessage.Message message, T data) {
    }

    /** Intermediate state provider for workspaces. */
    static final class IntermediateWorkspacesStateProvider implements StateProvider<MessageAndData<StringChoice[]>> {

        private static final TextMessage.Message NO_CREDENTIAL_MESSAGE =
            new TextMessage.Message("No connection to Power BI",
                "Cannot connect to Power BI. Ensure the Authenticator node is connected and executed successfully.",
                MessageType.INFO);

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public MessageAndData<StringChoice[]> computeState(final NodeParametersInput context) {
            // Check if we have a credential port connected
            final var inPortSpecs = context.getInPortSpecs();
            if (inPortSpecs.length == 0 || !(inPortSpecs[0] instanceof CredentialPortObjectSpec credSpec)) {
                // No credential port connected - return error message with empty array
                return new MessageAndData<>(NO_CREDENTIAL_MESSAGE, new StringChoice[0]);
            }

            try {
                final var accessor = PowerBICredentialUtil.toAccessTokenAccessor(credSpec);
                final AuthTokenProvider authProvider = accessor::getAccessToken;

                final var groups = PowerBIRestAPIUtils.getGroups(authProvider, null);

                final var workspaceChoices = Stream.of(groups.getValue()) //
                    .map(g -> new StringChoice(g.getId(), g.getName())) //
                    .collect(Collectors.toCollection(ArrayList::new));

                // Add default workspace (id is special-cased in the persistor)
                workspaceChoices.add(new StringChoice(DEFAULT_WORKSPACE, DEFAULT_WORKSPACE));
                return new MessageAndData<>(null, workspaceChoices.toArray(new StringChoice[0]));
            } catch (NoSuchCredentialException e) { // NOSONAR No Credentials - return error message with empty array
                return new MessageAndData<>(NO_CREDENTIAL_MESSAGE, new StringChoice[0]);
            } catch (Exception e) { // NOSONAR -- 3rd party code
                NodeLogger.getLogger(SendToPowerBINodeParameters.class)
                    .debug(String.format("Error loading workspaces: %s", e.getMessage()), e);
                return new MessageAndData<>(new TextMessage.Message("Error retrieving workspaces from Power BI",
                    "Failed to retrieve workspaces from Power BI. Verify your authentication and network connection.\n"
                    + "Details: " + StringUtils.defaultIfBlank(e.getMessage(), e.getClass().getSimpleName()),
                    MessageType.ERROR), new StringChoice[0]);
            }
        }
    }

    static final class WorkspaceChoicesProvider implements StringChoicesProvider {

        private Supplier<MessageAndData<StringChoice[]>> m_workspacesSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_workspacesSupplier = initializer.computeFromProvidedState(IntermediateWorkspacesStateProvider.class);
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            final var messageAndData = m_workspacesSupplier.get();
            // In case of error, empty array is returned by the intermediate state provider
            return Arrays.asList(messageAndData.data());
        }
    }

    static final class TableNameChoicesProvider implements StringChoicesProvider {

        private Supplier<MessageAndData<Dataset[]>> m_intermediateDatasetNamesStateProvider;

        private Supplier<DatasetName> m_datasetNameSupplier;

        private Supplier<String> m_workspaceSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_intermediateDatasetNamesStateProvider =
                initializer.computeFromProvidedState(IntermediateDatasetNamesStateProvider.class);
            m_datasetNameSupplier = initializer.computeFromValueSupplier(DatasetNameRef.class);
            m_workspaceSupplier = initializer.getValueSupplier(WorkspaceRef.class);
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) { // NOSONAR #returns too many
            final Dataset[] datasets = m_intermediateDatasetNamesStateProvider.get().data();
            if (datasets.length == 0) {
                // error occurred when loading datasets - return empty list; error reported elsewhere
                return List.of();
            }

            // safe type cast - errors above will have been caught already
            CredentialPortObjectSpec credSpec = (CredentialPortObjectSpec)context.getInPortSpecs()[0];
            try {

                final DatasetName datasetNameObj = m_datasetNameSupplier.get();
                final var datasetName = datasetNameObj.m_datasetMode == DatasetMode.SELECT_EXISTING
                    ? datasetNameObj.m_datasetNameSelect : datasetNameObj.m_datasetNameCreate;

                // Validate that we have a dataset name
                if (StringUtils.isEmpty(datasetName)) {
                    return List.of();
                }

                // Get the access token provider from the credential spec

                // Find the dataset with the matching name
                final Optional<Dataset> matchingDataset = Stream.of(datasets) //
                    .filter(dataset -> datasetName.equals(dataset.getName())) //
                    .findFirst();

                if (matchingDataset.isEmpty()) {
                    return List.of();
                }

                final var datasetId = matchingDataset.get().getId();
                final var workspaceId = m_workspaceSupplier.get();

                final var accessor = PowerBICredentialUtil.toAccessTokenAccessor(credSpec);
                final AuthTokenProvider authProvider = accessor::getAccessToken;
                final var tables = DEFAULT_WORKSPACE.equals(workspaceId) //
                    ? PowerBIRestAPIUtils.getTables(authProvider, datasetId, null) //
                    : PowerBIRestAPIUtils.getTables(authProvider, workspaceId, datasetId, null);

                // Convert tables to StringChoice objects (table name is both ID and display text)
                return Arrays.stream(tables.getValue()) //
                    .map(table -> new StringChoice(table.getName(), table.getName())) //
                    .toList();
            } catch (NoSuchCredentialException e) { // NOSONAR Credential not available - return placeholder
                return List.of();
            } catch (Exception e) { // NOSONAR -- 3rd party code
                NodeLogger.getLogger(SendToPowerBINodeParameters.class)
                    .debug(String.format("Error loading tables: %s", e.getMessage()), e);
                return List.of();
            }
        }
    }

    static final class TableNamesRef implements ParameterReference<TableName[]> {
    }

    static final class RelationshipDefaultValueProvider implements StateProvider<Relationship> {

        private Supplier<TableName[]> m_tableNamesSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_tableNamesSupplier = initializer.computeFromValueSupplier(TableNamesRef.class);
        }

        @Override
        public Relationship computeState(final NodeParametersInput input) throws StateComputationFailureException {
            final var tableNames = m_tableNamesSupplier.get();
            final var relationship = new Relationship();
            final var relationshipDefault = getDefaultRelationship(input, tableNames);
            relationship.m_toTable = relationshipDefault.toTable();
            relationship.m_toColumn = relationshipDefault.toColumn();
            relationship.m_fromTable = relationshipDefault.fromTable();
            relationship.m_fromColumn = relationshipDefault.fromColumn();
            return relationship;
        }

    }

    /** Helper record to hold default relationship values. */
    static record RelationshipDefault(String fromTable, String fromColumn, String toTable, String toColumn) {
    }

    /** Returns default relationship values based on the first two configured tables (first column per table). */
    static RelationshipDefault getDefaultRelationship(final NodeParametersInput input,
        final TableName[] tableNames) {

        if (ArrayUtils.isEmpty(tableNames) || tableNames.length < 2) {
            return new RelationshipDefault("", "", "", "");
        }

        Function<Optional<DataTableSpec>, String> firstColumnGetter = specOpt ->
            specOpt.flatMap(spec -> spec.stream().findFirst().map(DataColumnSpec::getName)).orElse("");

        // From first table
        final var fromTable = tableNames[0].getTableName();
        final var fromColumn =
            StringUtils.isNotEmpty(fromTable) ? firstColumnGetter.apply(input.getInTableSpec(1)) : "";

        // To second table
        final var toTable = tableNames[1].getTableName();
        final var toColumn = StringUtils.isNotEmpty(toTable) ? firstColumnGetter.apply(input.getInTableSpec(2)) : "";

        return new RelationshipDefault(fromTable, fromColumn, toTable, toColumn);
    }

    static final class TableNamesProvider implements StateProvider<TableName[]> {

        private Supplier<TableName[]> m_currentTableNamesSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
            m_currentTableNamesSupplier = initializer.getValueSupplier(TableNamesRef.class);
        }

        @Override
        public TableName[] computeState(final NodeParametersInput input) {
            final var tableCount = Math.max(0, input.getInPortTypes().length - 1); // Subtract 1 for the auth port

            final var currentTableNames = m_currentTableNamesSupplier.get();

            // If the size matches, return current settings unchanged
            if (currentTableNames.length == tableCount) {
                return currentTableNames;
            }

            // Create new array with correct size
            final var newTableNames = new TableName[tableCount];

            // Copy existing values up to the minimum of current and required size
            final int copyCount = Math.min(currentTableNames.length, tableCount);
            System.arraycopy(currentTableNames, 0, newTableNames, 0, copyCount);

            // Fill in new elements if we expanded the array
            for (int i = copyCount; i < tableCount; i++) {
                newTableNames[i] = new TableName();
            }

            return newTableNames;
        }
    }

    static final class TableOperationPersistor extends EnumBooleanPersistor<TableOperation> {

        TableOperationPersistor() {
            super(SendToPowerBINodeSettings2.CFG_KEY_APPEND_ROWS, TableOperation.class, TableOperation.APPEND);
        }
    }

    static final class RelationshipsWarningProvider implements StateProvider<Optional<TextMessage.Message>> {
        private Supplier<DatasetMode> m_datasetModeRefValueSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
            m_datasetModeRefValueSupplier = initializer.computeFromValueSupplier(DatasetModeRef.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput input) {
            final var tableCount = input.getInPortTypes().length - 1; // Subtract 1 for the credential port
            final DatasetMode datasetMode = m_datasetModeRefValueSupplier.get();

            final StringBuilder msgBuilder = new StringBuilder();
            if (tableCount < 2 && datasetMode != DatasetMode.CREATE_NEW) {
                msgBuilder.append("To define relationships, connect at least two input tables to this node, ")
                    .append("and create a new dataset.");
            } else if (tableCount < 2) {
                msgBuilder.append("To define relationships, connect at least two input tables to this node.");
            } else if (datasetMode != DatasetMode.CREATE_NEW) {
                msgBuilder.append(
                    "To define relationships, create a new dataset instead of selecting an existing one.");
            } else {
                return Optional.empty();
            }
            msgBuilder.append(" Each table must contain at least one column with a data type supported by Power BI.");

            return Optional.of(new TextMessage.Message("Relationships", msgBuilder.toString(), MessageType.INFO));
        }
    }

    static final class CannotDefineRelationships implements EffectPredicateProvider {
        @Override
        public EffectPredicate init(final PredicateInitializer i) {
            // Show warning when we cannot define relationships
            return i.getEnum(DatasetModeRef.class).isOneOf(DatasetMode.SELECT_EXISTING)
                .or(i.getConstant(input -> input.getInPortTypes().length - 1 < 2)); // Subtract 1 for credential port
        }
    }

    static final class IntermediateDatasetNamesStateProvider implements StateProvider<MessageAndData<Dataset[]>> {

        private Supplier<MessageAndData<StringChoice[]>> m_workspacesStateProvider;

        private Supplier<String> m_workspaceSupplier;

        private Supplier<DatasetMode> m_datasetModeRefValueSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_workspacesStateProvider = initializer.computeFromProvidedState(IntermediateWorkspacesStateProvider.class);
            m_workspaceSupplier = initializer.computeFromValueSupplier(WorkspaceRef.class);
            m_datasetModeRefValueSupplier = initializer.computeFromValueSupplier(DatasetModeRef.class);
        }

        @Override
        public MessageAndData<Dataset[]> computeState(final NodeParametersInput context) { // NOSONAR #returns too many
            // error loading workspaces (reported separately)
            if (m_workspacesStateProvider.get().data().length == 0) {
                return new MessageAndData<>(null, new Dataset[0]);
            }
            if (m_datasetModeRefValueSupplier.get() == DatasetMode.CREATE_NEW) {
                // Creating new dataset - no dataset names to load
                return new MessageAndData<>(null, new Dataset[0]);
            }
            final var inPortSpecs = context.getInPortSpecs();
            if (inPortSpecs.length == 0 || !(inPortSpecs[0] instanceof CredentialPortObjectSpec credSpec)) {
                return new MessageAndData<>(null, new Dataset[0]);
            }

            try {
                final var accessor = PowerBICredentialUtil.toAccessTokenAccessor(credSpec);
                final AuthTokenProvider authProvider = accessor::getAccessToken;
                final var workspaceId = m_workspaceSupplier.get();

                final Datasets datasets = DEFAULT_WORKSPACE.equals(workspaceId) //
                    ? PowerBIRestAPIUtils.getDatasets(authProvider, null) //
                    : PowerBIRestAPIUtils.getDatasets(authProvider, workspaceId, null);

                final Dataset[] dsArray = Arrays.stream(datasets.getValue()) //
                    .filter(Dataset::isAddRowsAPIEnabled) //
                    .toArray(Dataset[]::new);
                if (dsArray.length == 0) {
                    return new MessageAndData<>(
                        new TextMessage.Message("No push datasets available",
                            "No push datasets found in the selected workspace. Create a push dataset in Power BI or "
                                    + "select a different workspace.", MessageType.INFO),
                        new Dataset[0]);
                }
                return new MessageAndData<>(null, dsArray);
            } catch (NoSuchCredentialException e) { // NOSONAR No Credentials
                return new MessageAndData<>(new TextMessage.Message("Authentication failed",
                    "Authenticate using the Authenticator node to select a dataset.",
                    MessageType.ERROR), new Dataset[0]);
            } catch (Exception e) { // NOSONAR -- 3rd party code
                NodeLogger.getLogger(SendToPowerBINodeParameters.class).debug("Error loading datasets", e);
                return new MessageAndData<>(new TextMessage.Message("Error loading datasets",
                    "Failed to load datasets. Error: %s".formatted(e.getMessage()), MessageType.ERROR),
                    new Dataset[0]);
            }
        }
    }

    static final class DatasetNameChoicesProvider implements StringChoicesProvider {

        private Supplier<MessageAndData<Dataset[]>> m_intermediateDatasetsProvider;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_intermediateDatasetsProvider =
                initializer.computeFromProvidedState(IntermediateDatasetNamesStateProvider.class);
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            // error loading workspaces reported separately via m_workspaceInfo
            return Stream.of(m_intermediateDatasetsProvider.get().data()) //
                .map(s -> new StringChoice(s.getName(), s.getName())) //
                .toList();
        }
    }

    static final class WorkspaceInfoMessageProvider implements StateProvider<Optional<TextMessage.Message>> {

        private Supplier<MessageAndData<StringChoice[]>> m_workspacesMessageAndDataSupplier;

        private Supplier<MessageAndData<Dataset[]>> m_datasetsMessageAndDataSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_workspacesMessageAndDataSupplier =
                initializer.computeFromProvidedState(IntermediateWorkspacesStateProvider.class);
            m_datasetsMessageAndDataSupplier =
                initializer.computeFromProvidedState(IntermediateDatasetNamesStateProvider.class);
        }

        @Override
        public Optional<TextMessage.Message> computeState(final NodeParametersInput input) {
            final MessageAndData<StringChoice[]> workspaceMessageAndData = m_workspacesMessageAndDataSupplier.get();
            if (workspaceMessageAndData.message() != null) {
                return Optional.of(workspaceMessageAndData.message());
            }

            MessageAndData<Dataset[]> datasetsMessageAndData = m_datasetsMessageAndDataSupplier.get();
            if (datasetsMessageAndData.message() != null) {
                return Optional.of(datasetsMessageAndData.message());
            }
            return Optional.empty();

        }
    }

}
