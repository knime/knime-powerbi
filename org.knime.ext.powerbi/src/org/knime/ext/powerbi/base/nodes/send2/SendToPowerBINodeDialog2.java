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
package org.knime.ext.powerbi.base.nodes.send2;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.border.Border;
import javax.swing.border.TitledBorder;

import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.ColumnComboBoxRenderer;
import org.knime.core.node.util.ColumnFilter;
import org.knime.core.node.util.ColumnSelectionComboxBox;
import org.knime.core.node.util.SharedIcons;
import org.knime.core.util.SwingWorkerWithContext;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredentialPortObjectSpec;
import org.knime.ext.microsoft.authentication.port.oauth2.OAuth2Credential;
import org.knime.ext.powerbi.core.PowerBIDataTypeUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.ext.powerbi.core.rest.bindings.Dataset;
import org.knime.ext.powerbi.core.rest.bindings.Datasets;
import org.knime.ext.powerbi.core.rest.bindings.Groups;
import org.knime.ext.powerbi.core.rest.bindings.Table;
import org.knime.ext.powerbi.core.rest.bindings.Tables;

/**
 * Dialog for the Send to Power BI node.
 *
 * @author Benjamin Wilhem, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeDialog2 extends NodeDialogPane {

    static final String DATASET_PLACEHOLDER = "-- Authenticate to select dataset --";

    static final String TABLE_PLACEHOLDER = "-- Authenticate to select table --";

    static final String WORKSPACE_PLACEHOLDER = "-- Authenticate to select workspace --";

    private static final PowerBIElement DEFAULT_WORKSPACE = createPowerBIWorkspace("default", "", false);

    private static final PowerBIElement NO_PUSH_DATASETS_PLACEHOLDER =
        new PowerBIElement("", "no_push_datasets", true, "-- No push datasets available --");

    private static final PowerBIElement NO_TABLES_PLACEHOLDER =
        new PowerBIElement("", true, "-- No tables available --");

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeDialog2.class);

    private final SendToPowerBINodeSettings2 m_settings;

    private final int m_numberInputs;

    private final AtomicBoolean m_updatingWorkspaceOptions;

    private final AtomicBoolean m_updatingDatasetOptions;

    private JLabel m_authWarning;

    private JComboBox<PowerBIElement> m_workspace;

    private JRadioButton m_createNewButton;

    private JRadioButton m_selectExisting;

    private JCheckBox m_allowOverwrite;

    private JTextField m_datasetNameCreate;

    private JTextField[] m_tableNamesCreate;

    private JComboBox<PowerBIElement> m_datasetNameSelect;

    private JComboBox<PowerBIElement>[] m_tableNamesSelect;

    private JRadioButton m_appendToExisting;

    private JRadioButton m_refreshExisting;

    private JLabel m_workspaceInfo;

    private final RelationshipsTab m_relationshipsTab = new RelationshipsTab();

    private AuthTokenProvider m_authProvider;

    private boolean m_authenticated;

    public SendToPowerBINodeDialog2(final int numberInputs) {
        m_numberInputs = numberInputs;
        m_settings = new SendToPowerBINodeSettings2();
        m_updatingWorkspaceOptions = new AtomicBoolean(false);
        m_updatingDatasetOptions = new AtomicBoolean(false);
        addTab("Options", createDatasetTablePanel());
        // after creation & before loading no port specs are available
        m_relationshipsTab.disableEdit(RelationshipsTab.NO_PROPER_TABLES_CONNECTED);
        addTab("Relationships", m_relationshipsTab);
    }

    private JPanel createDatasetTablePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Authentication warning if the user is not authenticated
        gbc.gridwidth = 2;
        m_authWarning = new JLabel("", SharedIcons.ERROR.get(), SwingConstants.LEFT);
        m_authWarning.setForeground(Color.RED);
        m_authWarning.setVisible(false);
        panel.add(m_authWarning, gbc);
        gbc.gridwidth = 1;
        gbc.gridy++;

        // Workspace
        panel.add(new JLabel("Workspace"), gbc);
        gbc.gridx++;
        gbc.weightx = 5;
        m_workspace = new JComboBox<>(new PowerBIElement[]{DEFAULT_WORKSPACE});
        m_workspace.setEnabled(false);
        m_workspace.addActionListener(e -> updateDatasetOptions());
        panel.add(m_workspace, gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        // Create new dataset
        m_createNewButton = new JRadioButton("Create new Dataset");
        panel.add(m_createNewButton, gbc);
        m_createNewButton.setSelected(true);

        gbc.gridy++;
        gbc.gridx = 1;
        gbc.weightx = 1;
        panel.add(createCreateNewDatasetPanel(), gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        // Append to existing dataset
        m_selectExisting = new JRadioButton("Select existing Dataset");
        panel.add(m_selectExisting, gbc);

        gbc.gridy++;
        gbc.gridx = 1;
        gbc.weightx = 1;
        panel.add(createSelectDatasetPanel(), gbc);

        // Button group
        final ButtonGroup bg = new ButtonGroup();
        bg.add(m_createNewButton);
        bg.add(m_selectExisting);

        // Action listener for create new/append
        m_createNewButton.addActionListener(e -> enableDisableComboboxes());
        m_selectExisting.addActionListener(e -> enableDisableComboboxes());
        ActionListener enableRelationships = e -> m_relationshipsTab.checkEditable();
        m_createNewButton.addActionListener(enableRelationships);
        m_selectExisting.addActionListener(enableRelationships);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 2;

        // Nopush datasets info not visible by default
        m_workspaceInfo = new JLabel("", SharedIcons.INFO.get(), SwingConstants.LEFT);

        m_workspaceInfo.setVisible(false);
        panel.add(m_workspaceInfo, gbc);

        return panel;
    }

    /** Create a new dataset panel */
    private JPanel createCreateNewDatasetPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Dataset name
        panel.add(new JLabel("Dataset name"), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        m_datasetNameCreate = new JTextField("");
        panel.add(m_datasetNameCreate, gbc);

        m_tableNamesCreate = new JTextField[m_numberInputs];
        for (int i = 0; i < m_numberInputs; i++) {
            gbc.gridy++;
            gbc.gridx = 0;
            gbc.weightx = 1;
            // Table names
            final String label = m_numberInputs == 1 ? "Table name" : ("Table name " + (i + 1));
            panel.add(new JLabel(label), gbc);
            gbc.gridx++;
            gbc.weightx = 3;
            m_tableNamesCreate[i] = new JTextField("");
            m_tableNamesCreate[i].addFocusListener(m_relationshipsTab.m_updateTableNameListener);
            panel.add(m_tableNamesCreate[i], gbc);
        }

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        // Allow overwrite
        m_allowOverwrite = new JCheckBox("Delete and create new if dataset exists");
        panel.add(m_allowOverwrite, gbc);

        return panel;
    }

    /** Select an existing dataset panel */
    private JPanel createSelectDatasetPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Dataset name
        panel.add(new JLabel("Dataset name"), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        m_datasetNameSelect = new JComboBox<>();
        m_datasetNameSelect.addActionListener(e -> updateTableOptions());

        panel.add(m_datasetNameSelect, gbc);

        @SuppressWarnings("unchecked")
        final JComboBox<PowerBIElement>[] tableNamesAppendLocal = new JComboBox[m_numberInputs];
        m_tableNamesSelect = tableNamesAppendLocal;
        for (int i = 0; i < m_numberInputs; i++) {
            gbc.gridy++;
            gbc.gridx = 0;
            gbc.weightx = 1;
            // Table name
            final String label = m_numberInputs == 1 ? "Table name" : ("Table name " + (i + 1));
            panel.add(new JLabel(label), gbc);
            gbc.gridx++;
            gbc.weightx = 3;
            m_tableNamesSelect[i] = new JComboBox<>();
            panel.add(m_tableNamesSelect[i], gbc);
        }

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        // Append or refresh
        panel.add(createAppendRefreshPanel(), gbc);

        return panel;
    }

    /** Append or refresh an existing table panel */
    private JPanel createAppendRefreshPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.LINE_AXIS));

        // Append to existing table
        m_appendToExisting = new JRadioButton("Append rows");
        panel.add(m_appendToExisting);

        // Refresh existing table
        m_refreshExisting = new JRadioButton("Overwrite rows");
        panel.add(m_refreshExisting);

        // Button group for radio buttons
        final ButtonGroup bg = new ButtonGroup();
        bg.add(m_appendToExisting);
        bg.add(m_refreshExisting);

        return panel;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // Workspace
        m_settings.setWorkspace(((PowerBIElement)m_workspace.getSelectedItem()).getIdentifier());

        // Create new or append and overwrite
        final boolean createNew = m_createNewButton.isSelected();
        m_settings.setCreateNewDataset(createNew);
        m_settings.setAllowOverwrite(m_allowOverwrite.isSelected());
        m_settings.setAppendRows(m_appendToExisting.isSelected());

        // Dataset and table names
        final String datasetNameCreate = m_datasetNameCreate.getText();
        final String[] tableNamesCreate =
            Arrays.stream(m_tableNamesCreate).map(JTextField::getText).toArray(String[]::new);

        final PowerBIElement dataset = (PowerBIElement)m_datasetNameSelect.getSelectedItem();
        final String datasetNameSelect = dataset == null ? "" : dataset.getName();

        final String[] tableNamesSelect = Arrays.stream(m_tableNamesSelect)
            .map(c -> (PowerBIElement)c.getSelectedItem()).map(x -> x == null ? createPowerBITable("", false) : x)
            .map(PowerBIElement::getName).toArray(String[]::new);

        if (createNew) {
            m_settings.setDatasetName(datasetNameCreate);
            m_settings.setTableNames(tableNamesCreate);
            m_settings.setDatasetNameDialog(datasetNameSelect);
            m_settings.setTableNamesDialog(tableNamesSelect);
            if (m_relationshipsTab.checkEditable()) {
                m_settings.setRelationshipFromTables(m_relationshipsTab.getRelationshipFromTables());
                m_settings.setRelationshipFromColumns(m_relationshipsTab.getRelationshipFromColumns());
                m_settings.setRelationshipToTables(m_relationshipsTab.getRelationshipToTables());
                m_settings.setRelationshipToColumns(m_relationshipsTab.getRelationshipToColumns());
                m_settings
                    .setRelationshipCrossfilterBehaviors(m_relationshipsTab.getRelationshipCrossfilterBehaviors());
            }
        } else {
            if (dataset == null) {
                throw new InvalidSettingsException("Please select a dataset.");
            }
            m_settings.setDatasetName(datasetNameSelect);
            m_settings.setTableNames(tableNamesSelect);
            m_settings.setDatasetNameDialog(datasetNameCreate);
            m_settings.setTableNamesDialog(tableNamesCreate);
        }

        try {
            m_settings.saveSettingsTo(settings);
        } catch (IOException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        tryAuthenticate(specs[0]);

        // Do not block dialog if problem occurs
        try {
            m_settings.loadValidatedSettingsFrom(settings);
        } catch (InvalidSettingsException | IOException ex) {
            LOGGER.warn("Can't load settings. Reason: " + ex.getMessage(), ex);
        }

        // Workspace
        final String workspaceId = m_settings.getWorkspace();
        if (workspaceId.isEmpty()) {
            m_workspace.setSelectedItem(DEFAULT_WORKSPACE);
        } else {
            final PowerBIElement selectedWorkspace = createPowerBIWorkspace(workspaceId, workspaceId, true);
            m_workspace.addItem(selectedWorkspace);
            m_workspace.setSelectedItem(selectedWorkspace);
        }

        // Create or select
        final boolean createNewDataset = m_settings.isCreateNewDataset();
        m_createNewButton.setSelected(createNewDataset);
        m_selectExisting.setSelected(!createNewDataset);

        final String datasetNameCreate;
        final String datasetNameSelect;
        final String[] tableNamesCreate;
        final String[] tableNamesSelect;
        if (createNewDataset) {
            datasetNameCreate = m_settings.getDatasetName();
            datasetNameSelect = m_settings.getDatasetNameDialog();
            tableNamesCreate = m_settings.getTableNames();
            tableNamesSelect = m_settings.getTableNamesDialog();
        } else {
            datasetNameCreate = m_settings.getDatasetNameDialog();
            datasetNameSelect = m_settings.getDatasetName();
            tableNamesCreate = m_settings.getTableNamesDialog();
            tableNamesSelect = m_settings.getTableNames();
        }

        m_datasetNameCreate.setText(datasetNameCreate);
        for (int i = 0; i < m_numberInputs && i < tableNamesCreate.length; i++) {
            m_tableNamesCreate[i].setText(tableNamesCreate[i]);
        }

        final PowerBIElement dataset = createPowerBIDataset(datasetNameSelect, null, true);
        m_datasetNameSelect.addItem(dataset);
        m_datasetNameSelect.setSelectedItem(dataset);

        for (int i = 0; i < m_numberInputs && i < tableNamesSelect.length; i++) {
            final PowerBIElement powerBITable;
            if (!tableNamesSelect[0].equals("")) {
                powerBITable = createPowerBITable(tableNamesSelect[i], true);

            } else {
                powerBITable = createPowerBITable(tableNamesSelect[i], !m_authenticated);
            }
            m_tableNamesSelect[i].addItem(powerBITable);
            m_tableNamesSelect[i].setSelectedItem(powerBITable);
        }

        // Allow overwrite
        m_allowOverwrite.setSelected(m_settings.isAllowOverwrite());

        // Append or refresh
        final boolean appendRows = m_settings.isAppendRows();
        m_appendToExisting.setSelected(appendRows);
        m_refreshExisting.setSelected(!appendRows);

        // Update which components are enabled
        enableDisableComboboxes();
        if (m_authenticated) {
            updateWorkspaceOptions();
        }

        // reflect the loaded settings in the UI
        m_relationshipsTab.loadSettingsFrom(specs);
    }

    /**
     * Try to get the authentication token from the given port. Set {@link SendToPowerBINodeDialog2#m_authenticated} to
     * true on success, display a warning in the dialog otherwise.
     *
     * @param authPortSpec port spec of the port that is connected to node that provides the authentication, e.g.,
     *            Microsoft Authentication node
     */
    private void tryAuthenticate(final PortObjectSpec authPortSpec) {
        m_authenticated = false;
        String authWarningText = "Not authenticated. Please login in the 'Microsoft Authentication' node.";

        // if no authentication node is connected, the spec at port 0 will be null
        if (authPortSpec != null) {
            try {
                // Get the credentials
                m_authProvider = getAuthTokenProvider(authPortSpec);
                m_authProvider.getToken();
                m_authenticated = true;
            } catch (final IOException e) {
                authWarningText = "Could not get the token from the authentication. "
                    + "Please re-authenticate in the 'Microsoft Authentication' node.";
                LOGGER.warn(e);
            } catch (final NotConfigurableException e) {
                authWarningText = e.getMessage();
                LOGGER.debug(e);
            }
        }

        // show warning if not able to authenticate
        m_authWarning.setText(authWarningText);
        m_authWarning.setVisible(!m_authenticated);

    }

    private static AuthTokenProvider getAuthTokenProvider(final PortObjectSpec spec) throws NotConfigurableException {
        final MicrosoftCredential credentials = ((MicrosoftCredentialPortObjectSpec)spec).getMicrosoftCredential();
        final Optional<String> checkResult = SendToPowerBINodeModel2.checkCredentials(credentials);
        if (checkResult.isPresent()) {
            throw new NotConfigurableException(checkResult.get());
        }
        return new TokenSupplier((OAuth2Credential)credentials);
    }

    /* -------------------------------------- Handling changes ---------------------------- */

    /** Creates a PowerBI dataset element */
    private static PowerBIElement createPowerBIDataset(final String name, final String identifier,
        final boolean showPlaceholder) {
        return new PowerBIElement(name, identifier, showPlaceholder, DATASET_PLACEHOLDER);
    }

    /** Creates a PowerBI table element */
    private static PowerBIElement createPowerBITable(final String name, final boolean showPlaceholder) {
        return new PowerBIElement(name, showPlaceholder, TABLE_PLACEHOLDER);
    }

    /** Creates a PowerBI table element */
    private static PowerBIElement createPowerBIWorkspace(final String name, final String identifier,
        final boolean showPlaceholder) {
        return new PowerBIElement(name, identifier, showPlaceholder, WORKSPACE_PLACEHOLDER);
    }

    /** Enables or diables the create new fields and disables or enables the append fields */
    private void enableDisableComboboxes() {
        final PowerBIElement selectedItem = (PowerBIElement)m_datasetNameSelect.getSelectedItem();
        final boolean pushDatasetsAvailable = selectedItem != NO_PUSH_DATASETS_PLACEHOLDER;
        final boolean enableNewDataset = m_createNewButton.isSelected();
        final boolean enableDatasetSelect = !enableNewDataset && m_authenticated && pushDatasetsAvailable;

        // Workspace selection
        m_workspace.setEnabled(m_authenticated);

        // Create new dataset
        m_datasetNameCreate.setEnabled(enableNewDataset);
        for (final JTextField tableNameCreate : m_tableNamesCreate) {
            tableNameCreate.setEnabled(enableNewDataset);
        }
        m_allowOverwrite.setEnabled(enableNewDataset);

        // Append to existing dataset
        m_datasetNameSelect.setEnabled(enableDatasetSelect);
        for (final JComboBox<PowerBIElement> tableNameAppend : m_tableNamesSelect) {
            tableNameAppend.setEnabled(enableDatasetSelect);
        }
        m_appendToExisting.setEnabled(!enableNewDataset);
        m_refreshExisting.setEnabled(!enableNewDataset);
    }

    /** Start a thread to update the workspace options */
    private void updateWorkspaceOptions() {
        final DefaultSwingWorker<List<PowerBIElement>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableWorkspaces(m_authProvider), //
            this::setWorkspaceOptions, //
            "Updating the available datasets failed.");
        worker.execute();
    }

    /** Start a thread to update the dataset options */
    private void updateDatasetOptions() {
        if (m_updatingWorkspaceOptions.get() || !m_authenticated) {
            // Either updating the workspaces or not authenticated
            return;
        }
        final PowerBIElement workspace = (PowerBIElement)m_workspace.getSelectedItem();
        if (workspace == null) {
            // No workspace selected
            return;
        }
        final String workspaceId = workspace.getIdentifier();
        final DefaultSwingWorker<List<PowerBIElement>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableDatasets(m_authProvider, workspaceId), //
            this::setDatasetOptions, //
            "Updating the available datasets failed.");

        worker.execute();
    }

    /** Start a thread to update the table options */
    private void updateTableOptions() {
        if (m_updatingDatasetOptions.get() || !m_authenticated) {
            // Either updating the datasets or not authenticated
            return;
        }
        final PowerBIElement workspace = (PowerBIElement)m_workspace.getSelectedItem();
        final PowerBIElement dataset = (PowerBIElement)m_datasetNameSelect.getSelectedItem();

        if (workspace == null || dataset == null || dataset.getIdentifier() == null) {
            // No dataset selected (or loaded from the settings)
            return;
        }

        final String workspaceId = workspace.getIdentifier();
        final String datasetId = dataset.getIdentifier();
        final DefaultSwingWorker<List<PowerBIElement>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableTables(m_authProvider, workspaceId, datasetId), //
            this::setTableOptions, //
            "Updating the available tables failed.");
        worker.execute();
    }

    /** Set the workspace options to the given argument (and reselect or select the default) */
    private void setWorkspaceOptions(final List<PowerBIElement> workspaceNames) {
        // Get the selected value (or default)
        PowerBIElement selected = (PowerBIElement)m_workspace.getSelectedItem();
        final String identifier = selected.getIdentifier();
        final Optional<PowerBIElement> workspace =
            workspaceNames.stream().filter(id -> id.getIdentifier().equals(identifier)).findFirst();
        if (!workspace.isPresent()) {
            selected = DEFAULT_WORKSPACE;
        } else {
            selected = workspace.get();
        }

        // Update the options and reselect
        m_updatingWorkspaceOptions.set(true);
        m_workspace.removeAllItems();
        for (final PowerBIElement w : workspaceNames) {
            m_workspace.addItem(w);
        }
        m_updatingWorkspaceOptions.set(false);
        m_workspace.setSelectedItem(selected);
        m_workspace.setEnabled(true);
    }

    /** Set the dataset options to the given argument (and reselect or select the default) */
    private void setDatasetOptions(final List<PowerBIElement> datasetNames) {
        // Get the selected value
        final PowerBIElement selected = (PowerBIElement)m_datasetNameSelect.getSelectedItem();

        // Update the options and reselect
        m_updatingDatasetOptions.set(true);
        m_datasetNameSelect.removeAllItems();
        for (final PowerBIElement d : datasetNames) {
            m_datasetNameSelect.addItem(d);
        }
        m_updatingDatasetOptions.set(false);
        // Reselect
        if (datasetNames.contains(selected)) {
            m_datasetNameSelect.setSelectedItem(selected);
        } else if (datasetNames.isEmpty()) {
            // If there are no datasets: Reset the table dropdown
            setTableOptions(Collections.emptyList());
        } else {
            m_datasetNameSelect.setSelectedIndex(0);
        }
    }

    /** Set the table options to the given argument (and reselect or select the default) */
    private void setTableOptions(final List<PowerBIElement> tableNames) {
        for (int i = 0; i < m_numberInputs; i++) {
            final JComboBox<PowerBIElement> tableNameAppend = m_tableNamesSelect[i];
            // Get the selected values
            final PowerBIElement selected = (PowerBIElement)tableNameAppend.getSelectedItem();

            // Update the options and reselect
            tableNameAppend.removeAllItems();
            for (final PowerBIElement t : tableNames) {
                tableNameAppend.addItem(t);
            }
            // Reselect
            if (selected != null) {
                if (tableNames.contains(selected)) {
                    tableNameAppend.setSelectedItem(selected);
                } else if (!tableNames.isEmpty()) {
                    tableNameAppend.setSelectedIndex(i % tableNames.size());
                }
            }
        }
        //This is added in case there are no push datasets available to disable the dataset / table dropdowns
        enableDisableComboboxes();
    }

    /* ------------------------------------------------ REST API helpers ------------------------ */

    /** Call the REST API to get the available workspaces */
    private static List<PowerBIElement> getAvailableWorkspaces(final AuthTokenProvider auth)
        throws PowerBIResponseException {
        final Groups groups = PowerBIRestAPIUtils.getGroups(auth);
        final List<PowerBIElement> workspaces = Arrays.stream(groups.getValue()) //
            .map(g -> createPowerBIWorkspace(g.getName(), g.getId(), false)) //
            .collect(Collectors.toCollection(ArrayList::new));
        workspaces.add(DEFAULT_WORKSPACE);
        return workspaces;
    }

    /** Call the REST API to get the available datasets */
    private List<PowerBIElement> getAvailableDatasets(final AuthTokenProvider auth, final String workspaceId)
        throws PowerBIResponseException {
        final Datasets datasets;
        if (workspaceId.isEmpty()) {
            datasets = PowerBIRestAPIUtils.getDatasets(auth);
        } else {
            datasets = PowerBIRestAPIUtils.getDatasets(auth, workspaceId);
        }

        List<PowerBIElement> powerBIDatasets = Arrays.stream(datasets.getValue()).filter(Dataset::isAddRowsAPIEnabled)//
            .map(d -> createPowerBIDataset(d.getName(), d.getId(), false)) //
            .collect(Collectors.toList());

        showWorkspaceInfo(datasets.getValue().length, powerBIDatasets.size());

        return powerBIDatasets.isEmpty() ? Collections.singletonList(NO_PUSH_DATASETS_PLACEHOLDER) : powerBIDatasets;
    }

    /** Enables the workspaceInfo JLabel depending if no datasets available or non push datasets */
    private void showWorkspaceInfo(final int datasetsSize, final int powerBIDatasetsSize) {
        //Only enable if push datasets and non push datasets available or if workspace is empty
        final boolean workspaceIsEmpty = datasetsSize == 0;
        final boolean containsNonPushDataSets = datasetsSize > powerBIDatasetsSize;
        m_workspaceInfo.setVisible(containsNonPushDataSets || workspaceIsEmpty);

        if (workspaceIsEmpty) {
            m_workspaceInfo.setText("Selected workspace does not contain any datasets.");
        } else if (containsNonPushDataSets) {
            m_workspaceInfo.setText("Only push datasets are being displayed.");
        }
    }

    private static List<PowerBIElement> getAvailableTables(final AuthTokenProvider auth, final String workspaceId,
        final String datasetId) throws PowerBIResponseException {
        final Tables tables;

        if (datasetId.equals(NO_PUSH_DATASETS_PLACEHOLDER.getIdentifier())) {
            return Collections.singletonList(NO_TABLES_PLACEHOLDER);
        }
        if (workspaceId.isEmpty()) {
            tables = PowerBIRestAPIUtils.getTables(auth, datasetId);
        } else {
            tables = PowerBIRestAPIUtils.getTables(auth, workspaceId, datasetId);
        }

        return Arrays.stream(tables.getValue()) //
            .map(Table::getName).map(x -> createPowerBITable(x, false)) //
            .collect(Collectors.toList());
    }

    /** Create default GridBagConstraints */
    private static GridBagConstraints createGBC() {
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.insets = new Insets(5, 5, 5, 5);
        return gbc;
    }

    /** A Power BI Element to use in the combobox for the dataset, workspace and the tables (identified by the name) */
    private static final class PowerBIElement {

        private final String m_name;

        private final String m_identifier;

        private final String m_placeHolder;

        private boolean m_showPlaceholder;

        public PowerBIElement(final String name, final String identifier, final boolean showPlaceholder,
            final String placeHolder) {
            m_name = name;
            m_identifier = identifier;
            m_showPlaceholder = showPlaceholder;
            m_placeHolder = placeHolder;
        }

        public PowerBIElement(final String name, final boolean showPlaceholder, final String placeHolder) {
            this(name, null, showPlaceholder, placeHolder);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof PowerBIElement)) {
                return false;
            }
            final PowerBIElement o = (PowerBIElement)obj;
            return Objects.equals(m_name, o.m_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_name);
        }

        @Override
        public String toString() {
            return m_showPlaceholder ? m_placeHolder : m_name;
        }

        public String getName() {
            return m_name;
        }

        public String getIdentifier() {
            return m_identifier;
        }
    }

    /** Default swing worker which uses a supplier and consumer */
    private static final class DefaultSwingWorker<T> extends SwingWorkerWithContext<T, Void> {

        private final ThrowingSupplier<T> m_backgroundJob;

        private final Consumer<T> m_doneJob;

        private final String m_exceptionText;

        private DefaultSwingWorker(final ThrowingSupplier<T> backgroundJob, final Consumer<T> doneJob,
            final String exceptionText) {
            m_backgroundJob = backgroundJob;
            m_doneJob = doneJob;
            m_exceptionText = exceptionText;
        }

        @Override
        protected T doInBackgroundWithContext() throws Exception {
            return m_backgroundJob.get();
        }

        @Override
        protected void doneWithContext() {
            try {
                m_doneJob.accept(get());
            } catch (final InterruptedException | ExecutionException e) {
                LOGGER.warn(m_exceptionText, e);
            }
        }
    }

    @FunctionalInterface
    private static interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    /**
     * Dialog components and behavior for the tab in which table relationships can be entered/viewed.
     * {@link RelationshipPanel}s can be dynamically added using {@link #m_addButton} and removed using
     * {@link RelationshipPanel#m_remove} button. Control values are restored from the node settings via
     * {@link #loadSettingsFrom(PortObjectSpec[])}.
     */
    @SuppressWarnings("serial")
    private final class RelationshipsTab extends JPanel {

        /** Identifier for the panel that is shown when relationships are editable */
        private static final String CARD_LAYOUT_EDITABLE = "editable";

        /** Identifier for the panel that is shown when relationships are not */
        private static final String CARD_LAYOUT_UNAVAILABLE = "unavailable";

        private static final String CANT_DEFINE_ON_EXISTING =
            "Relationships can only be defined when creating a new data set.";

        /** When no tables are connected to the input ports, the column select combo boxes can not be populated. */
        private static final String NO_PROPER_TABLES_CONNECTED =
            "To define a relationship, connect at least two tables.\nEach table needs at most one column"
                + " that has a type supported by Power BI.";

        private final JTextArea m_errorLabel = new JTextArea();

        private final List<RelationshipPanel> m_relationshipPanels = new ArrayList<>();

        final transient FocusListener m_updateTableNameListener = new FocusListener() {

            @Override
            public void focusLost(final FocusEvent e) {
                onTableNamesChange();
            }

            @Override
            public void focusGained(final FocusEvent e) {
                // as long as the table names are not changed, no update is necessary
            }
        };

        private final JButton m_addButton = new JButton();

        private final JPanel m_relationshipPanelsContainer = new JPanel();

        /**
         * Keep a copy of the port object specs that this panel was initialized with during
         * {@link SendToPowerBINodeDialog2#loadSettingsFrom(NodeSettingsRO, org.knime.core.data.DataTableSpec[])} to be
         * able to initialize {@link ColumnSelectionComboxBox}es.
         */
        private transient PortObjectSpec[] m_latestPortSpecs;

        private RelationshipsTab() {
            setLayout(new CardLayout());
            add(getEditPanel(), CARD_LAYOUT_EDITABLE);
            add(getUnavailablePanel(), CARD_LAYOUT_UNAVAILABLE);
        }

        /**
         * Check whether the settings allow for editing relationships. If not, disable the panel.
         *
         * @return true if the relationship tab can be used to add/remove/edit relationships
         */
        private boolean checkEditable() {

            if (!m_createNewButton.isSelected()) {
                disableEdit(CANT_DEFINE_ON_EXISTING);
                return false;
            }

            if (m_latestPortSpecs == null) {
                return false;
            }

            if (Arrays.stream(m_latestPortSpecs).filter(s -> s instanceof DataTableSpec).count() < 2) {
                disableEdit("Relationships can only be defined between different tables.\n"
                    + "Add another input port to add another table.");
                return false;
            }

            // check that at least two tables with at least one column supported by Power BI each are connected
            Predicate<DataColumnSpec> isSupportedType =
                colSpec -> PowerBIDataTypeUtils.powerBITypeForKNIMEType(colSpec.getType()).isPresent();
            Predicate<PortObjectSpec> hasSupportedColumn =
                s -> s instanceof DataTableSpec && ((DataTableSpec)s).stream().filter(isSupportedType).count() > 0;
            if (Arrays.stream(m_latestPortSpecs).filter(hasSupportedColumn).count() < 2) {
                disableEdit(RelationshipsTab.NO_PROPER_TABLES_CONNECTED);
                return false;
            }

            ((CardLayout)getLayout()).show(this, CARD_LAYOUT_EDITABLE);
            return true;
        }

        private void disableEdit(final String reason) {
            m_errorLabel.setText(reason);
            ((CardLayout)getLayout()).show(this, CARD_LAYOUT_UNAVAILABLE);
        }

        /** The panel to show when relationships are editable */
        private JPanel getEditPanel() {
            JPanel editPanel = new JPanel();
            editPanel.setLayout(new BoxLayout(editPanel, BoxLayout.Y_AXIS));

            m_relationshipPanelsContainer.setLayout(new BoxLayout(m_relationshipPanelsContainer, BoxLayout.Y_AXIS));
            editPanel.add(m_relationshipPanelsContainer);

            m_addButton.setIcon(SharedIcons.ADD_PLUS.get());
            m_addButton.setText("Add relationship");
            m_addButton.addActionListener(e -> addRelationshipPanel(null, null, null, null, null));
            m_addButton.setAlignmentX(Component.CENTER_ALIGNMENT);
            editPanel.add(m_addButton);
            return editPanel;
        }

        /** The panel to show when relationships are not editable */
        private JPanel getUnavailablePanel() {
            final JPanel errorPanel = new JPanel(new GridBagLayout());
            final GridBagConstraints errorGbc = new GridBagConstraints();
            errorGbc.anchor = GridBagConstraints.CENTER;
            errorGbc.fill = GridBagConstraints.BOTH;

            m_errorLabel.setEditable(false);
            final JLabel dummy = new JLabel();
            m_errorLabel.setBackground(dummy.getBackground());
            m_errorLabel.setFont(dummy.getFont());
            m_errorLabel.setForeground(Color.RED);
            errorPanel.add(m_errorLabel, errorGbc);
            return errorPanel;
        }

        /**
         * Create a relationship panel per each relationship in the node settings.
         *
         * @param specs
         */
        private void loadSettingsFrom(final PortObjectSpec[] specs) {

            m_latestPortSpecs = specs;

            String[] relationshipFromTables = m_settings.getRelationshipFromTables();
            String[] relationshipFromColumns = m_settings.getRelationshipFromColumns();
            String[] relationshipToTables = m_settings.getRelationshipToTables();
            String[] relationshipToColumns = m_settings.getRelationshipToColumns();
            String[] relationshipCrossfilterBehaviors = m_settings.getRelationshipCrossfilterBehaviors();

            m_relationshipPanelsContainer.removeAll();
            m_relationshipPanels.clear();
            for (int i = 0; i < relationshipFromTables.length; i++) {
                addRelationshipPanel(relationshipFromTables[i], relationshipFromColumns[i], relationshipToTables[i],
                    relationshipToColumns[i], relationshipCrossfilterBehaviors[i]);
            }

            checkEditable();
        }

        /**
         * Create and add panel, then link the panel's remove button to
         * {@link #removeRelationshipPanel(RelationshipPanel)}.
         *
         * @param fromTable nullable default name of the selected table (the name entered by the user in the create data
         *            set section of the options tab)
         * @param fromColumn nullable default name of the column in the from table
         * @param toTable nullable default name of the target table
         * @param toColumn nullable default name of the target table's column
         * @param crossfilterBehavior nullable default cross filter behavior
         */
        private void addRelationshipPanel(final String fromTable, final String fromColumn, final String toTable,
            final String toColumn, final String crossfilterBehavior) {

            String borderTitle = "Relationship " + (m_relationshipPanels.size() + 1);

            RelationshipPanel panel =
                new RelationshipPanel(borderTitle, fromTable, fromColumn, toTable, toColumn, crossfilterBehavior);
            panel.m_remove.addActionListener(e -> removeRelationshipPanel(panel));

            m_relationshipPanels.add(panel);
            m_relationshipPanelsContainer.add(panel);
            m_relationshipPanelsContainer.revalidate();
            m_relationshipPanelsContainer.repaint();
        }

        /**
         * When the remove button on a relationship is pressed, remove the panel and relabel the existing ones
         * (Relationship 1, Relationship 2, ...)
         *
         * @param panel to remove
         */
        private void removeRelationshipPanel(final RelationshipPanel panel) {

            // remove the panel from the management collection
            m_relationshipPanels.remove(panel); // NOSONAR number of relationships is in the dozens at most

            // relabel the relationships according to their order of appearance
            m_relationshipPanels.forEach(new Consumer<RelationshipPanel>() {
                int m_number = 1;

                @Override
                public void accept(final RelationshipPanel t) {
                    t.setBorder(new TitledBorder("Relationship " + m_number));
                    m_number++;
                }
            });

            // remove the panel from the container panel
            m_relationshipPanelsContainer.remove(panel);
            m_relationshipPanelsContainer.revalidate();
            m_relationshipPanelsContainer.repaint();
        }

        /**
         * When the names of the tables in the "create dataset" section change, update the options in the select table
         * ComboBoxes.
         */
        private void onTableNamesChange() { // NOSONAR sonar didn't detect that this is called in an event listener
            m_relationshipPanels.forEach(RelationshipPanel::onTableNamesChange);
        }

        private String[] getRelationshipCrossfilterBehaviors() {
            return m_relationshipPanels.stream().map(RelationshipPanel::getCrossFilterBehavior).toArray(String[]::new);
        }

        private String[] getRelationshipToColumns() throws InvalidSettingsException {
            String[] names = new String[m_relationshipPanels.size()];
            for (int i = 0; i < m_relationshipPanels.size(); i++) {
                Optional<String> name = m_relationshipPanels.get(i).m_toPanel.getSelectedColumnName();
                if (name.isEmpty()) {
                    throw new InvalidSettingsException("Please select a target column in Relationship " + (i + 1));
                }
                names[i] = name.get();
            }
            return names;
        }

        private String[] getRelationshipToTables() {
            return m_relationshipPanels.stream().map(panel -> panel.m_toPanel.getSelectedTableName())
                .toArray(String[]::new);
        }

        private String[] getRelationshipFromColumns() throws InvalidSettingsException {
            String[] names = new String[m_relationshipPanels.size()];
            for (int i = 0; i < m_relationshipPanels.size(); i++) {
                Optional<String> name = m_relationshipPanels.get(i).m_fromPanel.getSelectedColumnName();
                if (name.isEmpty()) {
                    throw new InvalidSettingsException("Please select a source column in Relationship " + (i + 1));
                }
                names[i] = name.get();
            }
            return names;
        }

        private String[] getRelationshipFromTables() {
            return m_relationshipPanels.stream().map(panel -> panel.m_fromPanel.getSelectedTableName())
                .toArray(String[]::new);
        }

        /**
         * One instance per defined Relationship. Combines two {@link ColumnSelectorPanel}s and a selection option for
         * the cross filtering behavior.
         */
        private final class RelationshipPanel extends JPanel {

            private ColumnSelectorPanel m_fromPanel;

            private ColumnSelectorPanel m_toPanel;

            private transient DialogComponentButtonGroup m_crossFilterBehavior;

            private JButton m_remove;

            /**
             * Create
             *
             * @param borderTitle description of the relationship
             * @param fromTable nullable default name of the selected table (the name entered by the user in the create
             *            data set section of the options tab)
             * @param fromColumn nullable default name of the column in the from table
             * @param toTable nullable default name of the target table
             * @param toColumn nullable default name of the target table's column
             * @param crossfilterBehavior nullable default cross filter behavior
             */
            private RelationshipPanel(final String borderTitle, final String fromTable, final String fromColumn,
                final String toTable, final String toColumn, final String crossfilterBehavior) {

                setBorder(BorderFactory.createTitledBorder(borderTitle));
                setLayout(new GridBagLayout());

                m_remove = new JButton("Remove relationship", SharedIcons.DELETE_TRASH.get());
                m_fromPanel = new ColumnSelectorPanel("From", 1);
                m_toPanel = new ColumnSelectorPanel("To", 2);
                final TableSelection selectedFromTable = m_fromPanel.createControls(fromTable, fromColumn, -1);
                m_toPanel.createControls(toTable, toColumn, selectedFromTable.m_portNumber);
                m_fromPanel.setFilterPanel(m_toPanel);

                String defaultCrossFilter = crossfilterBehavior != null ? crossfilterBehavior : "Automatic";
                // the settings model is not used to load/save the value of the button group, see getCrossFilterBehavior
                SettingsModelString stringModel = new SettingsModelString("internal only", defaultCrossFilter);
                m_crossFilterBehavior = new DialogComponentButtonGroup(stringModel, "Cross filtering", true,
                    new String[]{"Automatic", "Both directions", "One direction"},
                    new String[]{"Automatic", "BothDirections", "OneDirection"});

                GridBagConstraints gbc = createGBC();
                gbc.insets = new Insets(5, 5, 5, 5);

                add(m_fromPanel, gbc);

                gbc.gridx++;
                add(m_toPanel, gbc);

                gbc.gridx++;
                add(m_crossFilterBehavior.getComponentPanel().getComponent(0), gbc);

                gbc.gridy++;
                gbc.fill = GridBagConstraints.NONE;
                gbc.anchor = GridBagConstraints.CENTER;
                add(m_remove, gbc);
            }

            /**
             * When the user changes the table name associated to an input port, update the comboboxes.
             */
            private void onTableNamesChange() {
                int fromTable = m_fromPanel.m_tableComboBox.getSelectedIndex();
                int fromColumn = m_fromPanel.m_columnComboBox.getSelectedIndex();
                int toTable = m_toPanel.m_tableComboBox.getSelectedIndex();
                int toColumn = m_toPanel.m_columnComboBox.getSelectedIndex();

                m_fromPanel.createControls(null, null, m_fromPanel.m_skipPort);
                m_toPanel.createControls(null, null, m_toPanel.m_skipPort);

                m_fromPanel.m_tableComboBox.setSelectedIndex(fromTable);
                m_fromPanel.m_columnComboBox.setSelectedIndex(fromColumn);
                m_toPanel.m_tableComboBox.setSelectedIndex(toTable);
                m_toPanel.m_columnComboBox.setSelectedIndex(toColumn);
            }

            private String getCrossFilterBehavior() {
                return ((SettingsModelString)m_crossFilterBehavior.getModel()).getStringValue();
            }

            @Override
            public Dimension getMaximumSize() {
                return new Dimension(Short.MAX_VALUE, getPreferredSize().height);
            }

            /**
             * One combined table/column selector - is used both for From (Table+Column) and To (Table+Column)
             */
            private final class ColumnSelectorPanel extends JPanel {

                JComboBox<TableSelection> m_tableComboBox;

                ColumnSelectionComboxBox m_columnComboBox;

                /**
                 * Pre-select the table name associated to this input port. Is 1 (first input table comes after auth
                 * port) for from panel and 2 for to panel.
                 */
                private final int m_defaultPortOffset;

                /**
                 * fired when {@link #m_tableComboBox} changes, excludes the selected table from the
                 * {@link ColumnSelectorPanel} passed to {@link #setFilterPanel(ColumnSelectorPanel)}
                 */
                private transient ItemListener m_updateFilterTargetPanel;

                /**
                 * Do not create an item for the table at the given port in the column selection combo box. Allows to
                 * exclude the table that is selected in the from {@link ColumnSelectorPanel} from the table combo box
                 * in the to {@link ColumnSelectorPanel}.
                 */
                private int m_skipPort = -1;

                /**
                 *
                 * @param defaultPortOffset preselected table
                 * @param borderTitle
                 */
                private ColumnSelectorPanel(final String label, final int defaultPortOffset) {
                    m_defaultPortOffset = defaultPortOffset;
                    setBorder(new TitledBorder(label));
                    setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));
                }

                /**
                 * Creates the table and column selection box.
                 *
                 * @param defaultTable nullable name of the table to preselect
                 * @param defaultColumn nullable name of the column to preselect
                 * @param skipPort port offset (zero based)
                 * @return the table selected during initialization (either because defaultTable was found or
                 *         m_defaultPortOffset was used instead)
                 */
                @SuppressWarnings("unchecked")
                private TableSelection createControls(final String defaultTable, final String defaultColumn,
                    final int skipPort) {

                    m_skipPort = skipPort;

                    removeAll();

                    m_tableComboBox = new JComboBox<>();
                    // fill table combo box
                    TableSelection selectedTable = populateTableComboBox(defaultTable, skipPort);
                    m_tableComboBox.setSelectedItem(selectedTable);
                    // add after restoring selection to avoid listeners firing in non-complete constructed environment
                    m_tableComboBox.addItemListener(e -> {
                        if (e.getStateChange() == ItemEvent.SELECTED) {
                            populateColumnComboBox((TableSelection)m_tableComboBox.getSelectedItem());
                        }
                    });
                    add(m_tableComboBox);

                    m_columnComboBox = new ColumnSelectionComboxBox((Border)null, DataValue.class);
                    ColumnComboBoxRenderer renderer = new ColumnComboBoxRenderer();
                    renderer.attachTo(m_columnComboBox);
                    populateColumnComboBox(selectedTable);
                    // restore column selection
                    if (defaultColumn != null) {
                        DataColumnSpec colSpec =
                            ((DataTableSpec)m_latestPortSpecs[selectedTable.m_portNumber]).getColumnSpec(defaultColumn);
                        if (colSpec != null) {
                            m_columnComboBox.setSelectedItem(colSpec);
                        } else {
                            // m_latestPortSpecs are outdated because they have changed without opening the dialog again
                            m_columnComboBox.setSelectedIndex(-1);
                        }
                    }
                    add(m_columnComboBox);

                    // when the table selection changes, update the available columns
                    m_tableComboBox.addItemListener(m_updateFilterTargetPanel);

                    return selectedTable;

                }

                /**
                 * Insert an option item for every table, except the one at port with offset `skipPort`
                 *
                 * @param defaultTable name (as entered in table name in options panel = m_tableNamesCreate) of the
                 *            table to select
                 * @param skipPort port offset (zero based) to not add to the combo box (to table selector should not
                 *            contain the table selected in the from table select)
                 * @return the combo box item representing the table with the name <code>defaultTable</code> or
                 *         representing the table at {@link #m_defaultPortOffset} or null if neither was found
                 */
                private TableSelection populateTableComboBox(final String defaultTable, final int skipPort) {
                    TableSelection selected = null;
                    // zero based index for table name text fields
                    int tableNameTextField = 0;
                    int ports = m_latestPortSpecs != null ? m_latestPortSpecs.length : 0;
                    for (int portOffset = 0; portOffset < ports; portOffset++) {
                        // skip non-data ports (authentication port)
                        if (!(m_latestPortSpecs[portOffset] instanceof DataTableSpec)) {
                            continue;
                        }
                        // skip explicitly excluded port
                        if (portOffset != skipPort) {
                            String tableName = m_tableNamesCreate[tableNameTextField].getText();
                            TableSelection comboItem = new TableSelection(portOffset, tableName);
                            m_tableComboBox.addItem(comboItem);
                            // if nothing has been found yet use the default port if matches
                            boolean useDefaultPort = portOffset == m_defaultPortOffset && selected == null;
                            // matching table name overwrites default port match
                            if (Objects.equals(defaultTable, tableName) || useDefaultPort) {
                                selected = comboItem;
                            }
                        }
                        // also skip the excluded port's associated table name text field
                        tableNameTextField++;
                    }
                    return selected == null ? m_tableComboBox.getItemAt(0) : selected;
                }

                /**
                 * Fill the column selection combo box with the columns available in the given table.
                 *
                 * @param selected contains the name and port offset of the table to use the datatablespec of
                 */
                private void populateColumnComboBox(final TableSelection selected) {
                    if (selected == null) {
                        return;
                    }
                    DataTableSpec portSpec = (DataTableSpec)m_latestPortSpecs[selected.m_portNumber];
                    try {
                        String tableName = m_tableNamesCreate[selected.m_portNumber - 1].getText() + " (Port "
                            + selected.m_portNumber + ")";
                        m_columnComboBox.update(portSpec, null, false, getPowerBICompatibleColumnsFilter(tableName));
                        m_columnComboBox.setSelectedIndex(0);
                    } catch (NotConfigurableException ex) {
                        LOGGER.warn(ex);
                    }

                }

                /**
                 * Create the listener that is attached to the table selection combo box: Upon selecting a table in this
                 * panel, remove that table from the other panel's table selector.
                 *
                 * @param toColumnSelectorPanel the panel in which to remove the selected table
                 */
                private void setFilterPanel(final ColumnSelectorPanel toColumnSelectorPanel) {

                    if (toColumnSelectorPanel == null) {
                        return;
                    }

                    /** Call when changing the selection of the from table combo box. */
                    m_updateFilterTargetPanel = e -> {
                        if (e.getStateChange() == ItemEvent.SELECTED) {
                            // restore selection
                            String selectedTable = toColumnSelectorPanel.getSelectedTableName();
                            String selectedColumn = toColumnSelectorPanel.getSelectedColumnName().orElse(null);
                            // don't include the current selected table in the to table selection combo box
                            int skipPort = ((TableSelection)m_tableComboBox.getSelectedItem()).m_portNumber;
                            toColumnSelectorPanel.createControls(selectedTable, selectedColumn, skipPort);
                        }
                    };
                    m_tableComboBox.addItemListener(m_updateFilterTargetPanel);
                }

                private ColumnFilter getPowerBICompatibleColumnsFilter(final String tableName) {
                    return new ColumnFilter() {
                        @Override
                        public boolean includeColumn(final DataColumnSpec colSpec) {
                            return PowerBIDataTypeUtils.powerBITypeForKNIMEType(colSpec.getType()).isPresent();
                        }

                        @Override
                        public String allFilteredMsg() {
                            return String.format("The table %s contains no columns that are supported by PowerBI",
                                tableName);
                        }

                    };
                }

                private String getSelectedTableName() {
                    TableSelection selectedItem = (TableSelection)m_tableComboBox.getSelectedItem();
                    return selectedItem == null ? null : selectedItem.m_tableName;
                }

                private Optional<String> getSelectedColumnName() {
                    return m_columnComboBox.getSelectedIndex() == -1 ? Optional.empty()
                        : Optional.of(m_columnComboBox.getSelectedColumn());
                }

            }

            /** ComboBox element type */
            private class TableSelection {
                private final int m_portNumber;

                private final String m_tableName;

                /**
                 * @param portNumber
                 * @param tableName
                 */
                TableSelection(final int portNumber, final String tableName) {
                    m_portNumber = portNumber;
                    m_tableName = tableName;
                }

                @Override
                public String toString() {
                    return String.format("Table %s (Port %s)", m_tableName, m_portNumber);
                }
            }

        }

    }
}
