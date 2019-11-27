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

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.border.TitledBorder;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.util.SharedIcons;
import org.knime.core.util.SwingWorkerWithContext;
import org.knime.ext.azuread.auth.Authenticator.AuthenticatorState;
import org.knime.ext.azuread.auth.AzureADAuthentication;
import org.knime.ext.azuread.auth.AzureADAuthenticator;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
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
final class SendToPowerBINodeDialog extends NodeDialogPane {

    static final String DATASET_PLACEHOLDER = "-- Authenticate to select dataset --";

    static final String TABLE_PLACEHOLDER = "-- Authenticate to select table --";

    private static final PowerBIDataset DATASET_PLACEHOLDER_OBJECT = new PowerBIDataset(DATASET_PLACEHOLDER, null);

    private static final PowerBIWorkspace DEFAULT_WORKSPACE = new PowerBIWorkspace("default", "");

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeModel.class);

    private final AzureADAuthenticator m_authenticator;

    private final SendToPowerBINodeSettings m_settings;

    private final int m_numberInputs;

    private final AtomicBoolean m_updatingWorkspaceOptions;

    private final AtomicBoolean m_updatingDatasetOptions;

    private JComboBox<PowerBIWorkspace> m_workspace;

    private OAuthSettingsPanel m_authPanel;

    private JRadioButton m_createNewButton;

    private JRadioButton m_appendToExisting;

    private JCheckBox m_allowOverwrite;

    private JTextField m_datasetNameCreate;

    private JTextField[] m_tableNamesCreate;

    private JComboBox<PowerBIDataset> m_datasetNameAppend;

    private JComboBox<String>[] m_tableNamesAppend;

    private JLabel m_authenticateInfo;

    public SendToPowerBINodeDialog() {
        m_numberInputs = 1;
        m_settings = new SendToPowerBINodeSettings();
        m_authenticator = new AzureADAuthenticator(SendToPowerBINodeModel.OAUTH_POWERBI_SCOPE);
        m_authenticator.addListener(this::authenticationChanged);
        m_authPanel = new OAuthSettingsPanel(m_authenticator);
        m_updatingWorkspaceOptions = new AtomicBoolean(false);
        m_updatingDatasetOptions = new AtomicBoolean(false);
        addTab("Options", createOptionsPanel());
    }

    private JPanel createOptionsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();
        gbc.weighty = 0;
        gbc.anchor = GridBagConstraints.NORTHWEST;

        // Authentication panel
        m_authPanel.setBorder(createTitledBorder("Authentication"));
        panel.add(m_authPanel, gbc);

        // Listeners to clear stored credentials
        m_authPanel.addClearSelectedLocationListener(a -> {
            CredentialsLocationType locationType = m_authPanel.getCredentialsSaveLocation();
            try {
                m_settings.clearAuthentication(locationType);
            } catch (InvalidSettingsException ex) {
                String msg =
                    "Could not clear " + locationType.getShortText() + " credentials. Reason: " + ex.getMessage();
                LOGGER.error(msg, ex);
            }
        });

        m_authPanel.addClearAllLocationListener(a -> {
            for (CredentialsLocationType clt : CredentialsLocationType.values()) {
                try {
                    m_settings.clearAuthentication(clt);
                } catch (InvalidSettingsException ex) {
                    String msg = "Could not clear " + clt.getShortText() + " credentials. Reason: " + ex.getMessage();
                    LOGGER.error(msg, ex);
                }
            }
        });

        gbc.gridy++;
        // Dataset and Table selection
        final JPanel datasetPanel = createDatasetTablePanel();
        datasetPanel.setBorder(createTitledBorder("Dataset"));
        panel.add(datasetPanel, gbc);

        // Hidden panel to make the dialog always stick to the top
        gbc.weighty = 1;
        panel.add(new JPanel(), gbc);

        return panel;
    }

    private JPanel createDatasetTablePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Workspace
        panel.add(new JLabel("Workspace"), gbc);
        gbc.gridx++;
        gbc.weightx = 5;
        m_workspace = new JComboBox<>(new PowerBIWorkspace[]{DEFAULT_WORKSPACE});
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

        gbc.gridy++;
        gbc.gridx = 1;
        gbc.weightx = 1;
        panel.add(createCreateNewDatasetPanel(), gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        // Append to existing dataset
        m_appendToExisting = new JRadioButton("Append to existing Dataset");
        panel.add(m_appendToExisting, gbc);

        gbc.gridy++;
        gbc.gridx = 1;
        gbc.weightx = 1;
        panel.add(createAppendToDatasetPanel(), gbc);

        // Button group
        final ButtonGroup bg = new ButtonGroup();
        bg.add(m_createNewButton);
        bg.add(m_appendToExisting);

        // Action listener for create new/append
        m_createNewButton.addActionListener(e -> enableDisableComboboxes());
        m_appendToExisting.addActionListener(e -> enableDisableComboboxes());

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        gbc.gridwidth = 2;
        // Authenticate info
        m_authenticateInfo = new JLabel("Authenticate to select workspace, dataset and table", SharedIcons.INFO.get(),
            SwingConstants.LEFT);
        panel.add(m_authenticateInfo, gbc);

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
            final String label = m_numberInputs == 1 ? "Table name" : "Table name " + (i + 1);
            panel.add(new JLabel(label), gbc);
            gbc.gridx++;
            gbc.weightx = 3;
            m_tableNamesCreate[i] = new JTextField("");
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

    /** Append to an existing dataset panel */
    private JPanel createAppendToDatasetPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Dataset name
        panel.add(new JLabel("Dataset name"), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        m_datasetNameAppend = new JComboBox<>(new PowerBIDataset[]{DATASET_PLACEHOLDER_OBJECT});
        m_datasetNameAppend.addActionListener(e -> updateTableOptions());
        panel.add(m_datasetNameAppend, gbc);

        @SuppressWarnings("unchecked")
        final JComboBox<String>[] tableNamesAppendLocal = new JComboBox[m_numberInputs];
        m_tableNamesAppend = tableNamesAppendLocal;
        for (int i = 0; i < m_numberInputs; i++) {
            gbc.gridy++;
            gbc.gridx = 0;
            gbc.weightx = 1;
            // Table name
            final String label = m_numberInputs == 1 ? "Table name" : "Table name " + (i + 1);
            panel.add(new JLabel(label), gbc);
            gbc.gridx++;
            gbc.weightx = 3;
            m_tableNamesAppend[i] = new JComboBox<>(new String[]{TABLE_PLACEHOLDER});
            panel.add(m_tableNamesAppend[i], gbc);
        }

        return panel;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        // Authentication
        m_settings.setAuthentication(m_authenticator.getAuthentication());

        // Workspace
        m_settings.setWorkspace(((PowerBIWorkspace)m_workspace.getSelectedItem()).getIdentifier());

        // Create new or append and overwrite
        final boolean createNew = m_createNewButton.isSelected();
        m_settings.setCreateNewDataset(createNew);
        m_settings.setAllowOverwrite(m_allowOverwrite.isSelected());

        // Dataset and table names
        if (createNew) {
            m_settings.setDatasetName(m_datasetNameCreate.getText());
            final String[] tableNames =
                Arrays.stream(m_tableNamesCreate).map(JTextField::getText).toArray(String[]::new);
            m_settings.setTableNames(tableNames);
        } else {
            final PowerBIDataset dataset = (PowerBIDataset)m_datasetNameAppend.getSelectedItem();
            if (dataset == null) {
                throw new InvalidSettingsException("Please select a dataset.");
            }
            m_settings.setDatasetName(dataset.m_name);

            final String[] tableNames =
                Arrays.stream(m_tableNamesAppend).map(c -> (String)c.getSelectedItem()).toArray(String[]::new);
            m_settings.setTableNames(tableNames);
        }

        final CredentialsLocationType saveLocation = m_authPanel.getCredentialsSaveLocation();
        m_settings.setCredentialsSaveLocation(saveLocation);
        m_settings.setFilesystemLocation(m_authPanel.getFilesystemLocation());

        try {
            m_settings.saveSettingsTo(settings);
        } catch (IOException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {

        // Do not block dialog if problem occurs
        try {
            m_settings.loadValidatedSettingsFrom(settings);
        } catch (InvalidSettingsException | IOException ex) {
            LOGGER.warn("Can't load settings. Reason: " + ex.getMessage(), ex);
        }

        // Authentication
        m_authPanel.setCredentialsSaveLocation(m_settings.getCredentialsSaveLocation());
        m_authPanel.setFilesystemLocation(m_settings.getFilesystemLocation());
        m_authenticator.setAuthentication(m_settings.getAuthentication());

        // Workspace
        final String workspaceId = m_settings.getWorkspace();
        if (workspaceId.isEmpty()) {
            m_workspace.setSelectedItem(DEFAULT_WORKSPACE);
        } else {
            final PowerBIWorkspace selectedWorkspace = new PowerBIWorkspace(workspaceId, workspaceId);
            m_workspace.addItem(selectedWorkspace);
            m_workspace.setSelectedItem(selectedWorkspace);
        }

        // Create or append
        final boolean createNewDataset = m_settings.isCreateNewDataset();
        if (createNewDataset) {
            m_createNewButton.doClick();
        } else {
            m_appendToExisting.doClick();
        }

        // Dataset and table name
        if (createNewDataset) {
            // Dataset name
            m_datasetNameCreate.setText(m_settings.getDatasetName());
            // Table names
            final String[] tableNames = m_settings.getTableNames();
            for (int i = 0; i < m_numberInputs && i < tableNames.length; i++) {
                m_tableNamesCreate[i].setText(tableNames[i]);
            }
        } else {
            // Dataset name
            final String datasetName = m_settings.getDatasetName();
            final PowerBIDataset dataset = new PowerBIDataset(datasetName, null);
            m_datasetNameAppend.addItem(dataset);
            m_datasetNameAppend.setSelectedItem(dataset);
            // Table names
            final String[] tableNames = m_settings.getTableNames();
            for (int i = 0; i < m_numberInputs && i < tableNames.length; i++) {
                m_tableNamesAppend[i].addItem(tableNames[i]);
                m_tableNamesAppend[i].setSelectedItem(tableNames[i]);
            }
        }

        // Allow overwrite
        m_allowOverwrite.setSelected(m_settings.isAllowOverwrite());
    }

    /* -------------------------------------- Handling changes ---------------------------- */

    private void authenticationChanged(final AuthenticatorState s) {
        if (AuthenticatorState.AUTHENTICATED.equals(s)) {
            updateWorkspaceOptions();
        }
        enableDisableComboboxes();
    }

    /** Enables or diables the create new fields and disables or enables the append fields */
    private void enableDisableComboboxes() {
        final boolean authenticated = AuthenticatorState.AUTHENTICATED.equals(m_authenticator.getState());
        final boolean enableNewDataset = m_createNewButton.isSelected();
        final boolean enableDatasetAppend = !enableNewDataset && authenticated;

        // Workspace selection
        m_workspace.setEnabled(authenticated);

        // Create new dataset
        m_datasetNameCreate.setEnabled(enableNewDataset);
        for (final JTextField tableNameCreate : m_tableNamesCreate) {
            tableNameCreate.setEnabled(enableNewDataset);
        }
        m_allowOverwrite.setEnabled(enableNewDataset);

        // Append to existing dataset
        m_datasetNameAppend.setEnabled(enableDatasetAppend);
        for (final JComboBox<String> tableNameAppend : m_tableNamesAppend) {
            tableNameAppend.setEnabled(enableDatasetAppend);
        }
    }

    /** Start a thread to update the workspace options */
    private void updateWorkspaceOptions() {
        final AzureADAuthentication auth = m_authenticator.getAuthentication();
        final DefaultSwingWorker<List<PowerBIWorkspace>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableWorkspaces(auth), //
            this::setWorkspaceOptions, //
            "Updating the available datasets failed.");
        worker.execute();
    }

    /** Start a thread to update the dataset options */
    private void updateDatasetOptions() {
        if (m_updatingWorkspaceOptions.get()) {
            return;
        }
        final PowerBIWorkspace workspace = (PowerBIWorkspace)m_workspace.getSelectedItem();
        if (workspace == null) {
            // No workspace selected
            return;
        }
        final String workspaceId = workspace.getIdentifier();
        final AzureADAuthentication auth = m_authenticator.getAuthentication();
        final DefaultSwingWorker<List<PowerBIDataset>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableDatasets(auth, workspaceId), //
            this::setDatasetOptions, //
            "Updating the available datasets failed.");
        worker.execute();
    }

    /** Start a thread to update the table options */
    private void updateTableOptions() {
        if (m_updatingDatasetOptions.get()) {
            return;
        }
        final PowerBIWorkspace workspace = (PowerBIWorkspace)m_workspace.getSelectedItem();
        final PowerBIDataset dataset = (PowerBIDataset)m_datasetNameAppend.getSelectedItem();
        if (workspace == null || dataset == null || dataset.getIdentifier() == null) {
            // No dataset selected (or loaded from the settings)
            return;
        }
        final String workspaceId = workspace.getIdentifier();
        final String datasetId = dataset.getIdentifier();
        final AzureADAuthentication auth = m_authenticator.getAuthentication();
        final DefaultSwingWorker<List<String>> worker = new DefaultSwingWorker<>( //
            () -> getAvailableTables(auth, workspaceId, datasetId), //
            this::setTableOptions, //
            "Updating the available tables failed.");
        worker.execute();
    }

    /** Set the workspace options to the given argument (and reselect or select the default) */
    private void setWorkspaceOptions(final List<PowerBIWorkspace> workspaceNames) {
        // Get the selected value (or default)
        PowerBIWorkspace selected = (PowerBIWorkspace)m_workspace.getSelectedItem();
        if (!workspaceNames.contains(selected)) {
            selected = DEFAULT_WORKSPACE;
        }

        // Update the options and reselect
        m_updatingWorkspaceOptions.set(true);
        m_workspace.removeAllItems();
        for (final PowerBIWorkspace w : workspaceNames) {
            m_workspace.addItem(w);
        }
        m_updatingWorkspaceOptions.set(false);
        m_workspace.setSelectedItem(selected);
        m_workspace.setEnabled(true);
    }

    /** Set the dataset options to the given argument (and reselect or select the default) */
    private void setDatasetOptions(final List<PowerBIDataset> datasetNames) {
        // Get the selected value
        final PowerBIDataset selected = (PowerBIDataset)m_datasetNameAppend.getSelectedItem();

        // Update the options and reselect
        m_updatingDatasetOptions.set(true);
        m_datasetNameAppend.removeAllItems();
        for (final PowerBIDataset d : datasetNames) {
            m_datasetNameAppend.addItem(d);
        }
        m_updatingDatasetOptions.set(false);
        // Reselect
        if (datasetNames.contains(selected)) {
            m_datasetNameAppend.setSelectedItem(selected);
        } else {
            m_datasetNameAppend.setSelectedIndex(0);
        }

    }

    /** Set the table options to the given argument (and reselect or select the default) */
    private void setTableOptions(final List<String> tableNames) {
        for (int i = 0; i < m_numberInputs; i++) {
            final JComboBox<String> tableNameAppend = m_tableNamesAppend[i];
            // Get the selected values
            final String selected = (String)tableNameAppend.getSelectedItem();

            // Update the options and reselect
            tableNameAppend.removeAllItems();
            for (final String t : tableNames) {
                tableNameAppend.addItem(t);
            }
            // Reselect
            if (tableNames.contains(selected)) {
                tableNameAppend.setSelectedItem(selected);
            } else {
                tableNameAppend.setSelectedIndex(i % tableNames.size());
            }
        }
    }

    /* ------------------------------------------------ REST API helpers ------------------------ */

    /** Call the REST API to get the available workspaces */
    private static List<PowerBIWorkspace> getAvailableWorkspaces(final AzureADAuthentication auth)
        throws PowerBIResponseException {
        final Groups groups = PowerBIRestAPIUtils.getGroups(auth);
        final List<PowerBIWorkspace> workspaces = Arrays.stream(groups.getValue()) //
            .map(g -> new PowerBIWorkspace(g.getName(), g.getId())) //
            .collect(Collectors.toCollection(ArrayList::new));
        workspaces.add(DEFAULT_WORKSPACE);
        return workspaces;
    }

    /** Call the REST API to get the available datasets */
    private static List<PowerBIDataset> getAvailableDatasets(final AzureADAuthentication auth, final String workspaceId)
        throws PowerBIResponseException {
        final Datasets datasets;
        if (workspaceId.isEmpty()) {
            datasets = PowerBIRestAPIUtils.getDatasets(auth);
        } else {
            datasets = PowerBIRestAPIUtils.getDatasets(auth, workspaceId);
        }
        return Arrays.stream(datasets.getValue()) //
            .map(d -> new PowerBIDataset(d.getName(), d.getId())) //
            .collect(Collectors.toList());
    }

    private static List<String> getAvailableTables(final AzureADAuthentication auth, final String workspaceId,
        final String datasetId) throws PowerBIResponseException {
        final Tables tables;
        if (workspaceId.isEmpty()) {
            tables = PowerBIRestAPIUtils.getTables(auth, datasetId);
        } else {
            tables = PowerBIRestAPIUtils.getTables(auth, workspaceId, datasetId);
        }
        return Arrays.stream(tables.getValue()) //
            .map(Table::getName) //
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

    private static TitledBorder createTitledBorder(final String title) {
        return BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), title);
    }

    /** A Power BI workspace to use in the combobox */
    private static class PowerBIWorkspace {

        private final String m_name;

        private final String m_identifier;

        public PowerBIWorkspace(final String name, final String identifier) {
            m_name = name;
            m_identifier = identifier;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof PowerBIWorkspace)) {
                return false;
            }
            final PowerBIWorkspace o = (PowerBIWorkspace)obj;
            return Objects.equals(m_identifier, o.m_identifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_identifier);
        }

        @Override
        public String toString() {
            return m_name;
        }

        public String getIdentifier() {
            return m_identifier;
        }
    }

    /** A Power BI dataset to use in the combobox (identified by the name) */
    private static class PowerBIDataset {

        private final String m_name;

        private final String m_identifier;

        public PowerBIDataset(final String name, final String identifier) {
            m_name = name;
            m_identifier = identifier;
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof PowerBIDataset)) {
                return false;
            }
            final PowerBIDataset o = (PowerBIDataset)obj;
            return Objects.equals(m_name, o.m_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(m_name);
        }

        @Override
        public String toString() {
            return m_name;
        }

        public String getIdentifier() {
            return m_identifier;
        }
    }

    /** Default swing worker which uses a supplier and consumer */
    private static class DefaultSwingWorker<T> extends SwingWorkerWithContext<T, Void> {

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
}
