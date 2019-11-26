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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.util.SwingWorkerWithContext;
import org.knime.ext.azuread.auth.Authenticator.AuthenticatorState;
import org.knime.ext.azuread.auth.AzureADAuthentication;
import org.knime.ext.azuread.auth.AzureADAuthenticator;
import org.knime.ext.powerbi.base.nodes.send.SendToPowerBINodeSettings.OverwritePolicy;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.ext.powerbi.core.rest.bindings.Groups;

/**
 * Dialog for the Send to Power BI node.
 *
 * @author Benjamin Wilhem, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeDialog extends NodeDialogPane {

    private static final PowerBIWorkspace WORKSPACE_PLACEHOLDER =
        new PowerBIWorkspace("Authenticate to select workspace", "no_identifier");

    private static final PowerBIWorkspace DEFAULT_WORKSPACE = new PowerBIWorkspace("default", "");

    private static final NodeLogger LOGGER = NodeLogger.getLogger(SendToPowerBINodeModel.class);

    private final AzureADAuthenticator m_authenticator;

    private final SendToPowerBINodeSettings m_settings;

    private JTextField m_datasetName;

    private JTextField m_tableName;

    private JComboBox<PowerBIWorkspace> m_workspace;

    private JRadioButton m_overwriteButton;

    private JRadioButton m_appendButton;

    private JRadioButton m_abortButton;

    private OAuthSettingsPanel m_authPanel;

    public SendToPowerBINodeDialog() {
        m_settings = new SendToPowerBINodeSettings();
        m_authenticator = new AzureADAuthenticator(SendToPowerBINodeModel.OAUTH_POWERBI_SCOPE);
        m_authenticator.addListener(this::authenticationChanged);
        m_authPanel = new OAuthSettingsPanel(m_authenticator);
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
        datasetPanel.setBorder(createTitledBorder("Dataset Selection"));
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
        gbc.weightx = 3;
        m_workspace = new JComboBox<>(new PowerBIWorkspace[]{WORKSPACE_PLACEHOLDER});
        m_workspace.setEnabled(false);
        panel.add(m_workspace, gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        // Dataset name
        panel.add(new JLabel("Dataset name"), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        m_datasetName = new JTextField("");
        panel.add(m_datasetName, gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        // Table name
        panel.add(new JLabel("Table name"), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        m_tableName = new JTextField("");
        panel.add(m_tableName, gbc);

        gbc.gridy++;
        gbc.gridx = 0;
        gbc.weightx = 1;
        // Overwrite policy
        panel.add(new JLabel("If dataset exists..."), gbc);
        gbc.gridx++;
        gbc.weightx = 3;
        panel.add(createOverwritePolicyPanel(), gbc);

        return panel;
    }

    private JPanel createOverwritePolicyPanel() {
        final JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.X_AXIS));
        final ButtonGroup bg = new ButtonGroup();

        // Overwrite button
        m_overwriteButton = new JRadioButton("Overwrite");
        m_overwriteButton.setAlignmentY(Component.TOP_ALIGNMENT);
        bg.add(m_overwriteButton);
        panel.add(m_overwriteButton);
        panel.add(Box.createHorizontalStrut(20));

        // Append button
        m_appendButton = new JRadioButton("Append");
        m_appendButton.setAlignmentY(Component.TOP_ALIGNMENT);
        bg.add(m_appendButton);
        panel.add(m_appendButton);
        panel.add(Box.createHorizontalStrut(20));

        // Abort button
        m_abortButton = new JRadioButton("Abort");
        m_abortButton.setAlignmentY(Component.TOP_ALIGNMENT);
        bg.add(m_abortButton);
        panel.add(m_abortButton);
        panel.add(Box.createHorizontalGlue());

        return panel;
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setAuthentication(m_authenticator.getAuthentication());
        m_settings.setWorkspace(((PowerBIWorkspace)m_workspace.getSelectedItem()).getIdentifier());
        m_settings.setDatasetName(m_datasetName.getText());
        m_settings.setTableName(m_tableName.getText());
        final OverwritePolicy overwritePolicy;
        if (m_overwriteButton.isSelected()) {
            overwritePolicy = OverwritePolicy.OVERWRITE;
        } else if (m_appendButton.isSelected()) {
            overwritePolicy = OverwritePolicy.APPEND;
        } else {
            overwritePolicy = OverwritePolicy.ABORT;
        }
        m_settings.setOverwritePolicy(overwritePolicy);

        CredentialsLocationType saveLocation = m_authPanel.getCredentialsSaveLocation();
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

        m_authPanel.setCredentialsSaveLocation(m_settings.getCredentialsSaveLocation());
        m_authPanel.setFilesystemLocation(m_settings.getFilesystemLocation());

        m_authenticator.setAuthentication(m_settings.getAuthentication());
        final String workspaceId = m_settings.getWorkspace();
        if (workspaceId.isEmpty()) {
            m_workspace.setSelectedItem(DEFAULT_WORKSPACE);
        } else {
            final PowerBIWorkspace selectedWorkspace = new PowerBIWorkspace(workspaceId, workspaceId);
            m_workspace.addItem(selectedWorkspace);
            m_workspace.setSelectedItem(selectedWorkspace);
        }
        m_datasetName.setText(m_settings.getDatasetName());
        m_tableName.setText(m_settings.getTableName());
        switch (m_settings.getOverwritePolicy()) {
            case OVERWRITE:
                m_overwriteButton.doClick();
                break;
            case APPEND:
                m_appendButton.doClick();
                break;
            case ABORT:
                m_abortButton.doClick();
                break;
            default:
                // Cannot happen
                break;
        }
    }

    private void authenticationChanged(final AuthenticatorState s) {
        if (AuthenticatorState.AUTHENTICATED.equals(s)) {
            updateWorkspaceOptions();
        }
    }

    private void updateWorkspaceOptions() {
        new SwingWorkerWithContext<List<PowerBIWorkspace>, Void>() {

            @Override
            protected List<PowerBIWorkspace> doInBackgroundWithContext() throws Exception {
                final AzureADAuthentication auth = m_authenticator.getAuthentication();
                return getAvailableWorkspaces(auth);
            }

            @Override
            protected void doneWithContext() {
                try {
                    setWorkspaceOptions(get());
                } catch (final InterruptedException | ExecutionException e) {
                    LOGGER.warn("Updating the available workspaces failed.", e);
                }
            }
        }.execute();
    }

    private void setWorkspaceOptions(final List<PowerBIWorkspace> workspaceNames) {
        // Get the selected value (or default)
        PowerBIWorkspace selected = (PowerBIWorkspace)m_workspace.getSelectedItem();
        if (!workspaceNames.contains(selected)) {
            selected = DEFAULT_WORKSPACE;
        }

        // Update the options and reselect
        m_workspace.removeAllItems();
        for (final PowerBIWorkspace w : workspaceNames) {
            m_workspace.addItem(w);
        }
        m_workspace.setSelectedItem(selected);
        m_workspace.setEnabled(true);
    }

    private static List<PowerBIWorkspace> getAvailableWorkspaces(final AzureADAuthentication auth)
        throws PowerBIResponseException {
        final Groups groups = PowerBIRestAPIUtils.getGroups(auth);
        final List<PowerBIWorkspace> workspaces = Arrays.stream(groups.getValue()) //
            .map(g -> new PowerBIWorkspace(g.getName(), g.getId())) //
            .collect(Collectors.toCollection(ArrayList::new));
        workspaces.add(DEFAULT_WORKSPACE);
        return workspaces;
    }

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
            return m_identifier.equals(o.m_identifier);
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
}
