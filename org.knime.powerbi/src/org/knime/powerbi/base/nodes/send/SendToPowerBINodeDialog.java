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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.border.TitledBorder;

import org.knime.azuread.auth.AzureADAuthenticator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.powerbi.base.nodes.send.SendToPowerBINodeSettings.OverwritePolicy;

/**
 * Dialog for the Send to PowerBI node.
 *
 * @author Benjamin Wilhem, KNIME GmbH, Konstanz, Germany
 */
class SendToPowerBINodeDialog extends NodeDialogPane {

    private final AzureADAuthenticator m_authenticator;

    private final SendToPowerBINodeSettings m_settings;

    private JTextField m_datasetName;

    private JTextField m_tableName;

    private JRadioButton m_overwriteButton;

    private JRadioButton m_appendButton;

    private JRadioButton m_abortButton;

    public SendToPowerBINodeDialog() {
        m_settings = new SendToPowerBINodeSettings();
        m_authenticator = new AzureADAuthenticator(SendToPowerBINodeModel.OAUTH_POWERBI_SCOPE);
        addTab("Options", createOptionsPanel());
    }

    private JPanel createOptionsPanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

        // Authentication panel
        final OAuthSettingsPanel authPanel = new OAuthSettingsPanel(m_authenticator);
        authPanel.setBorder(createTitledBorder("Authentication"));
        panel.add(authPanel, gbc);

        gbc.gridy++;
        // Dataset and Table selection
        final JPanel datasetPanel = createDatasetTablePanel();
        datasetPanel.setBorder(createTitledBorder("Dataset Selection"));
        panel.add(datasetPanel, gbc);

        return panel;
    }

    private JPanel createDatasetTablePanel() {
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = createGBC();

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
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {
        m_settings.loadSettingsFrom(settings, specs);
        m_authenticator.setAuthentication(m_settings.getAuthentication());
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
}
