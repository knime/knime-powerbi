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
 *   Oct 8, 2019 (benjamin): created
 */
package org.knime.powerbi.base.nodes.send;

import java.awt.Container;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;

import org.knime.azuread.auth.Authenticator;
import org.knime.azuread.auth.Authenticator.AuthenticatorState;

/**
 * A default settings panel for OAuth based authentication with a service.
 *
 * TODO Is this always OAuth or could it be something else?
 *
 * TODO Move somewhere else where it can be reused
 *
 * TODO Add options on where to save the token (see GoogleAuthNodeDialogPane)
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Ole Ostergaard, KNIME GmbH, Konstanz, Germany
 */
public class OAuthSettingsPanel extends JPanel {

    private static final String STATUS_LABEL_TEXT = "Status: ";

    private static final String INITIAL_AUTH_STATE_TEXT = "Unknown";

    private static final String AUTHENTICATE_BUTTON_TEXT = "Authenticate";

    private static final String CANCEL_BUTTON_TEXT = "Cancel";

    private static final String CLEAR_BUTTON_TEXT = "Clear Credentials";

    private static final String PROGRESS_BAR_TEXT = "Auth in browser...";

    private static final long serialVersionUID = 1L;

    private final Authenticator m_auth;

    private JLabel m_authStateLabel;

    private JPanel m_authButtonOrCancelPanel;

    private JButton m_authButton;

    private JProgressBar m_progressBar;

    private JButton m_cancelButton;

    private JButton m_clearButton;

    /**
     * Create a new OAuth authentication panel which uses the given authenticator to do the authentication.
     *
     * @param auth the authenticator which handles the authentication
     */
    public OAuthSettingsPanel(final Authenticator auth) {
        super(new GridBagLayout());
        m_auth = auth;

        createPanel();

        // Action listeners
        m_auth.addListener(this::authStateChanged);
        m_authButton.addActionListener(e -> startAuthentication());
        m_cancelButton.addActionListener(e -> cancelAuthentication());
        m_clearButton.addActionListener(e -> clearAuthentication());

        // Set the current state
        authStateChanged(m_auth.getState());
    }

    // <<<<<<<<<<<<<<<< Actions

    private void startAuthentication() {
        m_auth.authenticate();
        // Note: The UI will update because of an authenticator state change
    }

    private void cancelAuthentication() {
        m_auth.cancel();
        // Note: The UI will update because of an authenticator state change
    }

    private void clearAuthentication() {
        m_auth.clearAuthentication();
        // Note: The UI will update because of an authenticator state change
    }

    private synchronized void authStateChanged(final AuthenticatorState state) {
        m_authStateLabel.setText(state.toString());

        // Authentication in progress: Show the progress bar and cancel button
        // Else: Show the authenticate button
        showCancelButton(AuthenticatorState.AUTHENTICATION_IN_PROGRESS.equals(state));

        // If the authentication failed: Show the reason
        if (AuthenticatorState.FAILED.equals(state)) {
            showError();
        }
    }

    /** Show the cancel button (true) or the authenticate button (false) */
    private void showCancelButton(final boolean showCancel) {
        m_authButtonOrCancelPanel.removeAll();
        if (showCancel) {
            m_authButtonOrCancelPanel.add(m_progressBar);
            m_authButtonOrCancelPanel.add(m_cancelButton);
        } else {
            m_authButtonOrCancelPanel.add(m_authButton);
        }
        m_authButtonOrCancelPanel.revalidate();
        m_authButtonOrCancelPanel.repaint();
    }

    /** Show the error of the authenticator in a dialog */
    private void showError() {
        final String error = m_auth.getErrorDescription();
        if (error != null) {
            JOptionPane.showMessageDialog(findParentFrame(), error, "Authentication failed", JOptionPane.ERROR_MESSAGE);
        }
    }

    // <<<<<<<<<<<<<<<< UI

    private void createPanel() {
        final GridBagConstraints gbc = getDefaultGBC();

        add(createAuthButtonOrCancelPanel(), gbc);

        gbc.gridx++;
        // Status label
        add(new JLabel(STATUS_LABEL_TEXT), gbc);

        gbc.gridx++;
        // Authentication state label
        m_authStateLabel = new JLabel(INITIAL_AUTH_STATE_TEXT);
        add(m_authStateLabel, gbc);

        gbc.gridx = 0;
        gbc.gridy++;

        m_clearButton = new JButton(CLEAR_BUTTON_TEXT);
        add(m_clearButton, gbc);
    }

    private Frame findParentFrame() {
        Container container = this;
        while (container != null) {
            if (container instanceof Frame) {
                return (Frame)container;
            }
            container = container.getParent();
        }
        return null;
    }

    private JPanel createAuthButtonOrCancelPanel() {
        m_authButtonOrCancelPanel = new JPanel();

        // Auth button
        m_authButton = new JButton(AUTHENTICATE_BUTTON_TEXT);
        final Dimension authButtonDims = new Dimension(240, m_authButton.getPreferredSize().height);
        m_authButton.setSize(authButtonDims);
        m_authButton.setPreferredSize(authButtonDims);
        m_authButtonOrCancelPanel.add(m_authButton);

        // Progress bar
        m_progressBar = new JProgressBar();
        m_progressBar.setIndeterminate(true);
        m_progressBar.setStringPainted(true);
        m_progressBar.setString(PROGRESS_BAR_TEXT);

        // Cancel button
        m_cancelButton = new JButton(CANCEL_BUTTON_TEXT);

        return m_authButtonOrCancelPanel;
    }

    private static GridBagConstraints getDefaultGBC() {
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.anchor = GridBagConstraints.LINE_START;
        gbc.fill = GridBagConstraints.HORIZONTAL;
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 1;
        gbc.weighty = 1;
        gbc.insets = new Insets(5, 5, 5, 5);
        return gbc;
    }
}
