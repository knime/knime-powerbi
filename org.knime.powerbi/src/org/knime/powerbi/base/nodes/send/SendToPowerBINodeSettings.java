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
package org.knime.powerbi.base.nodes.send;

import org.knime.azuread.auth.AzureADAuthentication;
import org.knime.azuread.auth.DefaultAzureADAuthentication;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.config.Config;

/**
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
class SendToPowerBINodeSettings {

    private static final String ENCRYPTION_KEY = "9J4jG3m1v2FKmH9C5TffFw";

    private static final String CFG_KEY_AUTHENTICATION = "authentication";

    private static final String CFG_KEY_ACCESS_TOKEN = "access_token";

    private static final String CFG_KEY_REFRESH_TOKEN = "refresh_token";

    private static final String CFG_KEY_VALID_UNTIL = "valid_until";

    private static final String CFG_KEY_WORKSPACE = "workspace";

    private static final String CFG_KEY_DATASET_NAME = "dataset_name";

    private static final String CFG_KEY_TABLE_NAME = "table_name";

    private static final String CFG_KEY_OVERWRITE_POLICY = "overwrite_policy";

    private AzureADAuthentication m_authentication;

    private String m_workspace = "";

    private String m_datasetName = "";

    private String m_tableName = "";

    private OverwritePolicy m_overwritePolicy = OverwritePolicy.ABORT;

    /**
     * Policy how to proceed when output table exists (overwrite, abort, append).
     */
    enum OverwritePolicy {
            /** Fail during configure/execute. */
            ABORT,
            /** Overwrite existing file. */
            OVERWRITE,
            /** Append to existing file. */
            APPEND
    }

    /**
     * @return the authentication
     */
    AzureADAuthentication getAuthentication() {
        return m_authentication;
    }

    /**
     * @param authentication the authentication to set
     */
    void setAuthentication(final AzureADAuthentication authentication) {
        m_authentication = authentication;
    }

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
     * @return the tableName
     */
    String getTableName() {
        return m_tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    void setTableName(final String tableName) {
        m_tableName = tableName;
    }

    /**
     * @return the overwritePolicy
     */
    OverwritePolicy getOverwritePolicy() {
        return m_overwritePolicy;
    }

    /**
     * @param overwritePolicy the overwritePolicy to set
     */
    void setOverwritePolicy(final OverwritePolicy overwritePolicy) {
        m_overwritePolicy = overwritePolicy;
    }

    void saveSettingsTo(final NodeSettingsWO settings) {
        if (getAuthentication() != null) {
            final Config authConfig = settings.addConfig(CFG_KEY_AUTHENTICATION);
            authConfig.addPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY, getAuthentication().getAccessToken());
            authConfig.addPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY,
                getAuthentication().getRefreshToken().orElseGet(() -> null));
            authConfig.addLong(CFG_KEY_VALID_UNTIL, getAuthentication().getValidUntil());
        }
        settings.addString(CFG_KEY_WORKSPACE, getWorkspace());
        settings.addString(CFG_KEY_DATASET_NAME, getDatasetName());
        settings.addString(CFG_KEY_TABLE_NAME, getTableName());
        settings.addString(CFG_KEY_OVERWRITE_POLICY, getOverwritePolicy().name());
    }

    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Note that the workspace config can be empty
        settings.getString(CFG_KEY_WORKSPACE);
        final String datasetName = settings.getString(CFG_KEY_DATASET_NAME);
        final String tableName = settings.getString(CFG_KEY_TABLE_NAME);
        OverwritePolicy.valueOf(settings.getString(CFG_KEY_OVERWRITE_POLICY));
        if (datasetName == null || datasetName.trim().isEmpty()) {
            throw new InvalidSettingsException("The dataset name must be set.");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new InvalidSettingsException("The table name must be set.");
        }
    }

    void loadSettingsFrom(final NodeSettingsRO settings, @SuppressWarnings("unused") final DataTableSpec[] specs)
        throws NotConfigurableException {
        try {
            if (settings.containsKey(CFG_KEY_AUTHENTICATION)) {
                final Config authConfig = settings.getConfig(CFG_KEY_AUTHENTICATION);
                final String accessToken = authConfig.getPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY);
                final String refreshToken = authConfig.getPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY);
                final long validUntil = authConfig.getLong(CFG_KEY_VALID_UNTIL);
                setAuthentication(new DefaultAzureADAuthentication(accessToken, refreshToken, validUntil));
            } else {
                setAuthentication(null);
            }
            setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
            setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
            setTableName(settings.getString(CFG_KEY_TABLE_NAME));
            setOverwritePolicy(OverwritePolicy.valueOf(settings.getString(CFG_KEY_OVERWRITE_POLICY)));
        } catch (final InvalidSettingsException e) {
            // Leave defaults
        }
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        if (settings.containsKey(CFG_KEY_AUTHENTICATION)) {
            final Config authConfig = settings.getConfig(CFG_KEY_AUTHENTICATION);
            final String accessToken = authConfig.getPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY);
            final String refreshToken = authConfig.getPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY);
            final long validUntil = authConfig.getLong(CFG_KEY_VALID_UNTIL);
            setAuthentication(new DefaultAzureADAuthentication(accessToken, refreshToken, validUntil));
        } else {
            setAuthentication(null);
        }
        setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
        setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
        setTableName(settings.getString(CFG_KEY_TABLE_NAME));
        setOverwritePolicy(OverwritePolicy.valueOf(settings.getString(CFG_KEY_OVERWRITE_POLICY)));
    }
}
