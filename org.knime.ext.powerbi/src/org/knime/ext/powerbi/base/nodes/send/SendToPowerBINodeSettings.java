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
package org.knime.ext.powerbi.base.nodes.send;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;
import org.knime.core.util.FileUtil;
import org.knime.ext.azuread.auth.AzureADAuthentication;
import org.knime.ext.azuread.auth.DefaultAzureADAuthentication;

/**
 * Settings store managing all configurations required to send to data to PowerBI.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 */
final class SendToPowerBINodeSettings {

    private static final String ENCRYPTION_KEY = "9J4jG3m1v2FKmH9C5TffFw";

    private static final String CFG_KEY_AUTHENTICATION = "authentication";

    private static final String CFG_KEY_ACCESS_TOKEN = "access_token";

    private static final String CFG_KEY_REFRESH_TOKEN = "refresh_token";

    private static final String CFG_KEY_VALID_UNTIL = "valid_until";

    private static final String CFG_KEY_WORKSPACE = "workspace";

    private static final String CFG_KEY_DATASET_NAME = "dataset_name";

    private static final String CFG_KEY_TABLE_NAME = "table_name";

    private static final String CFG_KEY_OVERWRITE_POLICY = "overwrite_policy";

    private static final String CFG_KEY_FILESYSTEM_LOCATION = "filesystem_location";

    private static final String CFG_KEY_CREDENTIALS_SAVE_LOCATION = "credentials_save_location";

    private static final String CFG_KEY_NODE_ID = "node_id";

    private static final String LOCAL_FILE_ENCODING = "UTF-8";

    private static final String POWER_BI_CREDENTIAL_FILE_HEADER = "KNIME PowerBI Credentials";

    private AzureADAuthentication m_authentication;

    private String m_workspace = "";

    private String m_datasetName = "";

    private String m_tableName = "";

    private OverwritePolicy m_overwritePolicy = OverwritePolicy.ABORT;

    private String m_filesystemLocation = "";

    private CredentialsLocationType m_credentialsSaveLocation = CredentialsLocationType.MEMORY;

    private String m_nodeId = UUID.randomUUID().toString();

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

    /**
     * @return the filesystemLocation
     */
    String getFilesystemLocation() {
        return m_filesystemLocation;
    }

    /**
     * @param filesystemLocation the filesystemLocation to set
     */
    void setFilesystemLocation(final String filesystemLocation) {
        m_filesystemLocation = filesystemLocation;
    }

    /**
     * @return the credentialsSaveLocation
     */
    CredentialsLocationType getCredentialsSaveLocation() {
        return m_credentialsSaveLocation;
    }

    /**
     * @param credentialsSaveLocation the credentialsSaveLocation to set
     */
    void setCredentialsSaveLocation(final CredentialsLocationType credentialsSaveLocation) {
        m_credentialsSaveLocation = credentialsSaveLocation;
    }

    void saveSettingsTo(final NodeSettingsWO settings) throws IOException, InvalidSettingsException {
        settings.addString(CFG_KEY_WORKSPACE, getWorkspace());
        settings.addString(CFG_KEY_DATASET_NAME, getDatasetName());
        settings.addString(CFG_KEY_TABLE_NAME, getTableName());
        settings.addString(CFG_KEY_OVERWRITE_POLICY, getOverwritePolicy().name());

        settings.addString(CFG_KEY_NODE_ID, m_nodeId);
        settings.addString(CFG_KEY_FILESYSTEM_LOCATION, getFilesystemLocation());
        settings.addString(CFG_KEY_CREDENTIALS_SAVE_LOCATION, getCredentialsSaveLocation().getActionCommand());

        saveAuthentication(settings);
    }

    static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
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

        // Check selected location if chosen from radio buttons.
        if (CredentialsLocationType.fromActionCommand(settings.getString(CFG_KEY_CREDENTIALS_SAVE_LOCATION))
            .equals(CredentialsLocationType.FILESYSTEM)) {
            File file = resolveFilesystemLocation(settings.getString(CFG_KEY_FILESYSTEM_LOCATION));

            if (file.exists()) {
                if (!file.isFile()) {
                    throw new InvalidSettingsException("Selected credetials storage location must be a file.");
                }
            }
        }
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException, IOException {
        m_nodeId = settings.getString(CFG_KEY_NODE_ID);
        setFilesystemLocation(settings.getString(CFG_KEY_FILESYSTEM_LOCATION));
        setCredentialsSaveLocation(
            CredentialsLocationType.fromActionCommand(settings.getString(CFG_KEY_CREDENTIALS_SAVE_LOCATION)));

        loadAuthentication(settings);

        setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
        setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
        setTableName(settings.getString(CFG_KEY_TABLE_NAME));
        setOverwritePolicy(OverwritePolicy.valueOf(settings.getString(CFG_KEY_OVERWRITE_POLICY)));
    }

    /**
     * Clear the stored credentials of specified type. Authentication will always be set to null after this call.
     *
     * @param locationType The type of credentials to clear.
     * @throws IOException If {@link CredentialsLocationType#FILESYSTEM} and the file does not conform expected
     *             specification. Also see {@link #checkCredentialFileHeader(File)}.
     * @throws InvalidSettingsException If {@link CredentialsLocationType#FILESYSTEM} and the selected file path can't
     *             be resolved.
     */
    void clearAuthentication(final CredentialsLocationType locationType) throws IOException, InvalidSettingsException {

        switch (locationType) {
            case MEMORY:
                InMemoryCredentialStore.instance().remove(m_nodeId);
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());

                if (credentialsFile.exists()) {
                    // To make sure not to delete something on accident, only delete if the file has the magic header.
                    checkCredentialFileHeader(credentialsFile);

                    credentialsFile.delete();
                }
                break;

            case NODE:
                break;

            default:
                break;
        }

        setAuthentication(null);
    }

    /**
     * Save the authentication to the selected credentials location.
     *
     * @param settings Setting to save authentication to if {@link CredentialsLocationType#FILESYSTEM}.
     * @throws IOException If {@link CredentialsLocationType#FILESYSTEM} and the credentials can't be saved to file.
     * @throws InvalidSettingsException If {@link CredentialsLocationType#FILESYSTEM} and the selected file path can't
     *             be resolved.
     */
    private void saveAuthentication(final NodeSettingsWO settings) throws IOException, InvalidSettingsException {

        if (getAuthentication() == null) {
            return;
        }

        switch (m_credentialsSaveLocation) {
            case MEMORY:
                InMemoryCredentialStore.instance().put(m_nodeId, getAuthentication());
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());
                saveCredentialsToFile(credentialsFile, getAuthentication());
                break;

            case NODE:
                final Config authConfig = settings.addConfig(CFG_KEY_AUTHENTICATION);
                authConfig.addPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY, getAuthentication().getAccessToken());
                authConfig.addPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY,
                    getAuthentication().getRefreshToken().orElseGet(() -> null));
                authConfig.addLong(CFG_KEY_VALID_UNTIL, getAuthentication().getValidUntil());

            default:
                break;
        }
    }

    /**
     * Load the authentication from the selected credentials location.
     *
     * @param settings Setting to load authentication from if {@link CredentialsLocationType#FILESYSTEM}.
     * @throws IOException If {@link CredentialsLocationType#FILESYSTEM} and the credentials can't be loaded from file.
     * @throws InvalidSettingsException If {@link CredentialsLocationType#FILESYSTEM} and the selected file path can't
     *             be resolved.
     */
    private void loadAuthentication(final NodeSettingsRO settings) throws IOException, InvalidSettingsException {

        AzureADAuthentication credentials = null;

        switch (m_credentialsSaveLocation) {
            case MEMORY:
                credentials = InMemoryCredentialStore.instance().get(m_nodeId);
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());
                try {
                    credentials = readCredentialsFromFile(credentialsFile);
                } catch (IOException ex) {
                    setAuthentication(null);
                    throw ex;
                }
                break;

            case NODE:
                if (!settings.containsKey(CFG_KEY_AUTHENTICATION)) {
                    setAuthentication(null);
                    return;
                }

                final Config authConfig = settings.getConfig(CFG_KEY_AUTHENTICATION);
                final String accessToken = authConfig.getPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY);
                final String refreshToken = authConfig.getPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY);
                final long validUntil = authConfig.getLong(CFG_KEY_VALID_UNTIL);
                credentials = new DefaultAzureADAuthentication(accessToken, refreshToken, validUntil);

            default:
                break;
        }

        setAuthentication(credentials);
    }

    /**
     * Attempts to save the specified credentials to the specified file.
     *
     * @param saveLocation The file to save to. Will be created if not yet existing, otherwise overwritten.
     * @param auth The authentication to save.
     * @throws IOException If the file exists and does not conform expected specification. Also see
     *             {@link #checkCredentialFileHeader(File)}. Or file can't be written.
     */
    private static void saveCredentialsToFile(final File saveLocation, final AzureADAuthentication auth)
        throws IOException {
        String authTokens = POWER_BI_CREDENTIAL_FILE_HEADER + "\n";
        authTokens += auth.getAccessToken() + "\n";
        authTokens += auth.getRefreshToken().orElseGet(() -> "") + "\n";
        authTokens += auth.getValidUntil();

        try {
            if (saveLocation.exists()) {
                checkCredentialFileHeader(saveLocation);
            }

            FileUtils.writeStringToFile(saveLocation, authTokens, LOCAL_FILE_ENCODING);
        } catch (IOException ex) {
            throw new IOException("Can't write to selected credentials file. Reason: " + ex.getMessage(), ex);
        }
    }

    /**
     * Attempts to read credentials from the specified file.
     *
     * @param loadLocation The file to read from.
     * @return The read credentials.
     * @throws IOException If the file exists and does not conform expected specification. Also see
     *             {@link #checkCredentialFileHeader(File)}. Or file can't be read.
     */
    private static AzureADAuthentication readCredentialsFromFile(final File loadLocation) throws IOException {
        AzureADAuthentication auth = null;
        try {
            if (!loadLocation.exists()) {
                return null;
            }

            checkCredentialFileHeader(loadLocation);

            List<String> lines = FileUtils.readLines(loadLocation, LOCAL_FILE_ENCODING);
            final String accessToken = lines.get(1);
            String refreshToken = lines.get(2);
            if (refreshToken.isEmpty()) {
                refreshToken = null;
            }
            final long validUntil = Long.parseLong(lines.get(3));

            auth = new DefaultAzureADAuthentication(accessToken, refreshToken, validUntil);
        } catch (IOException ex) {
            throw new IOException("Can't read from selected credentials file. Reason: " + ex.getMessage(), ex);
        }
        return auth;
    }

    /**
     * Checks specified file for {@link #POWER_BI_CREDENTIAL_FILE_HEADER}.
     *
     * @param filesystemLocation The file to check.
     * @throws IOException If file can't be read or doesn't contain the header in its first line.
     */
    private static void checkCredentialFileHeader(final File filesystemLocation) throws IOException {
        String firstLine = "";
        try {
            firstLine = Files.lines(filesystemLocation.toPath()).findFirst().get();
        } catch (Exception ex) {
            throw new IOException("Selected file seems not to be a valid KNIME PowerBI credentials file.", ex);
        }

        if (!firstLine.equals(POWER_BI_CREDENTIAL_FILE_HEADER)) {
            throw new IOException("Selected file seems not to be a valid KNIME PowerBI credentials file.");
        }
    }

    /**
     * Attempts to resolve the given String to File, also resolving the KNIME protocol.
     *
     * @param filesystemLocation The String to resolve.
     * @return The file corresponding to the given String.
     * @throws InvalidSettingsException If the given String can't be resolved.
     */
    private static File resolveFilesystemLocation(final String filesystemLocation) throws InvalidSettingsException {
        Path resolvedPath;
        if (filesystemLocation.isEmpty()) {
            throw new InvalidSettingsException("Local file path must not be empty.");
        }
        try {
            resolvedPath = FileUtil.resolveToPath(FileUtil.toURL(filesystemLocation));
            if (resolvedPath == null) {
                throw new InvalidSettingsException("Not a valid local file path: '" + filesystemLocation + "'.");
            }
        } catch (IOException | URISyntaxException | InvalidPathException e) {
            throw new InvalidSettingsException("Not a valid local file path: '" + filesystemLocation + "'.", e);
        }
        return new File(resolvedPath.toUri());
    }
}
