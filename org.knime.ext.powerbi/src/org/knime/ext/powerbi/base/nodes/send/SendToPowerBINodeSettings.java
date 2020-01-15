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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.config.Config;
import org.knime.core.node.config.base.ConfigBase;
import org.knime.core.util.FileUtil;
import org.knime.core.util.crypto.Encrypter;
import org.knime.core.util.crypto.IEncrypter;
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

    private static final String CFG_KEY_TABLE_NAMES = "table_name";

    private static final String CFG_KEY_DATASET_NAME_DIALOG = "dataset_name_dialog";

    private static final String CFG_KEY_TABLE_NAMES_DIALOG = "table_name_dialog";

    private static final String CFG_KEY_CREATE_NEW_DATASET = "create_new_dataset";

    private static final String CFG_KEY_ALLOW_OVERWRITE = "allow_overwrite";

    private static final String CFG_KEY_APPEND_ROWS = "append_rows";

    private static final String CFG_KEY_FILESYSTEM_LOCATION = "filesystem_location";

    private static final String CFG_KEY_CREDENTIALS_SAVE_LOCATION = "credentials_save_location";

    private static final String CFG_KEY_NODE_ID = "node_id";

    private static final String CFG_KEY_CREDENTIALS_PERSITED_MARKER = "credentials_persited_marker";

    private static final String LOCAL_FILE_ENCODING = "UTF-8";

    private static final String POWER_BI_CREDENTIAL_FILE_HEADER = "KNIME PowerBI Credentials";

    private AzureADAuthentication m_authentication;

    private String m_workspace = "";

    private String m_datasetName = "";

    private String[] m_tableNames = {""};

    /** The unused dataset name in the unselected dialog option */
    private String m_datasetNameDialog = "";

    /** The unused table names in the unselected dialog option */
    private String[] m_tableNamesDialog = {""};

    private boolean m_createNewDataset = true;

    private boolean m_allowOverwrite = false;

    private boolean m_appendRows = true;

    private String m_filesystemLocation = "";

    private CredentialsLocationType m_credentialsSaveLocation = CredentialsLocationType.MEMORY;

    private String m_nodeId = UUID.randomUUID().toString();

    /**
     * Binary marker to check which credentials location was already used for saving, i.e. we can expect that
     * credentials are present under the chosen location.
     *
     * 1 := MEMORY; 2 := FILESYSTEM; 4 := NODE
     *
     * Also see {@link #setMarker(CredentialsLocationType)} and {@link #wasPersited(CredentialsLocationType)}
     */
    private int m_credentialsPersitedMarker = 0;

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
        settings.addStringArray(CFG_KEY_TABLE_NAMES, getTableNames());
        settings.addString(CFG_KEY_DATASET_NAME_DIALOG, getDatasetNameDialog());
        settings.addStringArray(CFG_KEY_TABLE_NAMES_DIALOG, getTableNamesDialog());
        settings.addBoolean(CFG_KEY_CREATE_NEW_DATASET, m_createNewDataset);
        settings.addBoolean(CFG_KEY_ALLOW_OVERWRITE, m_allowOverwrite);
        settings.addBoolean(CFG_KEY_APPEND_ROWS, m_appendRows);

        settings.addString(CFG_KEY_NODE_ID, m_nodeId);
        settings.addString(CFG_KEY_FILESYSTEM_LOCATION, getFilesystemLocation());
        settings.addString(CFG_KEY_CREDENTIALS_SAVE_LOCATION, getCredentialsSaveLocation().getActionCommand());

        saveAuthentication(settings);
    }

    static void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // Note that the workspace config can be empty
        settings.getString(CFG_KEY_WORKSPACE);

        // Check the dataset name
        final String datasetName = settings.getString(CFG_KEY_DATASET_NAME);
        if (datasetName == null || datasetName.trim().isEmpty()
            || datasetName.equals(SendToPowerBINodeDialog.DATASET_PLACEHOLDER)) {
            throw new InvalidSettingsException("The dataset name must be set.");
        }

        // Check the table names
        final String[] tableNames = settings.getStringArray(CFG_KEY_TABLE_NAMES);
        checkTableNamesValid(tableNames);

        // Check selected location if chosen from radio buttons.
        if (CredentialsLocationType.fromActionCommand(
            settings.getString(CFG_KEY_CREDENTIALS_SAVE_LOCATION)) == CredentialsLocationType.FILESYSTEM) {
            File file = resolveFilesystemLocation(settings.getString(CFG_KEY_FILESYSTEM_LOCATION));

            if (file.exists() && !file.isFile()) {
                throw new InvalidSettingsException("Selected credetials storage location must be a file.");
            }
        }
    }

    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException, IOException {
        m_nodeId = settings.getString(CFG_KEY_NODE_ID);
        setFilesystemLocation(settings.getString(CFG_KEY_FILESYSTEM_LOCATION));
        setCredentialsSaveLocation(
            CredentialsLocationType.fromActionCommand(settings.getString(CFG_KEY_CREDENTIALS_SAVE_LOCATION)));
        setWorkspace(settings.getString(CFG_KEY_WORKSPACE));
        setDatasetName(settings.getString(CFG_KEY_DATASET_NAME));
        setTableNames(settings.getStringArray(CFG_KEY_TABLE_NAMES));
        setDatasetNameDialog(settings.getString(CFG_KEY_DATASET_NAME_DIALOG, ""));
        setTableNamesDialog(settings.getStringArray(CFG_KEY_TABLE_NAMES_DIALOG, ""));

        setCreateNewDataset(settings.getBoolean(CFG_KEY_CREATE_NEW_DATASET));
        setAllowOverwrite(settings.getBoolean(CFG_KEY_ALLOW_OVERWRITE));
        setAppendRows(settings.getBoolean(CFG_KEY_APPEND_ROWS, true));

        // Load the authentication last (in case it fails)
        loadAuthentication(settings);
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
    void clearAuthentication(final CredentialsLocationType locationType) throws InvalidSettingsException {

        switch (locationType) {
            case MEMORY:
                InMemoryCredentialStore.getInstance().remove(m_nodeId);
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());

                if (credentialsFile.exists()) {
                    // To make sure not to delete something on accident, only delete if the file has the magic header.
                    try {
                        checkCredentialFileHeader(credentialsFile);
                    } catch (InvalidCredentialsFileFormatException ex) {
                        throw new InvalidSettingsException(ex);
                    }

                    credentialsFile.delete();
                }
                break;

            case NODE:
                break;

            default:
                throw new NotImplementedException("Case " + locationType + " not yet implemented.");
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
                InMemoryCredentialStore.getInstance().put(m_nodeId, getAuthentication());
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());
                try {
                    saveCredentialsToFile(credentialsFile, getAuthentication());
                } catch (InvalidCredentialsFileFormatException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
                break;

            case NODE:
                final Config authConfig = settings.addConfig(CFG_KEY_AUTHENTICATION);
                authConfig.addPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY, getAuthentication().getAccessToken());
                authConfig.addPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY,
                    getAuthentication().getRefreshToken().orElse(null));
                authConfig.addLong(CFG_KEY_VALID_UNTIL, getAuthentication().getValidUntil());
                break;

            default:
                throw new NotImplementedException("Case " + m_credentialsSaveLocation + " not yet implemented.");
        }

        // Set marker indicating which location was used for storing
        setMarker(m_credentialsSaveLocation);
        settings.addInt(CFG_KEY_CREDENTIALS_PERSITED_MARKER, m_credentialsPersitedMarker);
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
        m_credentialsPersitedMarker = settings.getInt(CFG_KEY_CREDENTIALS_PERSITED_MARKER, 0);

        AzureADAuthentication credentials = null;

        switch (m_credentialsSaveLocation) {
            case MEMORY:
                credentials = InMemoryCredentialStore.getInstance().get(m_nodeId);
                if (wasPersited(m_credentialsSaveLocation) && credentials == null) {
                    throw new IOException(
                        "Could not load credentials from memory. Maybe KNIME was closed in the meantime?");
                }
                break;

            case FILESYSTEM:
                File credentialsFile = resolveFilesystemLocation(getFilesystemLocation());

                try {
                    if (credentialsFile.exists()) {
                        credentials = readCredentialsFromFile(credentialsFile);
                    } else if (wasPersited(m_credentialsSaveLocation)) {
                        throw new IOException("Could not load credentials from selected file: '"
                            + credentialsFile.toString() + "' File does not exist.");
                    }
                } catch (IOException ex) {
                    setAuthentication(null);
                    throw ex;
                } catch (InvalidCredentialsFileFormatException ex) {
                    setAuthentication(null);
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
                break;

            case NODE:
                if (!settings.containsKey(CFG_KEY_AUTHENTICATION)) {
                    break;
                }

                final Config authConfig = settings.getConfig(CFG_KEY_AUTHENTICATION);
                final String accessToken = authConfig.getPassword(CFG_KEY_ACCESS_TOKEN, ENCRYPTION_KEY);
                final String refreshToken = authConfig.getPassword(CFG_KEY_REFRESH_TOKEN, ENCRYPTION_KEY);
                final long validUntil = authConfig.getLong(CFG_KEY_VALID_UNTIL);
                credentials = new DefaultAzureADAuthentication(accessToken, validUntil, refreshToken);
                break;

            default:
                throw new NotImplementedException("Case " + m_credentialsSaveLocation + " not yet implemented.");
        }

        setAuthentication(credentials);
    }

    /**
     * Convenience function to set the bit corresponding to the specified credentials location.
     */
    private void setMarker(final CredentialsLocationType locationType) {

        switch (locationType) {

            case MEMORY:
                m_credentialsPersitedMarker = m_credentialsPersitedMarker | 1;
                break;
            case FILESYSTEM:
                m_credentialsPersitedMarker = m_credentialsPersitedMarker | 2;
                break;
            case NODE:
                m_credentialsPersitedMarker = m_credentialsPersitedMarker | 4;
                break;

            default:
                throw new NotImplementedException("Case " + locationType + " not yet implemented.");
        }
    }

    /**
     * Checks if the bit corresponding to the specified credentials location is set.
     */
    private boolean wasPersited(final CredentialsLocationType locationType) {
        switch (locationType) {

            case MEMORY:
                return (m_credentialsPersitedMarker & 1) == 1;
            case FILESYSTEM:
                return (m_credentialsPersitedMarker & 2) == 2;
            case NODE:
                return (m_credentialsPersitedMarker & 4) == 4;

            default:
                throw new NotImplementedException("Case " + locationType + " not yet implemented.");
        }

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
        throws IOException, InvalidCredentialsFileFormatException {

        if (saveLocation.exists()) {
            checkCredentialFileHeader(saveLocation);
        }

        String authTokens = POWER_BI_CREDENTIAL_FILE_HEADER + "\n";
        authTokens += encryptString(ENCRYPTION_KEY, auth.getAccessToken()) + "\n";

        if (auth.getRefreshToken().isPresent()) {
            authTokens += encryptString(ENCRYPTION_KEY, auth.getRefreshToken().get()) + "\n";
        } else {
            authTokens += "\n";
        }

        authTokens += auth.getValidUntil();

        try {
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
    private static AzureADAuthentication readCredentialsFromFile(final File loadLocation)
        throws IOException, InvalidCredentialsFileFormatException {
        AzureADAuthentication auth = null;
        try {
            checkCredentialFileHeader(loadLocation);

            List<String> lines = FileUtils.readLines(loadLocation, LOCAL_FILE_ENCODING);
            final String accessToken = decryptString(ENCRYPTION_KEY, lines.get(1));
            String refreshToken = lines.get(2);
            if (refreshToken.isEmpty()) {
                refreshToken = null;
            } else {
                refreshToken = decryptString(ENCRYPTION_KEY, refreshToken);
            }

            final long validUntil = Long.parseLong(lines.get(3));

            auth = new DefaultAzureADAuthentication(accessToken, validUntil, refreshToken);
        } catch (IOException ex) {
            throw new IOException("Can't read from selected credentials file. Reason: " + ex.getMessage(), ex);
        }
        return auth;
    }

    /**
     * Checks specified file for {@link #POWER_BI_CREDENTIAL_FILE_HEADER}.
     *
     * @param filesystemLocation The file to check.
     * @throws InvalidCredentialsFileFormatException If file can't be read or doesn't contain the header in its first
     *             line.
     */
    private static void checkCredentialFileHeader(final File filesystemLocation)
        throws InvalidCredentialsFileFormatException {
        try {
            String firstLine = Files.lines(filesystemLocation.toPath()).findFirst().orElse("");

            if (!firstLine.equals(POWER_BI_CREDENTIAL_FILE_HEADER)) {

                throw new InvalidCredentialsFileFormatException(
                    "Selected file seems not to be a valid KNIME PowerBI credentials file.");
            }
        } catch (Exception ex) {
            throw new InvalidCredentialsFileFormatException(
                "Selected file seems not to be a valid KNIME PowerBI credentials file.", ex);
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

    /**
     * Encryption functionality copied and adapted from {@link ConfigBase#addPassword(String, String, String)} and
     * {@link ConfigBase#getPassword(String, String, String)}.
     **/

    private static String encryptString(final String encryptionKey, final String value) {
        try {
            return createEncrypter(encryptionKey).encrypt(value, value == null ? 0 : value.length());
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException
                | InvalidAlgorithmParameterException ex) {
            throw new RuntimeException("Error while encrypting password: " + ex.getMessage(), ex);
        }
    }

    private static String decryptString(final String encryptionKey, final String value) {
        try {
            return createEncrypter(encryptionKey).decrypt(value);
        } catch (InvalidKeyException | BadPaddingException | IllegalBlockSizeException
                | InvalidAlgorithmParameterException | IOException ex) {
            throw new RuntimeException("Error while decrypting password: " + ex.getMessage(), ex);
        }
    }

    private static IEncrypter createEncrypter(final String key) {
        try {
            return new Encrypter(key);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeySpecException ex) {
            throw new RuntimeException("Could not create encrypter: " + ex.getMessage(), ex);
        }
    }

    /** Checks that no table name are valid. All set and none twice. */
    private static void checkTableNamesValid(final String[] tableNames) throws InvalidSettingsException {
        // Loop over names and check
        final Set<String> allNames = new HashSet<>();
        for (int i = 0; i < tableNames.length; i++) {
            if (tableNames[i] == null || tableNames[i].trim().isEmpty()
                || tableNames[i].equals(SendToPowerBINodeDialog.TABLE_PLACEHOLDER)) {
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
}
