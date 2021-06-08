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
 *   11.11.2019 (David Kolb, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.ext.powerbi.base.nodes.send;

import java.util.HashMap;
import java.util.Map;

import org.knime.ext.azuread.auth.AzureADAuthentication;

/**
 * Singleton HashMap to store AzureADAuthentication credentials.
 *
 * @author David Kolb, KNIME GmbH, Konstanz, Germany
 * @deprecated
 */
@Deprecated
final class InMemoryCredentialStore {

    private static InMemoryCredentialStore m_instance;

    private Map<String, AzureADAuthentication> m_dataStore = new HashMap<String, AzureADAuthentication>();

    private InMemoryCredentialStore() {
    }

    /**
     * Get the single InMemoryCredentialStore instance.
     *
     * @return The singleton InMemoryCredentialStore instance.
     */
    public static InMemoryCredentialStore getInstance() {
        if (m_instance == null) {
            m_instance = new InMemoryCredentialStore();
        }
        return m_instance;
    }

    /**
     * Store the specified AzureADAuthentication with specified key.
     *
     * @param key The key to use.
     * @param value The AzureADAuthentication to store.
     */
    public void put(final String key, final AzureADAuthentication value) {
        m_dataStore.put(key, value);
    }

    /**
     * Get the AzureADAuthentication associated with the specified key.
     *
     * @param key The key to get the value of.
     * @return The AzureADAuthentication for the specified key or null if no mapping is available.
     */
    public AzureADAuthentication get(final String key) {
        return m_dataStore.get(key);
    }

    /**
     * Remove the AzureADAuthentication with specified key.
     *
     * @param key The key to remove the value of.
     */
    public void remove(final String key) {
        m_dataStore.remove(key);
    }
}
