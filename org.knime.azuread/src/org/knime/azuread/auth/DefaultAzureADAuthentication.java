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
package org.knime.azuread.auth;

import java.util.Date;
import java.util.Optional;

/**
 * The default implementation of the {@link AzureADAuthentication} that just gets the access and refresh token in the
 * constructor.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class DefaultAzureADAuthentication implements AzureADAuthentication {

    private final String m_accessToken;

    private final Optional<String> m_refreshToken;

    private final long m_validUntil;

    /**
     * Create a new default {@link AzureADAuthentication} with the given tokens.
     *
     * @param accessToken the access token
     * @param refreshToken the refresh token. Can be <code>null</code>.
     * @param validUntil the date until the access token is valid (in milliseconds from 1. January 1970)
     */
    public DefaultAzureADAuthentication(final String accessToken, final String refreshToken, final long validUntil) {
        m_accessToken = accessToken;
        m_refreshToken = Optional.ofNullable(refreshToken);
        m_validUntil = validUntil;
    }

    /**
     * Create a new default {@link AzureADAuthentication} with the given tokens.
     *
     * @param accessToken the access token
     * @param refreshToken the refresh token. Can be <code>null</code>.
     * @param validUntil the date until the access token is valid
     */
    public DefaultAzureADAuthentication(final String accessToken, final String refreshToken, final Date validUntil) {
        this(accessToken, refreshToken, validUntil.getTime());
    }

    @Override
    public String getAccessToken() {
        return m_accessToken;
    }

    @Override
    public Optional<String> getRefreshToken() {
        return m_refreshToken;
    }

    @Override
    public long getValidUntil() {
        return m_validUntil;
    }
}
