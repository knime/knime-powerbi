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
 *   02.06.2025 (loescher): created
 */
package org.knime.ext.powerbi.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.knime.base.node.io.filehandling.webui.ReferenceStateProvider;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.NodeLogger;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings.DefaultNodeSettingsContext;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoice;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.StringChoicesProvider;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;

/**
 * Common modern UI dialog components for Power BI nodes
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class NodeDialogCommon {

    private NodeDialogCommon() {
        throw new UnsupportedOperationException("Utility class");
    }

    /**
     * A provider for Workspace-ids, <code>null</code> meaning the default
     * “My Workspace”-workspace in accordance with {@link PowerBIRestAPIUtils}.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    public static class WorkspaceChoicesProvider implements StringChoicesProvider {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final DefaultNodeSettingsContext context) {
            final var result = new LinkedList<StringChoice>();
            result.add(new StringChoice("\t", "My Workspace"));

            try {
                final var cred = (CredentialPortObjectSpec)context.getPortObjectSpec(0)
                        .orElseThrow(NoSuchCredentialException::new);
                final AuthTokenProvider auth = PowerBICredentialUtil.toAccessTokenAccessor(cred)::getAccessToken;
                Arrays.stream(PowerBIRestAPIUtils.getGroups(auth, null).getValue())
                    .map(g -> new StringChoice(g.getId(), g.getName()))
                    .forEach(result::add); // NOSONAR Allow (fallback) default
            } catch (NoSuchCredentialException | IOException ex) {
                NodeLogger.getLogger(getClass()).debug("Could not authenticate: " + ex.getMessage(), ex);
            } catch (PowerBIResponseException | CanceledExecutionException ex) {
                NodeLogger.getLogger(getClass()).debug("Could not fetch groups: " + ex.getMessage(), ex);
            }

            return result;
        }

    }

    /**
     * A provider for Dataset/Semantic Model-ids..
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    public static class DatasetChoicesProvider implements StringChoicesProvider {

        private Supplier<String> m_workspace;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
            m_workspace = initializer.computeFromValueSupplier(WorkspaceRef.class);
        }

        @Override
        public List<StringChoice> computeState(final DefaultNodeSettingsContext context) {
            try {
                final var raw = m_workspace.get();
                if (raw == null)  {
                    return List.of();
                }
                // API wants null for My Workspace but UI maps that to unselected
                final var workspace = Optional.of(raw).filter(Predicate.not(String::isBlank)).orElse(null);
                final var cred = (CredentialPortObjectSpec)context.getPortObjectSpec(0)
                        .orElseThrow(NoSuchCredentialException::new);
                final AuthTokenProvider auth = PowerBICredentialUtil.toAccessTokenAccessor(cred)::getAccessToken;
                return Arrays.stream(PowerBIRestAPIUtils.getDatasets(auth, workspace, null).getValue())
                    .map(d -> new StringChoice(d.getId(), d.getName()))
                    .toList();
            } catch (NoSuchCredentialException | IOException ex) {
                NodeLogger.getLogger(getClass()).debug("Could not authenticate: " + ex.getMessage(), ex);
                // Do not fail
                return List.of();
            } catch (PowerBIResponseException | CanceledExecutionException ex) {
                NodeLogger.getLogger(getClass()).debug("Could not fetch Semantic Models: " + ex.getMessage(), ex);
                // Maybe we do not have access (e.g. App tries to read My Workspace)
                // Do not fail
                return List.of();
            }
        }

    }

    /** Reference to a field containing a workspace id */
    public static class WorkspaceRef extends ReferenceStateProvider<String> {
    }

    /** Reference to a field containing a dataset id */
    public static class DatasetRef extends ReferenceStateProvider<String> {
    }


}
