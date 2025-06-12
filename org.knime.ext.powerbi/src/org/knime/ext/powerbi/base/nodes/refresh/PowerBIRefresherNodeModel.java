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
package org.knime.ext.powerbi.base.nodes.refresh;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.PowerBIResponseException;
import org.knime.ext.powerbi.core.rest.bindings.Refresh;
import org.knime.ext.powerbi.core.rest.bindings.Refresh.ObjectRefreshDefinition;
import org.knime.ext.powerbi.util.PowerBICredentialUtil;

/**
 * Power BI Reader node model.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
final class PowerBIRefresherNodeModel extends WebUINodeModel<PowerBIRefresherNodeSettings> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PowerBIRefresherNodeModel.class);

    static final long MAX_TIMEOUT_MINUTES = 24L * 60L;

    public PowerBIRefresherNodeModel(final PortsConfiguration portsConfig,
        final Class<PowerBIRefresherNodeSettings> settings) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), settings);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final PowerBIRefresherNodeSettings modelSettings) throws InvalidSettingsException {
        modelSettings.validate(inSpecs[0]);
        return new PortObjectSpec[0];
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final PowerBIRefresherNodeSettings settings) throws Exception {
        settings.validate(inObjects[0].getSpec());

        final var cred = (CredentialPortObjectSpec)inObjects[0].getSpec();
        final AuthTokenProvider auth = PowerBICredentialUtil.toAccessTokenAccessor(cred)::getAccessToken;

        exec.setMessage("Requesting refresh");
        final var workspace = Optional.of(settings.m_workspaceId).filter(Predicate.not(String::isBlank)).orElse(null);
        final var refresh = createRefresh(settings);
        final var refreshId =
            PowerBIRestAPIUtils.refreshDataset(auth, workspace, settings.m_dataset, refresh, exec);

        exec.setMessage("Waiting for refresh to finish");
        waitRefreshed(auth, exec, settings, workspace, refreshId);

        return new PortObject[0];
    }

    private static Refresh createRefresh(final PowerBIRefresherNodeSettings settings) { // NOSONAR switch is nicer
        final var type = switch (settings.m_type) {
            case AUTOMATIC -> Refresh.Type.Automatic;
            case CALCULATE -> Refresh.Type.Calculate;
            case CLEAR_VALUES -> Refresh.Type.ClearValues;
            case DATA_ONLY -> Refresh.Type.DataOnly;
            case DEFRAGMENT -> Refresh.Type.Defragment;
            case FULL -> Refresh.Type.Full;

        };

        final var timeout = String.format("%d:%02d:00", settings.m_timeout / 60, settings.m_timeout % 60);

        final var objects = new LinkedList<ObjectRefreshDefinition>();

        // remove every object if both fields are empty
        for (final var tab : settings.m_tables) {
            final var table = emptyToNull(tab.m_table);
            final var partition = emptyToNull(tab.m_partition);
            if (table != null || partition != null) {
                objects.add(new ObjectRefreshDefinition(table, partition));
            }
        }

        // do not add objects if none given
        return new Refresh(type, timeout, objects.isEmpty() ? null : objects);
    }

    private static String emptyToNull(final String input) {
        if (StringUtils.isEmpty(input)) {
            return null;
        }
        return input;
    }

    private void waitRefreshed(final AuthTokenProvider auth, final ExecutionContext exec,
        final PowerBIRefresherNodeSettings settings, final String workspaceId, final String refreshId)
        throws IOException, PowerBIResponseException, CanceledExecutionException {

        var end = System.currentTimeMillis() + settings.m_timeout * 60000;

        try {
            // for the first 10 seconds check more frequently
            for (var i = 0; i < 10 && System.currentTimeMillis() < end; ++i) {
                exec.checkCanceled();
                Thread.sleep(1000);
                var refresh = PowerBIRestAPIUtils
                        .getDatasetRefreshStatus(auth, workspaceId, settings.m_dataset, refreshId, exec);
                if (isRefreshFinished(refresh)) {
                    handleMessages(refresh).ifPresent(this::setWarningMessage);
                    return;
                }
            }

            // then every 5 seconds
            while (System.currentTimeMillis() < end) {
                exec.checkCanceled();
                Thread.sleep(5000);
                var refresh = PowerBIRestAPIUtils
                        .getDatasetRefreshStatus(auth, workspaceId, settings.m_dataset, refreshId, exec);
                if (isRefreshFinished(refresh)) {
                    handleMessages(refresh).ifPresent(this::setWarningMessage);
                    return;
                }
            }

        } catch (CanceledExecutionException | InterruptedException ex) { // NOSONAR content not interesting
            Thread.currentThread().interrupt();
            cancelRefresh(auth, exec, settings, workspaceId, refreshId);
            throw new CanceledExecutionException(ex.getMessage());
        }
        throw new IOException("Timeout while waiting for refresh to finish");
    }

    private static boolean isRefreshFinished(final Refresh refresh) throws IOException {
        return switch (refresh.getExtendedStatus()) {
            case Completed -> true;
            case InProgress, NotStarted, Unknown -> false;
            default -> throw new IOException("Refresh " + refresh.getExtendedStatus()
                    + handleMessages(refresh).map(s -> " (" + s + ")").orElse(""));
        };
    }

    private static Optional<String> handleMessages(final Refresh refresh) {
        final var messages = refresh.getMessages();
        if (messages == null || messages.isEmpty()) {
            return Optional.empty();
        }
        if (messages.size() == 1) {
            return Optional.of(messages.get(0).toString());
        }
        return Optional.of(messages.toString());
    }

    private static void cancelRefresh(final AuthTokenProvider auth, final ExecutionContext exec,
        final PowerBIRefresherNodeSettings settings, final String workspaceId, final String refreshId) {
        try {
            PowerBIRestAPIUtils.cancelDatasetRefresh(auth, workspaceId, settings.m_dataset, refreshId, exec);
        } catch (PowerBIResponseException | CanceledExecutionException e) {
            // nothing to do but log
            LOGGER.error("Could not cancel refresh: " + e.getMessage(), e);
        }
    }

}
