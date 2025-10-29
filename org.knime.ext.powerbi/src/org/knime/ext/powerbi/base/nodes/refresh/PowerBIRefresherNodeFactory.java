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
 *   Feb 5, 2016 (wiswedel): created
 */
package org.knime.ext.powerbi.base.nodes.refresh;

import java.io.IOException;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.credentials.base.CredentialPortObject;
import org.xml.sax.SAXException;

/**
 * Factory for the Power BI Refresher node.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class PowerBIRefresherNodeFactory
    extends ConfigurableNodeFactory<PowerBIRefresherNodeModel>
    implements NodeDialogFactory {

    private static final String FULL_DESCRIPTION = """
            <p>
                Refresh a Power BI Semantic Model to ensure that its data
                is up to date. The refresh is always executed as a transaction.
            </p>
            <p>
                The refreshing capabilities may be <a
                href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset#limitations">limited
                in some cases</a>.
            </p>
            <p>
                Canceling this node will also cancel the refresh.
            </p>
            <p>
            <b>Note:</b> The scopes required by this node have to be consented
            to at least once interactively by the user or workspace admin, especially for scopeless
            secrets. Please refer to the <a \
            href="https://docs.knime.com/latest/community_hub_secrets_guide/index.html\
            #secret_type_azure_scopes">documentation</a> to find out which scopes are required.
            </p>
            """;

    private static final String INPUT_PORT_GROUP = "Credential";

    private static final WebUINodeConfiguration CONFIG = WebUINodeConfiguration.builder()//
        .name("Power BI Model Refresher")//
        .icon("./power_bi_refresher.png") //
        .shortDescription("Refresh a Power BI Semantic Model") //
        .fullDescription(FULL_DESCRIPTION) //
        .modelSettingsClass(PowerBIRefresherNodeSettings.class) //
        .nodeType(NodeType.Sink)//
        .addInputPort(INPUT_PORT_GROUP, CredentialPortObject.TYPE,
            "Microsoft/Azure credential (access token)", false)//
        .keywords("Microsoft", "Power BI", "Semantic Model", "Dataset")
        .sinceVersion(5, 5, 0).build();

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        return WebUINodeFactory.createNodeDescription(CONFIG);
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, PowerBIRefresherNodeSettings.class);
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        // we do not want to use the dialog provided by the framework
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final var b = new PortsConfigurationBuilder();
        b.addFixedInputPortGroup(INPUT_PORT_GROUP, CredentialPortObject.TYPE);
        return Optional.of(b);
    }

    @Override
    protected PowerBIRefresherNodeModel createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new PowerBIRefresherNodeModel(
            creationConfig.getPortConfig().orElseThrow(), PowerBIRefresherNodeSettings.class);
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<PowerBIRefresherNodeModel> createNodeView(
        final int viewIndex, final PowerBIRefresherNodeModel nodeModel) {
        return null; // no views
    }

    @Override
    protected boolean hasDialog() {
        return false;
    }
}
