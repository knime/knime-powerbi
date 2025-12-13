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
package org.knime.ext.powerbi.base.nodes.send2;

import static org.knime.node.impl.description.PortDescription.dynamicPort;
import static org.knime.node.impl.description.PortDescription.fixedPort;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.xmlbeans.XmlException;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ConfigurableNodeFactory;
import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeView;
import org.knime.core.node.context.NodeCreationConfiguration;
import org.knime.core.node.port.PortType;
import org.knime.core.util.Version;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultKaiNodeInterface;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterface;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterfaceFactory;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.node.impl.description.DefaultNodeDescriptionUtil;
import org.knime.node.impl.description.PortDescription;
import org.xml.sax.SAXException;

/**
 * Send to Power BI factory.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @author Bernd Wiswedel, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class SendToPowerBINodeFactory2 extends ConfigurableNodeFactory<SendToPowerBINodeModel2>
    implements NodeDialogFactory, KaiNodeInterfaceFactory {

    private static final String SHORT_DESCRIPTION = "Sends a table to Microsoft Power BI.";

    private static final String FULL_DESCRIPTION = """
        This node sends the input table to Microsoft Power BI.
        <p />
        The node only uploads columns that are supported by Power BI. Other columns are ignored. The supported types
        are: <i>String</i>, <i>Number (Integer)</i>, <i>Number (Long Integer)</i>, <i>Number (Float)</i>,
        <i>Boolean</i>, <i>Date</i>, and <i>Date&amp;Time (Local)</i>.
        <p />
        The node uploads rows in chunks to Microsoft Power BI. If the node is canceled, the already uploaded rows will
        remain in the Power BI dataset.
        <p />
        Use the <b>Microsoft Authenticator</b> node to connect to your Microsoft account.<br/>
        The KNIME Analytics Platform Azure Application needs the following permissions for this node:
        <ul>
            <li><b>View all datasets</b>: Needed to check if a dataset already exists in your Power BI workspace.
                </li>
            <li><b>Read and write all datasets</b>: Needed to upload a table to a Power BI dataset in your workspace.
                </li>
            <li><b>View all workspaces</b>: Needed to get the identifier of the selected Power BI workspace.
                </li>
            <li><b>Maintain access to data you have given it access to</b>: Needed to access the Power BI API during the
                node execution without asking you to log in again.</li>
        </ul>
        <b>Note:</b> The Power BI REST API has some limitations in terms of the size of a dataset and the number of rows
        that can be shipped per hour. For more information visit the
        <a href="https://docs.microsoft.com/en-us/power-bi/developer/api-rest-api-limitations">documentation</a>.
        """;

    @Override
    protected Optional<PortsConfigurationBuilder> createPortsConfigBuilder() {
        final PortsConfigurationBuilder b = new PortsConfigurationBuilder();
        b.addFixedInputPortGroup("auth", CredentialPortObject.TYPE);
        b.addExtendableInputPortGroup("input", new PortType[]{BufferedDataTable.TYPE}, BufferedDataTable.TYPE);
        return Optional.of(b);
    }

    @Override
    protected SendToPowerBINodeModel2 createNodeModel(final NodeCreationConfiguration creationConfig) {
        return new SendToPowerBINodeModel2(creationConfig.getPortConfig().get());
    }

    @Override
    protected NodeDialogPane createNodeDialogPane(final NodeCreationConfiguration creationConfig) {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<SendToPowerBINodeModel2> createNodeView(final int viewIndex,
        final SendToPowerBINodeModel2 nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, SendToPowerBINodeParameters.class);
    }

    @Override
    protected NodeDescription createNodeDescription() throws SAXException, IOException, XmlException {
        Collection<PortDescription> inPortDescriptions = List.of(//
            fixedPort("Credential (JWT)", "A JWT credential as provided by the Microsoft Authenticator node."), //
            fixedPort("Table", "Data to be sent to Power BI."), //
            dynamicPort("input", "Additional input table", "Additional data to be sent to your data set."));

        return DefaultNodeDescriptionUtil.createNodeDescription("Send to Power BI", //
            "send_to_power_bi.png", //
            inPortDescriptions, //
            List.of(), //
            SHORT_DESCRIPTION, //
            FULL_DESCRIPTION, //
            List.of(), // resources
            SendToPowerBINodeParameters.class, //
            List.of(), // view descriptions
            NodeType.Sink, //
            List.of(), // keywords
            new Version(4, 1, 0));
    }

    @Override
    public KaiNodeInterface createKaiNodeInterface() {
        return new DefaultKaiNodeInterface(Map.of(SettingsType.MODEL, SendToPowerBINodeParameters.class));
    }
}
