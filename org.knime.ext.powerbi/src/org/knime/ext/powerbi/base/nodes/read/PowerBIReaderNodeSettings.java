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
 *  This program is distributed in the hope that it will be useful, but.
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
 *   2024-06-04 (jloescher): created
 */
package org.knime.ext.powerbi.base.nodes.read;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeSettings;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Layout;
import org.knime.core.webui.node.dialog.defaultdialog.layout.Section;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Label;
import org.knime.core.webui.node.dialog.defaultdialog.widget.TextAreaWidget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.ValueSwitchWidget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.Widget;
import org.knime.core.webui.node.dialog.defaultdialog.widget.choices.ChoicesProvider;
import org.knime.core.webui.node.dialog.defaultdialog.widget.updates.ValueReference;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.ext.powerbi.util.NodeDialogCommon.DatasetChoicesProvider;
import org.knime.ext.powerbi.util.NodeDialogCommon.DatasetRef;
import org.knime.ext.powerbi.util.NodeDialogCommon.WorkspaceChoicesProvider;
import org.knime.ext.powerbi.util.NodeDialogCommon.WorkspaceRef;
import org.knime.ext.powerbi.util.PowerBICredentialUtil;

/**
 * Settings store managing all configurations required for the node.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class PowerBIReaderNodeSettings
    implements DefaultNodeSettings {

    @Section(title = "Semantic Model")
    interface DatasetSection {
    }

    @Section(title = "Query")
    interface QuerySection {
    }

    @Widget(title = "Workspace",
            description = """
                    The workspace which contains the Semantic Models.
                    The first option, “My Workspace”, always refers to the private user workspace.
                    (The workspace is referenced by its ID.)""")
    @ChoicesProvider(WorkspaceChoicesProvider.class)
    @ValueReference(WorkspaceRef.class)
    @Layout(DatasetSection.class)
    String m_workspaceId;

    @Widget(title = "Semantic model",
            description = "The Semantic model (also known as dataset) to read. (The model is referenced by its ID.)")
    @ChoicesProvider(DatasetChoicesProvider.class)
    @ValueReference(DatasetRef.class)
    @Layout(DatasetSection.class)
    String m_dataset;

    @Widget(title = "DAX query",
            description = """
                The <a href="https://learn.microsoft.com/en-us/dax/dax-queries">DAX query</a> to evaluate.
                See node description for limitations regarding output size.
                The query
                <pre>
                EVALUATE
                    'table_name';
                </pre>
                can be used to just read a table with a given name.
                The table name can for example be found in the Power BI (Web) interface.
                """)
    @TextAreaWidget
    @Layout(QuerySection.class)
    String m_daxQuery = """
            EVALUATE
                'table';""";

    @Widget(title = "If returned data was limited because of result size",
            description = """
                Power BI limits the result size of DAX queries (see
                <a href="learn.microsoft.com/en-us/rest/api/power-bi/datasets/execute-queries#limitations">here</a>).
                This setting determines how to handle a truncated query result due these limits or other errors.
                """)
    @ValueSwitchWidget
    @Layout(QuerySection.class)
    ErrorHandling m_queryErrorHandling = ErrorHandling.FAIL;

    enum ErrorHandling {
        @Label(value = "Fail",
                description = "Fail node execution")
        FAIL,
        @Label(value = "Warn",
                description = "Set a node warning")
        WARN,
        @Label(value = "Ignore",
                description = "Do nothing")
        IGNORE;
    }


    void validate(final PortObjectSpec cred) throws InvalidSettingsException {
        PowerBICredentialUtil.validateCredentialOnConfigure((CredentialPortObjectSpec)cred);

        CheckUtils.checkSetting(!StringUtils.isEmpty(m_workspaceId), "Please specify a workspace!");
        CheckUtils.checkSetting(!StringUtils.isBlank(m_dataset), "Please specify a semantic model!");
        CheckUtils.checkSetting(!StringUtils.isBlank(m_daxQuery), "Please specify a DAX query!");
    }

}
