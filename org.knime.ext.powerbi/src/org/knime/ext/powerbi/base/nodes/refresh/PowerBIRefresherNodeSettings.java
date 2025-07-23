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
package org.knime.ext.powerbi.base.nodes.refresh;

import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.ext.powerbi.util.NodeDialogCommon.DatasetChoicesProvider;
import org.knime.ext.powerbi.util.NodeDialogCommon.DatasetRef;
import org.knime.ext.powerbi.util.NodeDialogCommon.WorkspaceChoicesProvider;
import org.knime.ext.powerbi.util.NodeDialogCommon.WorkspaceRef;
import org.knime.ext.powerbi.util.PowerBICredentialUtil;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.array.ArrayWidget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.Label;
import org.knime.node.parameters.widget.message.TextMessage;
import org.knime.node.parameters.widget.message.TextMessage.MessageType;
import org.knime.node.parameters.widget.message.TextMessage.SimpleTextMessageProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;

/**
 * Settings store managing all configurations required for the node.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
public final class PowerBIRefresherNodeSettings
    implements NodeParameters {

    @Section(title = "Semantic Model")
    interface DatasetSection {
    }

    @Section(title = "Refresh")
    @Advanced
    interface RefreshSection {
    }

    @Section(title = "Timeouts")
    @Advanced
    interface TimeoutsSection {
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

    @Widget(title = "Type",
            description = """
                    The type of refresh to perform. The refresh types are documented
                    <a href="https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-refresh-execution-details#datasetrefreshdetailtype">here</a>""") // NOSONAR
    @Layout(RefreshSection.class)
    Type m_type = Type.CALCULATE;

    @TextMessage(NoTablesSpecifiedMessage.class)
    @Layout(RefreshSection.class)
    Void m_message;

    @Widget(title = "Tables",
            description = "The tables to refresh. If no elements are provided, all tables will be refreshed.")
    @Layout(RefreshSection.class)
    @ArrayWidget(elementTitle = "Table", addButtonText = "Add Table")
    @ValueReference(TablesRef.class)
    TableEntry[] m_tables = new TableEntry[] {};


    @Widget(title = "Refresh timeout (minutes)",
            description = """
                    How long to wait for the refresh operation to complete in minutes.
                    The refresh in Power BI will be cancelled if it takes longer.
                    """)
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class, maxValidation = MaxTimeOut.class)
    @Layout(TimeoutsSection.class)
    long m_timeout = 3L * 60L;


    enum Type {
        @Label(value = "Automatic", description = "Refresh the data and recalculate dependencies only if needed.")
        AUTOMATIC,
        @Label(value = "Calculate", description = "Recalculates the data only if needed.")
        CALCULATE,
        @Label(value = "Data Only", description = "Refresh the data and clear dependencies.")
        DATA_ONLY,
        @Label(value = "Defragment",
               description = "Defragment the data by removing data that has been removed from the columns.")
        DEFRAGMENT,
        @Label(value = "Full", description = "Refresh the data and recalculate the dependencies every time.")
        FULL,
        @Label(value = "Clear Values", description = "Clears all values in the table(s) and their dependencies!")
        CLEAR_VALUES;
    }

    static class TableEntry implements NodeParameters {
        @Widget(title = "Table",
                description = "Name of the table to refresh.")
        String m_table = "";
        @Widget(title = "Partition",
                description = "Name of the table to refresh.")
        String m_partition = "";

    }

    static class NoTablesSpecifiedMessage implements SimpleTextMessageProvider {

        private Supplier<TableEntry[]> m_tables;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeBeforeOpenDialog();
            m_tables = initializer.computeFromValueSupplier(TablesRef.class);
        }

        @Override
        public boolean showMessage(final NodeParametersInput context) {
            return m_tables.get().length == 0;
        }

        @Override
        public String title() {
            return "All tables will be refreshed";
        }

        @Override
        public String description() {
            return "If no specific tables are specified, all of them will be refreshed.";
        }

        @Override
        public MessageType type() {
            return MessageType.INFO;
        }

    }

    static class MaxTimeOut extends NumberInputWidgetValidation.MaxValidation {

        @Override
        protected double getMax() {
            return PowerBIRefresherNodeModel.MAX_TIMEOUT_MINUTES;
        }

    }

    static class TablesRef implements ParameterReference<TableEntry[]> {
    }


    void validate(final PortObjectSpec cred) throws InvalidSettingsException {
        PowerBICredentialUtil.validateCredentialOnConfigure((CredentialPortObjectSpec) cred);

        CheckUtils.checkSetting(!StringUtils.isEmpty(m_workspaceId), "Please specify a workspace.");
        CheckUtils.checkSetting(!StringUtils.isBlank(m_dataset), "Please specify a semantic model.");
        CheckUtils.checkSetting(m_timeout >= 0, "Please specify a non-negative timeout.");
        CheckUtils.checkSetting(m_timeout <= PowerBIRefresherNodeModel.MAX_TIMEOUT_MINUTES,
                "Please specify a timeout of at most 24 hours.");
    }

}
