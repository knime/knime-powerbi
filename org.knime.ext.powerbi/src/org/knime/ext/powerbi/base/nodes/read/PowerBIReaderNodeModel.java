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
package org.knime.ext.powerbi.base.nodes.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.util.Pair;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils;
import org.knime.ext.powerbi.core.rest.PowerBIRestAPIUtils.AuthTokenProvider;
import org.knime.ext.powerbi.core.rest.bindings.QueryResults.Result;
import org.knime.ext.powerbi.core.rest.bindings.QueryResults.Result.Error;
import org.knime.ext.powerbi.core.rest.bindings.QueryResults.Result.Table;
import org.knime.ext.powerbi.util.PowerBICredentialUtil;

/**
 * Power BI Reader node model.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction") // New Node UI is not yet API
final class PowerBIReaderNodeModel extends WebUINodeModel<PowerBIReaderNodeSettings> {

    private static final long MAX_ROWS_SCANNED = 10_000; // from CSV reader

    public PowerBIReaderNodeModel(final PortsConfiguration portsConfig,
        final Class<PowerBIReaderNodeSettings> settings) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), settings);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final PowerBIReaderNodeSettings modelSettings)
        throws InvalidSettingsException {
        modelSettings.validate(inSpecs[0]);
        // we do not know the result of the query without executing it; oh well...
        return new PortObjectSpec[1];
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final PowerBIReaderNodeSettings settings) throws Exception {
        settings.validate(inObjects[0].getSpec());

        final var cred = (CredentialPortObjectSpec)inObjects[0].getSpec();
        final AuthTokenProvider auth = PowerBICredentialUtil.toAccessTokenAccessor(cred)::getAccessToken;


        final var workspace = Optional.of(settings.m_workspaceId).filter(Predicate.not(String::isBlank)).orElse(null);
        exec.setMessage("Executing query");
        final var result = PowerBIRestAPIUtils.executeDAXQuery(
            auth, workspace, settings.m_dataset, settings.m_daxQuery, exec);

        handleError(result, settings);
        checkInput(result);

        exec.setMessage("Parsing spec");

        final var spec = guessSpec(result.tables()[0]);

        exec.setMessage("Writing table");

        return new PortObject[] { createTable(spec.getFirst(), spec.getSecond(), result.tables()[0], exec) };
    }

    private void handleError(final Result result, final PowerBIReaderNodeSettings settings)
            throws InvalidSettingsException, IOException {
        if (result == null) {
            throw new IOException("No data or unexpected format returned.");
        }
        if (result.error() != null) {
            final var error = Optional.ofNullable(result).map(Result::error);

            var message = error.map(Error::message).map(s -> ": " + s).orElse("");
            message += error.map(Error::code).map(s -> " (Code: " + s + ")").orElse("");

            switch (settings.m_queryErrorHandling) {
                case FAIL -> throw new InvalidSettingsException("Data may be missing, "
                    + "not continuing due to user settings" + message);
                case WARN -> setWarningMessage("Data may be missing" + message);
                case IGNORE -> { /* do nothing */}
            }
        }
    }

    private static void checkInput(final Result result) {
        if (result.tables() == null || result.tables().length == 0) {
            throw new IllegalStateException("No tables in response.");
        }
        if (result.tables().length > 1) {
            throw new IllegalStateException("More than one table in response.");
        }
    }

    private static Pair<DataTableSpec, List<String>> guessSpec(final Table table) {
        if (table.rows().isEmpty()) {
            return Pair.create(new DataTableSpec(), List.of());
        }

        final var colNr = table.rows().get(0).size();

        final var columns = new LinkedList<DataColumnSpec>();
        final var colNames = new ArrayList<String>();
        var rowsScanned = 0;

        for (final var row : table.rows()) {
            rowsScanned++;
            guessRow(columns, colNames, rowsScanned, row);

            if (columns.size() == colNr) {
                break;
            }
        }

        return Pair.create(new DataTableSpec(columns.toArray(DataColumnSpec[]::new)), colNames);
    }

    private static void guessRow(final LinkedList<DataColumnSpec> columns, final ArrayList<String> colNames,
        int rowsScanned, final LinkedHashMap<String, Object> row) {
        for (final var kv : row.entrySet()) {
            if (!colNames.contains(kv.getKey())) {
                final var val = kv.getValue();
                rowsScanned++;
                if (val == null && rowsScanned < MAX_ROWS_SCANNED) {
                    continue; // cannot determine… yet
                }

                final var type = getDataType(val);

                colNames.add(kv.getKey());
                columns.add(new DataColumnSpecCreator(parseColumnName(kv.getKey()), type).createSpec());
            }
        }
    }

    private static DataType getDataType(final Object val) {
        if (val == null || val instanceof String) {
            return StringCell.TYPE;
        } else if (val instanceof Boolean) {
            return BooleanCell.TYPE;
        } else if (val instanceof Double) {
            return DoubleCell.TYPE;
        } else {
            throw new IllegalStateException("Unknown type: " + val.getClass().getSimpleName());
        }
    }

    private static String parseColumnName(final String columnName) {
        // extract name from possible fully qualified name 'table[col]'
        // or renamed column syntax '[col]'
        final var indirectIdx = columnName.indexOf('[');
        if (indirectIdx != -1 && columnName.length() - 1 > indirectIdx) {
            return columnName.substring(indirectIdx + 1, columnName.length() - 1);
        } else {
            return columnName;
        }
    }

    private static BufferedDataTable createTable(final DataTableSpec spec,
        final List<String> colNames, final Table table, final ExecutionContext exec) {
        final var out = exec.createDataContainer(spec);
        try {
            var rowKey = 0L;
            for (final var row : table.rows()) {
                out.addRowToTable(new DefaultRow(RowKey.createRowKey(rowKey), createRow(colNames, row, spec)));
                rowKey++;
            }
        } finally {
            out.close();
        }
        return out.getTable();
    }

    private static DataCell[] createRow(final List<String> colNames, final Map<String, Object> row,
        final DataTableSpec spec) {
        final var result = new DataCell[spec.getNumColumns()];
        for (var index = 0; index < spec.getNumColumns(); index++) {
            final var val = row.getOrDefault(colNames.get(index), null);
            final var type = spec.getColumnSpec(index).getType();
            if (val == null) {
                result[index] = DataType.getMissingCell();
            } else if (type.equals(StringCell.TYPE)) {
                result[index] = StringCell.StringCellFactory.create((String) val);
            } else if (type.equals(BooleanCell.TYPE)) {
                result[index] = BooleanCell.BooleanCellFactory.create((Boolean)val);
            } else if (type.equals(DoubleCell.TYPE)) {
                result[index] = DoubleCell.DoubleCellFactory.create((Double)val);
            }
        }
        return result;
    }
}
