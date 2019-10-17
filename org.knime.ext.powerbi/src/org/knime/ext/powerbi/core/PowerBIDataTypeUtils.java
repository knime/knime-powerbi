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
package org.knime.ext.powerbi.core;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataType;
import org.knime.core.data.DataValue;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.StringValue;
import org.knime.core.data.time.localdate.LocalDateValue;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;

import com.google.gson.Gson;

/**
 * See https://docs.microsoft.com/en-us/power-bi/developer/api-dataset-properties
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 */
public class PowerBIDataTypeUtils {

    private static final Gson GSON = new Gson();

    private PowerBIDataTypeUtils() {
        // Utility class
    }

    /**
     * Convert the KNIME type to a Power BI type.
     *
     * @param knimeType the KNIME type
     * @return the Power BI type
     */
    public static Optional<String> powerBITypeForKNIMEType(final DataType knimeType) {
        if (knimeType.isCompatible(BooleanValue.class)) {
            return Optional.of("Boolean");
        } else if (knimeType.isCompatible(DoubleValue.class)) {
            return Optional.of("Double");
        } else if (knimeType.isCompatible(IntValue.class)) {
            return Optional.of("Int32");
        } else if (knimeType.isCompatible(LongValue.class)) {
            return Optional.of("Int64");
        } else if (knimeType.isCompatible(LocalDateValue.class)) {
            return Optional.of("DateTime");
        } else if (knimeType.isCompatible(LocalDateTimeValue.class)) {
            return Optional.of("DateTime");
        } else if (knimeType.isCompatible(StringValue.class)) {
            return Optional.of("String");
        }
        return Optional.empty();
    }

    /**
     * Converts a KNIME data value to its string representation in a JSON object for Power BI.
     *
     * @param value the KNIME data value
     * @return the JSON string representation of the value for Power BI
     */
    public static Optional<String> powerBIValueForKNIMEValue(final DataValue value) {
        if (value instanceof MissingCell) {
            return Optional.of("null");
        } else if (value instanceof BooleanValue) {
            final boolean v = ((BooleanValue)value).getBooleanValue();
            return Optional.of(String.valueOf(v));
        } else if (value instanceof DoubleValue) {
            final double v = ((DoubleValue)value).getDoubleValue();
            return Optional.of(String.valueOf(v));
        } else if (value instanceof IntValue) {
            final int v = ((IntValue)value).getIntValue();
            return Optional.of(String.valueOf(v));
        } else if (value instanceof LongValue) {
            final long v = ((LongValue)value).getLongValue();
            return Optional.of(String.valueOf(v));
        } else if (value instanceof LocalDateValue) {
            final LocalDate localDate = ((LocalDateValue)value).getLocalDate();
            final String v = localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
            return Optional.of('"' + v + '"');
        } else if (value instanceof LocalDateTimeValue) {
            final LocalDateTime localDateTime = ((LocalDateTimeValue)value).getLocalDateTime();
            final String v = localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return Optional.of('"' + v + '"');
        } else if (value instanceof StringValue) {
            final String v = ((StringValue)value).getStringValue();
            String json = GSON.toJson(v);
            return Optional.of(json);
        }
        return Optional.empty();
    }
}
