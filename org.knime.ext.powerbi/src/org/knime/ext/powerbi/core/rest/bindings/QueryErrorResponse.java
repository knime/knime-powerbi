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
package org.knime.ext.powerbi.core.rest.bindings;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.annotations.SerializedName;

/**
 * A query error reported by the Power BI REST API.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("java:S116") // Names given by JSON Structure
public final class QueryErrorResponse {

    private Error error;

    @Override
    public String toString() {
        return getError().toString();
    }

    /**
     * @return a string representation which is intended to be shown as node message
     */
    public String toNodeMessage() {
        return getError().getNodeMessage();
    }

    /**
     * @return the error
     */
    public PBIError getError() {
        return error != null ? error.getPbiError() : new PBIError("Unspecified");
    }

    /**
     * A query error message of Power BI.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static final class Error {
        private String code;

        @SerializedName("pbi.error")
        private PBIError pbiError;

        @Override
        public String toString() {
            return pbiError != null ? pbiError.toString() : "Unspecified";
        }

        /**
         * @return the actual nested pbi error thingy
         */
        public PBIError getPbiError() {
            return pbiError;
        }

        /**
         * Returns the error code.
         *
         * @return the error code
         */
        public String getCode() {
            return code;
        }
    }

    /**
     *
     * A query error message of Power BI.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static final class PBIError {
        private String code;

        private Detail[] details;

        private PBIError(final String c) {
            this.code = c;
        }

        /**
         * @return a string representation which is intended to be shown as node message
         */
        public String getNodeMessage() {
            for (var d : getDetails()) {
                if (d.code.equals("DetailsMessage") && StringUtils.isNotBlank(d.getDetailData().getValue())) {
                    return d.getDetailData().getValue();
                }
            }
            // fall back
            return toString();
        }

        @Override
        public String toString() {
            return "Error Code: " + getCode() + //
                (getDetails() != null ? (", Details: " + Arrays.toString(getDetails())) : "");
        }

        /**
         * Returns the error code.
         *
         * @return the error code
         */
        public String getCode() {
            return code;
        }

        /**
         * Returns the details.
         *
         * @return the details
         */
        public Detail[] getDetails() {
            return details;
        }
    }

    /**
     * Details of a query error message of Power BI.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static final class Detail {
        private String code;

        private DetailData detail; // NOSONAR given by JSON Structure

        @Override
        public String toString() {
            return detail.toString() + " (" + code + ")";
        }

        /**
         * @return the inner detail data
         */
        public DetailData getDetailData() {
            return detail;
        }

    }
    /**
     * Details of a detail of a query error message of Power BI.
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static final class DetailData {
        private static final Pattern TAG_PATTERN = Pattern.compile("<[^>]*>");

        private int type;

        private String value;

        @Override
        public String toString() {
            return getValue();
        }

        /**
         * Returns the type.
         *
         * @return the type
         */
        public int getType() {
            return type;
        }

        /**
         * Returns the value.
         *
         * @return the value
         */
        public String getValue() {
            return TAG_PATTERN.matcher(value).replaceAll("");
        }
    }
}
