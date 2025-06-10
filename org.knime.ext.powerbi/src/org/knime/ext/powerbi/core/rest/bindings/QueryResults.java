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
 *   27.05.2025 (loescher): created
 */
package org.knime.ext.powerbi.core.rest.bindings;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * A PowerBI query result which may contain the result of multiple queries.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("java:S116") // Names given by JSON Structure
public final class QueryResults {
    private final Result[] results;

    private QueryResults(final Result[] results) { // NOSONAR Needed by GSON
        this.results = results;
    }

    /**
     * @return the results
     */
    public Result[] results() {
        return results;
    }

    /**
     * A result structure
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static final class Result {
        private Table[] tables;
        private Error error;

        /**
         * @return the resulting tables
         */
        public Table[] tables() {
            return tables;
        }

        /**
         * @return any optional error notice
         */
        public Error error() {
            return error;
        }

        /**
         * A table structure
         * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
         */
        @SuppressWarnings({"java:S116", "java:S1319"}) // Names given by JSON Structure, keep order
        public static final class Table {
            private List<LinkedHashMap<String, Object>> rows;

            /**
             * @return the rows
             */
            public List<LinkedHashMap<String,Object>> rows() {
                return rows;
            }

            /**
             * A a row in a table
             * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
             */
            @SuppressWarnings({"java:S116", "java:S1319"}) // Names given by JSON Structure, keep order
            public static final class Row {
                private LinkedHashMap<String, Object> values;

                /**
                 * @return the cells
                 */
                public LinkedHashMap<String, Object> values() {
                    return values;
                }

            }
        }

        /**
         * An error structure
         * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
         */
        @SuppressWarnings("java:S116") // Names given by JSON Structure
        public static final class Error {
            private String code;
            private String message;

            /**
             * @return the error code
             */
            public String code() {
                return code;
            }

            /**
             * @return the message
             */
            public String message() {
                return message;
            }

        }
    }
}
