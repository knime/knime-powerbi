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

import java.util.List;

/**
 * A PowerBI refresh details.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings({"java:S116", "java:S1068"}) // Names given by JSON Structure and used by it
public final class Refresh {

    private Status extendedStatus; // set by GSON

    private final String commitMode = "Transactional"; // NOSONAR always add to request

    private final String timeout;

    private final Type type;

    private final List<ObjectRefreshDefinition> objects;

    /**
     * Create a new refresh request object
     * @param type the type
     * @param timeout the timeout to be sent to Power BI
     * @param objects the objects to reset, may be null
     */
    public Refresh(final Type type, final String timeout, final List<ObjectRefreshDefinition> objects) {
        this.timeout = timeout;
        this.type = type;
        this.objects = objects;
    }

    /**
     * @return status of refresh
     */
    public Status getExtendedStatus() {
        return extendedStatus;
    }

    /**
     * @return objects to be reset
     */
    public List<ObjectRefreshDefinition> getObjects() {
        return objects;
    }

    /**
     * Specifies objects to be reset
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S116") // Names given by JSON Structure
    public static class ObjectRefreshDefinition {

        private String table;
        private String partition;

        /**
         * And Object which shall be reset
         * @param table the name of the table, may be null
         * @param partition the name of the portion, may be null
         */
        public ObjectRefreshDefinition(final String table, final String partition) {
            super();
            this.table = table;
            this.partition = partition;
        }
    }

    /**
     * Status of refresh
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S115") // Names given by JSON Structure (reduce GSON boilerplate)
    public enum Status {
        Canceled,
        Completed,
        Disabled,
        Failed,
        InProgress,
        NotStarted,
        TimedOut,
        Unknown;
    }

    /**
     * Type of refresh
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @SuppressWarnings("java:S115") // Names given by JSON Structure (reduce GSON boilerplate)
    public enum Type {
        Automatic,
        Calculate,
        ClearValues,
        DataOnly,
        Defragment,
        Full;
    }

}
