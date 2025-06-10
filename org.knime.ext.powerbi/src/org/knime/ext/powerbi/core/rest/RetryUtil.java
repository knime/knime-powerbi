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
 *   03.02.2025 (loescher): created
 */
package org.knime.ext.powerbi.core.rest;

import java.time.Duration;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status.Family;

/**
 * Contains utilities to retry requests for Power BI nodes with a backoff strategy.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
public final class RetryUtil {

    private RetryUtil() {
        throw new UnsupportedOperationException("Utility class");
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(RetryUtil.class);

    /** Wait times after a request failed (and no Retry-After is provided). */
    private static final Duration[] RETRY_WAIT = new Duration[] {
        Duration.ofMillis(500),
        Duration.ofSeconds(5),
        Duration.ofSeconds(10),
        Duration.ofSeconds(20),
        Duration.ofSeconds(40),
        Duration.ofSeconds(80),
        Duration.ofSeconds(160),
    };

    /**
     * Retries a request with a backoff strategy. A response will be retried if it has
     * status code 429 (TOO MANY REQUESTS) or any 5xx (SERVER ERROR). After a fixed amount of retries the request will
     * not be retried.
     *
     * @param request the request to retry. It will be executed for each try.
     * @param exec
     *         the execution context used to notify the user about the waiting period when waiting. The message will
     *         be restored. Can be {@code null} in which case no message will be set.
     * @return the return value of the request
     * @throws CanceledExecutionException may be thrown by a request (will not retry)
     */
    public static Response withRetry(final RetryableRequest request, final ExecutionContext exec)
        throws CanceledExecutionException {
        for (var wait : RETRY_WAIT) {
            final var res = request.execute();
            final var code = res.getStatus();
            if (code == 429 /* TOO MANY REQUESTS */ || (res.getStatusInfo().getFamily() == Family.SERVER_ERROR)) {
                wait = parseRetryAfter(res, wait);
                waitFor("Got status \"" + res.getStatusInfo().getReasonPhrase() + "\" (" + code + ")", wait, exec);
            } else {
                return res;
            }
        }
        return request.execute();
    }

    private static Duration parseRetryAfter(final Response e, final Duration defaultRetryAfter) {
        final var headerVal = e.getHeaders().get("Retry-After");
        if (headerVal != null && !headerVal.isEmpty()) {
            final var value = headerVal.get(0).toString();
            try {
                return Duration.ofSeconds(Long.parseLong(value));
            } catch (NumberFormatException ignored) {
                // just fall back to default
            }
        }
        return defaultRetryAfter;
    }

    private static void waitFor(final String cause, final Duration time, final ExecutionContext exec)
        throws CanceledExecutionException {
        if (time.isNegative() || time.isZero()) {
            return;
        }
        final var message = String.format("%s, retrying after %dms", cause, time.toMillis());
        final var oldMessage = exec != null ? exec.getProgressMonitor().getMessage() : null;
        LOGGER.debug(message);
        try {
            long millis = time.toMillis();
            long seconds = time.toSeconds();
            while (millis > 1000) {
                if (exec != null) {
                    exec.setMessage(String.format("%s - %s, retrying after %ds", oldMessage, cause, seconds));
                }
                Thread.sleep(1000);
                millis -= 1000;
                seconds--;
            }
            if (millis > 0) {
                Thread.sleep(millis);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new CanceledExecutionException();
        }
        if (exec != null) {
            exec.setMessage(oldMessage);
        }

    }

    /**
     * A request accepted by {@link RetryUtil#withRetry(RetryableRequest, ExecutionContext)}
     *
     * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
     */
    @FunctionalInterface
    public interface RetryableRequest { // NOSONAR better documentation
        /**
         * Execute the given request.
         *
         * @return the return type of the request
         */
        Response execute();
    }

}
