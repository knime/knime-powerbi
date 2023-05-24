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
package org.knime.ext.azuread.auth;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.DesktopUtil;

import com.github.scribejava.apis.MicrosoftAzureActiveDirectory20Api;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractFuture;

import jakarta.ws.rs.core.MediaType;

/**
 * Static utility class to authenticate with Azure Active Directory.
 *
 * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
 * @deprecated
 */
@Deprecated
public class AzureADAuthenticationUtils {

    private static final NodeLogger LOGGER =  NodeLogger.getLogger(AzureADAuthenticationUtils.class);

    /** The client id of the app registration */
    private static final String CLIENT_ID = "cf47ff49-7da6-4603-b339-f4475176432b";

    /** Only one OAuth flow can be in progress at one time. This value tracks if there is one in progress currently. */
    private static final AtomicBoolean OAUTH_IN_PROGRESS = new AtomicBoolean(false);

    /** The port of the callback server */
    private static final int OAUTH_CALLBACK_PORT = 51355;

    private static final String OAUTH_LISTENER_PATH = "/oauth";

    private static final String OAUTH_CALLBACK_URL = "http://127.0.0.1:" + OAUTH_CALLBACK_PORT + OAUTH_LISTENER_PATH;

    private static final String OAUTH_SUCCESS_PAGE = "Received verification code. You may now close this window...";

    private static final String OAUTH_ERROR_PAGE = "Authentication failed.\n";

    private AzureADAuthenticationUtils() {
        // Utility class
    }

    /**
     * Authenticate with Azure AD using OAuth2. Note that a web browser will be opened asking the user to login. This
     * call does not block until the user has authenticated. Wait until the future is done.
     *
     * Cancel the future to cancel the authentication process.
     *
     * @param scope the OAuth scope that is requested
     * @return A future holding the authentication
     * @throws AuthenticationException if the authentication fails because of any reason
     */
    @SuppressWarnings("resource") // The service and server are closed by a waiting thread
    static synchronized Future<AzureADAuthentication> authenticate(final OAuth20Scope scope)
        throws AuthenticationException {
        if (OAUTH_IN_PROGRESS.get()) {
            // Another authentication is already in progress
            // We cannot do this in parallel because we open a webserver with a port
            throw new AuthenticaionInProgressException(
                "A authentication with Azure Active Directory is already in progress. "
                    + "Wait until the other authentication process is done or cancel it.");
        }
        // No authentication is in progress + No can start because this method is synchronized

        // The future authentication object
        final AzureADAuthenticationFuture authFuture = new AzureADAuthenticationFuture();

        // The OAuth20 service
        final OAuth20Service service = new ServiceBuilder(CLIENT_ID) //
            .defaultScope(scope.getScope()) //
            .callback(OAUTH_CALLBACK_URL) //
            .build(MicrosoftAzureActiveDirectory20Api.instance());

        var callbackServer = new Server(OAUTH_CALLBACK_PORT);
        var callbackHandler = new OAuthCallbackHandler(service, authFuture);
        callbackServer.setHandler(callbackHandler);
        try {
            callbackServer.start();
        } catch (Exception ex) {
            throw new AuthenticationException("Could not start callback server: " + ex.getMessage(), ex);
        }


        try {
            // Start a thread which closes the service and everything once the authentication is done
            startClosingThread(authFuture, service, callbackServer);
            OAUTH_IN_PROGRESS.set(true);

            // Start the authentication flow
            final String authorizationUrl = service.getAuthorizationUrl();

            // Open the browser and show the authentication site
            DesktopUtil.browse(new URL(authorizationUrl));
        } catch (final MalformedURLException ex) {
            authFuture.cancel(true);
            throw new AuthenticationException("Malformed authentication URL: " + ex.getMessage(), ex);
        } catch (final Exception ex) {
            // Always cancel if something goes wrong
            // Note: OAUTH_IN_PROGRESS is set to false by the "closingThread" because the future is canceled
            authFuture.cancel(true);
            throw ex;
        }

        return authFuture;
    }

    @SuppressWarnings("resource") // we must not close the servlet output stream
    private static class OAuthCallbackHandler extends AbstractHandler {

        private final AzureADAuthenticationFuture m_authFuture;

        private final OAuth20Service m_service;

        OAuthCallbackHandler(final OAuth20Service service, final AzureADAuthenticationFuture authFuture) {
            m_service = service;
            m_authFuture = authFuture;
        }

        @Override
        public void handle(final String target, final Request baseRequest, final HttpServletRequest request,
            final HttpServletResponse response) throws IOException, ServletException {
            if (!OAUTH_LISTENER_PATH.equals(target)) {
                response.sendError(404);
                return;
            }

            try {
                // Get the auth code
                final Optional<String> authCode = getAuthCodeFromRequest(request);
                if (authCode.isPresent()) {
                    // Request a token
                    final long requestTime = System.currentTimeMillis();
                    final OAuth2AccessToken accessToken = m_service.getAccessToken(authCode.get());

                    // Set the future result
                    m_authFuture.setResult(new DefaultAzureADAuthentication(accessToken.getAccessToken(),
                            requestTime + accessToken.getExpiresIn() * 1000, accessToken.getRefreshToken()));

                    configureResponse(response, 200, OAUTH_SUCCESS_PAGE);
                } else {
                    throw new AuthenticationException(getErrorFromRequest(request));
                }
            } catch (final Throwable t) {
                m_authFuture.setFailed(t);
                configureResponse(response, 400, OAUTH_ERROR_PAGE + t.getMessage());
            }
            response.getOutputStream().flush();
        }
    }

    @SuppressWarnings("resource") // we must not close the servlet output stream
    private static void configureResponse(final HttpServletResponse response, final int status, final String message)
            throws IOException {
        response.setContentType(MediaType.TEXT_HTML);
        response.setStatus(status);
        response.getOutputStream().write(message.getBytes(StandardCharsets.UTF_8));
    }


    /** Starts a thread which waits until the future is done and closes the service and stops the callback server */
    private static void startClosingThread(final Future<AzureADAuthentication> authFuture, final OAuth20Service service,
        final Server callbackServer) {
        new Thread(() -> {
            // Wait until the authentication is done
            try {
                authFuture.get();
                /* Wait for a few seconds before stopping the service, otherwise users
                 * might get a 404 due to a race condition between stopping the service
                 * and retrieving the redirect page. Similar issue as (SRV-2482). */
                Thread.sleep(5000);
            } catch (final ExecutionException | InterruptedException | CancellationException ex) {
                // Ignore
            }
            try {
                service.close();
            } catch (final IOException ex) {
                // Ignore
            }
            // Check that everything gets closed and stopped correctly
            try {
                callbackServer.stop();
            } catch (Exception ex) {
                LOGGER.warn("Could not stop OAuth callback server" +  ex.getMessage(), ex);
            }
            OAUTH_IN_PROGRESS.set(false);
        }).start();

    }

    /** Get the auth code from the parameters of a request */
    private static Optional<String> getAuthCodeFromRequest(final HttpServletRequest request) {
        return Optional.ofNullable(request.getParameter("code")).map(Strings::emptyToNull);
    }

    /** Parses the error from the request parameters (if present) and returns a formated error string */
    private static String getErrorFromRequest(final HttpServletRequest request) {
        final StringBuilder errorMessage = new StringBuilder();

        // Error parameter
        var errorParamValues = request.getParameterValues("error");
        if (errorParamValues != null) {
            if (errorParamValues.length > 1) {
                errorMessage.append("Errors: ").append(Arrays.toString(errorParamValues));
            } else if (errorParamValues.length == 1) {
                errorMessage.append("Error: ").append(errorParamValues[0]);
            }
        }

        // Error description parameter
        var errorDescParamValues = request.getParameterValues("error_description");
        if (errorDescParamValues != null) {
            if (errorDescParamValues.length > 1) {
                errorMessage.append("\nError descriptions: ").append(Arrays.toString(errorDescParamValues));
            } else if (errorDescParamValues.length == 1) {
                errorMessage.append("\nError description: ").append(errorDescParamValues[0]);
            }
        }

        return errorMessage.toString();
    }

    /**
     * Refresh the given authentication by requesting a new access token using the refresh token. This method blocks
     * until the token has been refreshed. Note that the <code>auth</code> parameter is edited.
     *
     * @param auth the authentication (which will be refreshed)
     * @return the refreshed authentication (same as <code>auth</code>)
     * @throws AuthenticationException if the authentication fails because of any reason
     * @throws InterruptedException if the authentication gets interrupted
     */
    public static AzureADAuthentication refreshToken(final AzureADAuthentication auth)
        throws AuthenticationException, InterruptedException {
        // Check if there is a refresh token
        final Optional<String> refreshToken = auth.getRefreshToken();
        if (!refreshToken.isPresent()) {
            throw new AuthenticationException("Cannot refresh the access token. No refresh token present.");
        }

        try (final OAuth20Service service =
            new ServiceBuilder(CLIENT_ID).build(MicrosoftAzureActiveDirectory20Api.instance())) {
            // Request the new access token
            final long requestTime = System.currentTimeMillis();
            final OAuth2AccessToken updatedAuth = service.refreshAccessToken(refreshToken.get());
            auth.updateAccessToken(updatedAuth.getAccessToken(), requestTime + updatedAuth.getExpiresIn() * 1000l);
            return auth;
        } catch (final IOException | ExecutionException e) {
            // Re-throw the exception
            throw new AuthenticationException(e.getMessage(), e);
        }
    }

    /**
     * Class implementing an Microsoft Azure Active Directory authentication future.
     *
     * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
     */
    private static final class AzureADAuthenticationFuture extends AbstractFuture<AzureADAuthentication> {

        void setResult(final AzureADAuthentication auth) {
            set(auth);
        }

        boolean setFailed(final Throwable throwable) {
            return setException(throwable);
        }
    }

    /**
     * An exception that is thrown if the authentication with Azure Active Directory failed because of any reason.
     *
     * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
     */
    public static class AuthenticationException extends Exception {

        private static final long serialVersionUID = 1L;

        private AuthenticationException(final String message) {
            super(message);
        }

        private AuthenticationException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An exception that is thrown if the authentication with Azure Active Directory failed because another
     * authentication is already in progress.
     *
     * @author Benjamin Wilhelm, KNIME GmbH, Konstanz, Germany
     */
    public static final class AuthenticaionInProgressException extends AuthenticationException {
        private static final long serialVersionUID = 1L;

        private AuthenticaionInProgressException(final String message) {
            super(message);
        }
    }
}
