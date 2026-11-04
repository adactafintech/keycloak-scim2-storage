package dev.suvera.scim2.client;

import java.io.IOException;

/**
 * Supplies the {@code Authorization} header value for SCIM requests.
 */
@FunctionalInterface
interface RequestAuthorizer {

    /**
     * @param refresh {@code false} for the first attempt of a request; {@code true} after the server answered
     *                401, to obtain fresh credentials for a single retry.
     * @return the header value, or {@code null} to send the request without one (or not to retry)
     */
    String authorization(boolean refresh) throws IOException, InterruptedException;

    /** Fixed credentials: sent with every request, never retried after a 401. */
    static RequestAuthorizer fixed(String headerValue) {
        return refresh -> refresh ? null : headerValue;
    }
}
