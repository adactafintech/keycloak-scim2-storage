package dev.suvera.scim2.client;

import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.stream.Collectors;

/**
 * Sends SCIM requests with {@link java.net.http.HttpClient}: adds the Authorization header, retries once with
 * refreshed credentials after a 401, and logs each exchange at debug level.
 */
final class ScimHttpTransport {
    private static final Logger log = Logger.getLogger(ScimHttpTransport.class);
    private static final int UNAUTHORIZED = 401;

    private final HttpClient client;
    private final Duration requestTimeout;
    private final RequestAuthorizer authorizer;

    ScimHttpTransport(HttpClient client, Duration requestTimeout, RequestAuthorizer authorizer) {
        this.client = client;
        this.requestTimeout = requestTimeout;
        this.authorizer = authorizer;
    }

    HttpResponse<String> send(HttpRequest.Builder builder) throws IOException, InterruptedException {
        builder.timeout(requestTimeout);
        if (authorizer == null) {
            return exchange(builder.build());
        }

        HttpResponse<String> response = exchange(withAuthorization(builder, authorizer.authorization(false)));
        if (response.statusCode() == UNAUTHORIZED) {
            String refreshed = authorizer.authorization(true);
            if (refreshed != null) {
                response = exchange(withAuthorization(builder, refreshed));
            }
        }
        return response;
    }

    private static HttpRequest withAuthorization(HttpRequest.Builder builder, String authorization) {
        if (authorization != null) {
            builder.setHeader("Authorization", authorization);
        }
        return builder.build();
    }

    private HttpResponse<String> exchange(HttpRequest request) throws IOException, InterruptedException {
        long start = System.nanoTime();
        if (log.isDebugEnabled()) {
            log.debugf("HTTP %s %s%n%s", request.method(), request.uri(), describe(request.headers()));
        }

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        if (log.isDebugEnabled()) {
            log.debugf("HTTP %d for %s %s in %.1fms%n%s", response.statusCode(), request.method(), request.uri(),
                    (System.nanoTime() - start) / 1e6d, describe(response.headers()));
        }
        return response;
    }

    /** Header dump for debug logs, with credentials masked. */
    private static String describe(HttpHeaders headers) {
        return headers.map().entrySet().stream()
                .map(e -> e.getKey() + ": " + ("authorization".equalsIgnoreCase(e.getKey()) ? "***" : String.join(", ", e.getValue())))
                .collect(Collectors.joining("\n"));
    }
}
