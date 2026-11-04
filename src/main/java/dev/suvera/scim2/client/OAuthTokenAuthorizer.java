package dev.suvera.scim2.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Obtains a bearer token from the authority's OpenID Connect token endpoint (password grant when username and
 * password are set, client credentials otherwise) and caches it until the SCIM server rejects it with a 401.
 * The token endpoint is always called with TLS verification enabled.
 */
final class OAuthTokenAuthorizer implements RequestAuthorizer {
    private static final Logger log = Logger.getLogger(OAuthTokenAuthorizer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(TIMEOUT)
            .build();
    private final URI tokenUri;
    private final String form;

    private String accessToken;

    OAuthTokenAuthorizer(String authorityUrl, String username, String password, String clientId, String clientSecret) {
        this.tokenUri = URI.create(StringUtils.strip(authorityUrl, "/") + "/protocol/openid-connect/token");

        Map<String, String> params = new LinkedHashMap<>();
        if (clientId != null) {
            params.put("client_id", clientId);
            if (clientSecret != null) {
                params.put("client_secret", clientSecret);
            }
            if (username != null && password != null) {
                params.put("username", username);
                params.put("password", password);
                params.put("grant_type", "password");
            } else {
                params.put("grant_type", "client_credentials");
            }
        }
        this.form = params.entrySet().stream()
                .map(e -> encode(e.getKey()) + "=" + encode(e.getValue()))
                .collect(Collectors.joining("&"));
    }

    @Override
    public synchronized String authorization(boolean refresh) throws IOException, InterruptedException {
        if (refresh) {
            accessToken = null;
        }
        if (accessToken == null) {
            accessToken = fetchToken();
        }
        return accessToken == null ? null : "Bearer " + accessToken;
    }

    private String fetchToken() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(tokenUri)
                .timeout(TIMEOUT)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            log.warnf("Token request to %s failed with HTTP %d", tokenUri, response.statusCode());
            return null;
        }
        return objectMapper.readValue(response.body(), Token.class).getAccessToken();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
