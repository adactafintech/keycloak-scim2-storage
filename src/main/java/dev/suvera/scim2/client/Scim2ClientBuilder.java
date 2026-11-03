package dev.suvera.scim2.client;

import dev.suvera.scim2.schema.ex.ScimException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.net.Socket;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;

/**
 * author: suvera
 * date: 10/17/2020 2:26 PM
 */
@SuppressWarnings({"RedundantThrows", "unused"})
public class Scim2ClientBuilder {
    // Same values as the previous OkHttp client (connect and read timeouts).
    private static final Duration TIMEOUT = Duration.ofSeconds(1200000);

    private final String endPoint;
    private final HttpClient.Builder httpClientBuilder;
    private RequestAuthorizer authorizer;
    private String spConfigJson;
    private String resourceTypesJson;
    private String schemasJson;

    public Scim2ClientBuilder(String endPoint) {
        this.endPoint = endPoint;
        // HTTP/1.1 like before: avoids the h2c upgrade headers HttpClient sends on plain-HTTP URLs by default.
        this.httpClientBuilder = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(TIMEOUT);
    }

    public Scim2Client build() throws ScimException {
        return new Scim2ClientImpl(endPoint, buildTransport(), spConfigJson, resourceTypesJson, schemasJson);
    }

    ScimHttpTransport buildTransport() {
        return new ScimHttpTransport(httpClientBuilder.build(), TIMEOUT, authorizer);
    }

    /**
     * Kept for API compatibility: requests and responses are always logged at debug level
     * (category {@code dev.suvera.scim2.client.ScimHttpTransport}).
     */
    public Scim2ClientBuilder enableDebugging() {
        return this;
    }

    public Scim2ClientBuilder usernamePassword(String username, String password) {
        String credentials = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.ISO_8859_1));
        authorizer = RequestAuthorizer.fixed("Basic " + credentials);
        return this;
    }

    public Scim2ClientBuilder bearerToken(String token) {
        authorizer = RequestAuthorizer.fixed("Bearer " + token);
        return this;
    }

    public Scim2ClientBuilder clientSecret(String authorityUrl, String username, String password, String clientId, String clientSecret) {
        authorizer = new OAuthTokenAuthorizer(authorityUrl, username, password, clientId, clientSecret);
        return this;
    }

    public Scim2ClientBuilder serviceProviderConfig(String spConfigJson) {
        this.spConfigJson = spConfigJson;
        return this;
    }

    public Scim2ClientBuilder resourceTypes(String resourceTypesJson) {
        this.resourceTypesJson = resourceTypesJson;
        return this;
    }

    public Scim2ClientBuilder schemas(String schemasJson) {
        this.schemasJson = schemasJson;
        return this;
    }

    /**
     * When {@code true}, the SCIM endpoint's TLS certificate and host name are not verified. This applies to this
     * client only (not to the token endpoint, and not JVM-wide). Use only for test setups with self-signed certificates.
     */
    public Scim2ClientBuilder allowSelfSigned(boolean flag) {
        if (!flag) {
            return this;
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{new TrustAllTrustManager()}, new SecureRandom());
            httpClientBuilder.sslContext(sslContext);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not create TLS context", e);
        }
        return this;
    }

    /**
     * Accepts every certificate. It must be an {@link X509ExtendedTrustManager}: JSSE performs the host name check
     * inside the trust manager, and wraps a plain X509TrustManager in one that still checks the host name.
     */
    private static final class TrustAllTrustManager extends X509ExtendedTrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
