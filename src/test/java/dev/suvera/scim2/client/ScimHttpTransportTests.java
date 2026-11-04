package dev.suvera.scim2.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLHandshakeException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import dev.suvera.scim2.schema.util.UrlUtil;

/**
 * Unit tests for the java.net.http based SCIM transport. MockServer serves HTTP and HTTPS on the same port,
 * with a certificate for "localhost" signed by MockServer's own CA (untrusted by the JVM).
 */
public class ScimHttpTransportTests {
    private ClientAndServer server;

    @Before
    public void start() {
        // Keep the certificate's host names fixed, so a request to 127.0.0.2 is a real host name mismatch.
        ConfigurationProperties.preventCertificateDynamicUpdate(true);
        // MockServer writes its generated certificate files; keep them out of the working tree.
        ConfigurationProperties.directoryToSaveDynamicSSLCertificate("target");
        server = ClientAndServer.startClientAndServer(0);
    }

    @After
    public void stop() {
        server.stop();
    }

    private HttpRequest.Builder get(String scheme, String host, String path) {
        return HttpRequest.newBuilder(URI.create(scheme + "://" + host + ":" + server.getPort() + path)).GET();
    }

    @Test
    public void untrustedCertificateIsRejectedByDefault() {
        server.when(request().withPath("/ping")).respond(response().withStatusCode(200));
        ScimHttpTransport transport = new Scim2ClientBuilder("https://localhost").buildTransport();

        assertThrows(SSLHandshakeException.class, () -> transport.send(get("https", "localhost", "/ping")));
    }

    @Test
    public void allowSelfSignedAcceptsUntrustedCertificateAndHostMismatch() throws Exception {
        server.when(request().withPath("/ping")).respond(response().withStatusCode(200).withBody("pong"));
        ScimHttpTransport transport = new Scim2ClientBuilder("https://localhost").allowSelfSigned(true).buildTransport();

        assertEquals("pong", transport.send(get("https", "localhost", "/ping")).body());
        // 127.0.0.2 is not in the certificate: only passes because host name verification is off for this client.
        assertEquals("pong", transport.send(get("https", "127.0.0.2", "/ping")).body());
    }

    @Test
    public void controlHostMismatchIsRealWhenCaIsTrusted() throws Exception {
        // Proves the 127.0.0.2 case above is a real host name mismatch: with MockServer's CA trusted and normal
        // verification, localhost works and 127.0.0.2 is rejected.
        server.when(request().withPath("/ping")).respond(response().withStatusCode(200).withBody("pong"));
        java.security.KeyStore trustStore = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        trustStore.load(null, null);
        try (java.io.InputStream ca = getClass().getClassLoader()
                .getResourceAsStream("org/mockserver/socket/CertificateAuthorityCertificate.pem")) {
            trustStore.setCertificateEntry("mockserver-ca",
                    java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(ca));
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);
        ScimHttpTransport transport = new ScimHttpTransport(
                java.net.http.HttpClient.newBuilder().sslContext(sslContext).build(), java.time.Duration.ofSeconds(10), null);

        assertEquals("pong", transport.send(get("https", "localhost", "/ping")).body());
        assertThrows(SSLHandshakeException.class, () -> transport.send(get("https", "127.0.0.2", "/ping")));
    }

    @Test
    public void unauthorizedResponseRefreshesCredentialsAndRetriesOnce() throws Exception {
        server.when(request().withPath("/Users").withHeader("Authorization", "Bearer old"))
                .respond(response().withStatusCode(401));
        server.when(request().withPath("/Users").withHeader("Authorization", "Bearer new"))
                .respond(response().withStatusCode(200).withBody("ok"));

        AtomicInteger refreshes = new AtomicInteger();
        RequestAuthorizer authorizer = refresh -> {
            if (refresh) {
                refreshes.incrementAndGet();
                return "Bearer new";
            }
            return "Bearer old";
        };
        ScimHttpTransport transport = new ScimHttpTransport(
                java.net.http.HttpClient.newHttpClient(), java.time.Duration.ofSeconds(10), authorizer);

        HttpResponse<String> response = transport.send(get("http", "localhost", "/Users"));

        assertEquals(200, response.statusCode());
        assertEquals("ok", response.body());
        assertEquals(1, refreshes.get());
    }

    @Test
    public void fixedCredentialsAreNotRetriedAfterUnauthorized() throws Exception {
        server.when(request().withPath("/Users")).respond(response().withStatusCode(401));
        ScimHttpTransport transport = new Scim2ClientBuilder("http://localhost").bearerToken("t").buildTransport();

        assertEquals(401, transport.send(get("http", "localhost", "/Users")).statusCode());
        server.verify(request().withPath("/Users").withHeader("Authorization", "Bearer t"), VerificationTimes.once());
    }

    @Test
    public void oauthTokenIsCachedAndRefreshedAfterUnauthorized() throws Exception {
        String tokenPath = "/realms/test/protocol/openid-connect/token";
        server.when(request().withMethod("POST").withPath(tokenPath))
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"abc\"}"));
        OAuthTokenAuthorizer authorizer = new OAuthTokenAuthorizer(
                "http://localhost:" + server.getPort() + "/realms/test/", "user", "pass", "client", null);

        assertEquals("Bearer abc", authorizer.authorization(false));
        assertEquals("Bearer abc", authorizer.authorization(false));
        server.verify(request().withPath(tokenPath)
                .withBody("client_id=client&username=user&password=pass&grant_type=password"), VerificationTimes.once());

        assertEquals("Bearer abc", authorizer.authorization(true));
        server.verify(request().withPath(tokenPath), VerificationTimes.exactly(2));
    }

    @Test
    public void requestTimeoutAbortsASlowResponse() {
        server.when(request().withPath("/slow"))
                .respond(response().withStatusCode(200).withDelay(java.util.concurrent.TimeUnit.SECONDS, 5));
        ScimHttpTransport transport = new Scim2ClientBuilder("http://localhost")
                .requestTimeout(java.time.Duration.ofSeconds(1))
                .buildTransport();

        long start = System.nanoTime();
        assertThrows(java.net.http.HttpTimeoutException.class, () -> transport.send(get("http", "localhost", "/slow")));
        long seconds = java.time.Duration.ofNanos(System.nanoTime() - start).toSeconds();
        assertTrue("took " + seconds + " s", seconds < 4);
    }

    @Test
    public void timeoutsMustBePositive() {
        Scim2ClientBuilder builder = new Scim2ClientBuilder("http://localhost");

        assertThrows(IllegalArgumentException.class, () -> builder.connectTimeout(java.time.Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> builder.requestTimeout(java.time.Duration.ofSeconds(-1)));
    }

    @Test
    public void toUriEncodesIllegalCharactersAndKeepsExistingEscapes() {
        assertEquals("http://h/Users?filter=userName+eq+%22j%C3%B6hn%20doe%22",
                UrlUtil.toUri("http://h/Users?filter=userName+eq+%22jöhn doe%22").toString());
        assertEquals("http://h/Users?filter=emails+eq+%22a%7Cb@x.com%22",
                UrlUtil.toUri("http://h/Users?filter=emails+eq+%22a|b@x.com%22").toString());
        // A '%' that does not start a valid escape is itself encoded.
        assertEquals("http://h/a%252", UrlUtil.toUri("http://h/a%2").toString());
    }
}
