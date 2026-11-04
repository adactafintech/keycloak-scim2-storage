package dev.suvera.keycloak.scim2.storage.storage;

import dev.suvera.scim2.client.Scim2ClientBuilder;
import org.keycloak.component.ComponentModel;
import org.keycloak.component.ComponentValidationException;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.RealmModel;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.storage.UserStorageProviderFactory;
import org.keycloak.storage.UserStorageProviderModel;
import org.keycloak.storage.user.ImportSynchronization;
import org.keycloak.storage.user.SynchronizationResult;

import java.time.Duration;
import java.util.Date;
import java.util.List;

/**
 * author: suvera
 * date: 10/15/2020 8:54 AM
 */
public class SkssStorageProviderFactory implements UserStorageProviderFactory<SkssStorageProvider>, ImportSynchronization {
    protected static final List<ProviderConfigProperty> configMetadata;

    public static final String PROVIDER_ID = "skss-scim2-storage";
    public static final String ALLOW_SELF_SIGNED = "allowSelfSigned";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String REQUEST_TIMEOUT = "requestTimeout";

    static {
        configMetadata = ProviderConfigurationBuilder.create()
                .property()
                .name("endPoint")
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("SCIM 2.0 url")
                .helpText("External SCIM 2.0 base URL")
                .add()

                .property()
                .name("authorityUrl")
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("Authority url")
                .helpText("URL to the authority that protects SCIM server")
                .add()

                .property()
                .name("username")
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("Username")
                .helpText("Username to access the scim resources")
                .add()

                .property()
                .name("password")
                .type(ProviderConfigProperty.PASSWORD)
                .secret(true)
                .label("Password")
                .helpText("Password to access the scim resources")
                .add()

                .property()
                .name("clientId")
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("Client Id")
                .helpText("Client id to access the scim resources")
                .add()

                .property()
                .name("clientSecret")
                .type(ProviderConfigProperty.PASSWORD)
                .secret(true)
                .label("Client Secret")
                .helpText("Client secret to access the scim resources")
                .add()

                .property()
                .name(ALLOW_SELF_SIGNED)
                .type(ProviderConfigProperty.BOOLEAN_TYPE)
                .label("Trust any TLS certificate")
                .helpText("Skip TLS certificate and host name verification for the SCIM 2.0 url. "
                        + "Only for test setups with self-signed certificates; never enable in production.")
                .defaultValue(false)
                .add()

                .property()
                .name(CONNECT_TIMEOUT)
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("Connect timeout (seconds)")
                .helpText("How long to wait for the connection to the SCIM 2.0 server. Empty means "
                        + Scim2ClientBuilder.DEFAULT_CONNECT_TIMEOUT.toSeconds() + " seconds.")
                .defaultValue(String.valueOf(Scim2ClientBuilder.DEFAULT_CONNECT_TIMEOUT.toSeconds()))
                .add()

                .property()
                .name(REQUEST_TIMEOUT)
                .type(ProviderConfigProperty.STRING_TYPE)
                .label("Request timeout (seconds)")
                .helpText("How long to wait for the SCIM 2.0 server to answer a request. Empty means "
                        + Scim2ClientBuilder.DEFAULT_REQUEST_TIMEOUT.toSeconds() + " seconds.")
                .defaultValue(String.valueOf(Scim2ClientBuilder.DEFAULT_REQUEST_TIMEOUT.toSeconds()))
                .add()

                .build();
    }

    //private ScimSyncRunner syncRunner;

    @Override
    public List<ProviderConfigProperty> getConfigProperties() {
        return configMetadata;
    }

    @Override
    public void validateConfiguration(KeycloakSession session, RealmModel realm, ComponentModel config)
            throws ComponentValidationException {
        if (config.get("endPoint") == null || config.get("endPoint").isEmpty()) {
            throw new ComponentValidationException("SCIM 2.0 endpoint is required.", "endPoint");
        }
        if (config.get("authorityUrl") == null || config.get("authorityUrl").isEmpty()) {
            throw new ComponentValidationException("Authority URL is required.", "authorityUrl");
        }
        if (config.get("clientId") == null || config.get("clientId").isEmpty()) {
            throw new ComponentValidationException("Client ID is required.", "clientId");
        }
        validateTimeout(config, CONNECT_TIMEOUT, "Connect timeout");
        validateTimeout(config, REQUEST_TIMEOUT, "Request timeout");
    }

    private static void validateTimeout(ComponentModel config, String name, String label) {
        try {
            timeout(config, name, Duration.ZERO);
        } catch (IllegalArgumentException e) {
            throw new ComponentValidationException(label + " must be a whole number of seconds greater than 0.", name);
        }
    }

    /**
     * The timeout setting {@code name} in seconds, or {@code defaultValue} when it is empty (also for providers
     * created before the setting existed).
     *
     * @throws IllegalArgumentException when the value is not a whole number greater than 0
     */
    static Duration timeout(ComponentModel config, String name, Duration defaultValue) {
        String value = config.get(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        long seconds;
        try {
            seconds = Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            seconds = 0;
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(name + " is not a whole number of seconds greater than 0: " + value);
        }
        return Duration.ofSeconds(seconds);
    }

    @Override
    public SkssStorageProvider create(KeycloakSession keycloakSession, ComponentModel componentModel) {
        return new SkssStorageProvider(keycloakSession, componentModel, JobEnqueuerFactory.create(keycloakSession));
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public SynchronizationResult sync(KeycloakSessionFactory sessionFactory, String realmId,
            UserStorageProviderModel model) {
        ScimSyncRunner syncRunner = new ScimSyncRunner(sessionFactory, model);
        return syncRunner.syncAll(realmId);
    }

    @Override
    public SynchronizationResult syncSince(Date lastSync, KeycloakSessionFactory sessionFactory, String realmId,
            UserStorageProviderModel model) {
        
        ScimSyncRunner syncRunner = new ScimSyncRunner(sessionFactory, model);
        return syncRunner.syncSince(lastSync, realmId);
    }
}
