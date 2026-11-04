package dev.suvera.keycloak.scim2.storage.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.Test;
import org.keycloak.component.ComponentModel;
import org.keycloak.component.ComponentValidationException;
import org.keycloak.provider.ProviderConfigProperty;

import dev.suvera.scim2.client.Scim2ClientBuilder;

public class SkssStorageProviderFactoryTests {

    private static ProviderConfigProperty property(String name) {
        return new SkssStorageProviderFactory().getConfigProperties().stream()
                .filter(p -> name.equals(p.getName()))
                .findFirst()
                .orElseThrow();
    }

    private static ComponentModel validConfig() {
        ComponentModel config = new ComponentModel();
        config.put("endPoint", "https://scim.example.com");
        config.put("authorityUrl", "https://auth.example.com/realms/x");
        config.put("clientId", "scim");
        return config;
    }

    @Test
    public void credentialsAreSecretPasswordFields() {
        for (String name : new String[]{"password", "clientSecret"}) {
            ProviderConfigProperty property = property(name);

            assertEquals(name, ProviderConfigProperty.PASSWORD, property.getType());
            assertTrue(name + " must be masked in the admin API and in exports", property.isSecret());
        }
    }

    @Test
    public void timeoutSettingsDefaultToTheClientDefaults() {
        assertEquals("10", property(SkssStorageProviderFactory.CONNECT_TIMEOUT).getDefaultValue());
        assertEquals("100", property(SkssStorageProviderFactory.REQUEST_TIMEOUT).getDefaultValue());

        // Providers created before the settings existed have no value.
        assertEquals(Scim2ClientBuilder.DEFAULT_REQUEST_TIMEOUT, SkssStorageProviderFactory.timeout(
                validConfig(), SkssStorageProviderFactory.REQUEST_TIMEOUT, Scim2ClientBuilder.DEFAULT_REQUEST_TIMEOUT));
    }

    @Test
    public void timeoutSettingIsReadInSeconds() {
        ComponentModel config = validConfig();
        config.put(SkssStorageProviderFactory.CONNECT_TIMEOUT, " 30 ");

        assertEquals(Duration.ofSeconds(30), SkssStorageProviderFactory.timeout(
                config, SkssStorageProviderFactory.CONNECT_TIMEOUT, Scim2ClientBuilder.DEFAULT_CONNECT_TIMEOUT));
    }

    @Test
    public void validationAcceptsEmptyAndPositiveTimeouts() {
        ComponentModel config = validConfig();
        config.put(SkssStorageProviderFactory.CONNECT_TIMEOUT, "");
        config.put(SkssStorageProviderFactory.REQUEST_TIMEOUT, "300");

        new SkssStorageProviderFactory().validateConfiguration(null, null, config);
    }

    @Test
    public void validationRejectsInvalidTimeouts() {
        for (String name : new String[]{SkssStorageProviderFactory.CONNECT_TIMEOUT, SkssStorageProviderFactory.REQUEST_TIMEOUT}) {
            for (String value : new String[]{"0", "-5", "1.5", "10s", "abc"}) {
                ComponentModel config = validConfig();
                config.put(name, value);

                assertThrows(name + "=" + value, ComponentValidationException.class,
                        () -> new SkssStorageProviderFactory().validateConfiguration(null, null, config));
            }
        }
    }
}
