package dev.suvera.keycloak.scim2.storage.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.keycloak.provider.ProviderConfigProperty;

public class SkssStorageProviderFactoryTests {

    @Test
    public void credentialsAreSecretPasswordFields() {
        for (String name : new String[]{"password", "clientSecret"}) {
            ProviderConfigProperty property = new SkssStorageProviderFactory().getConfigProperties().stream()
                    .filter(p -> name.equals(p.getName()))
                    .findFirst()
                    .orElseThrow();

            assertEquals(name, ProviderConfigProperty.PASSWORD, property.getType());
            assertTrue(name + " must be masked in the admin API and in exports", property.isSecret());
        }
    }
}
