package dev.suvera.keycloak.scim2.storage.storage;

import dev.suvera.scim2.schema.ex.ScimException;
import org.keycloak.component.ComponentModel;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * author: suvera
 * date: 10/16/2020 9:31 AM
 */
public class ScimClient2Factory {
    public static final Map<String, ScimClient2> instances = new ConcurrentHashMap<>();

    public static synchronized ScimClient2 getClient(ComponentModel componentModel) throws ScimException {
        String id = componentModel.getId();
        ScimClient2 scimClient = instances.get(id);

        if (scimClient != null && scimClient.clientHashCode != ScimClient2.getClientHashFromModel(componentModel)) {
            scimClient = null;
            instances.remove(id);
        }

        if (scimClient == null) {
            scimClient = new ScimClient2(componentModel);

            scimClient.validate();

            instances.put(componentModel.getId(), scimClient);
        }
        return scimClient;
    }
}
