package dev.suvera.keycloak.scim2.storage.storage;

import org.jboss.logging.MDC;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;

public class JobEnqueuerFactory {
    private static final String MDC_REALM_KEY = "kc.realmName";

    private JobEnqueuerFactory() {
    }

    public static JobEnqueuer create(KeycloakSession session) {
        if (MDC.get(MDC_REALM_KEY) == null) {
            RealmModel realm = session.getContext().getRealm();
            if (realm != null) {
                MDC.put(MDC_REALM_KEY, realm.getName());
            }
        }
        return new JobEnqueuer(session);
    }
}
