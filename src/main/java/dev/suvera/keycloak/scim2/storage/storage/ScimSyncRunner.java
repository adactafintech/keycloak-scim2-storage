package dev.suvera.keycloak.scim2.storage.storage;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.persistence.EntityManager;

import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.connections.jpa.JpaConnectionProvider;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.storage.user.SynchronizationResult;

import dev.suvera.keycloak.scim2.storage.jpa.ScimSyncJobQueue;

public class ScimSyncRunner {
    private static final Logger log = Logger.getLogger(ScimSyncRunner.class);
    private KeycloakSessionFactory sessionFactory;
    private ComponentModel model;

    public ScimSyncRunner(KeycloakSessionFactory sessionFactory, ComponentModel model) {
        this.sessionFactory = sessionFactory;
        this.model = model;
    }

    public SynchronizationResult syncAll(String realmId) {
        log.infof("Starting synchronization of all users for realm: %s", realmId);

        SynchronizationResult result = new SynchronizationResult();

        List<String> ldapComponentModels = ComponentModelUtils
                .getLDAPComponentsWithScimEventsEnabled(sessionFactory, realmId)
                .collect(Collectors.toList());

        KeycloakSession session = sessionFactory.create();
        RealmModel realm = session.realms().getRealm(realmId);

        int batchNumber = 0;
        List<UserModel> users;
        do {
            log.infof("Fetching batch %d of users...", batchNumber);
            users = getBatchOfUsers(realm, ldapComponentModels, 1000, batchNumber);
            log.infof("Fetched %d users in batch %d.", users.size(), batchNumber);

            users.forEach(user -> executeSyncUserJob(user, realmId, result));
            log.infof("Executed synchronization of users for batch %d.", batchNumber);

            batchNumber++;
        } while (!users.isEmpty());

        log.infof("Completed synchronization of all users for realm: %s", realmId);
        return result;
    }

    public SynchronizationResult syncSince(Date lastSync, String realmId) {
        log.infof("Starting syncing pending jobs.", lastSync, realmId);
        SynchronizationResult result = callSyncJobs();
        log.info("Syncing pending jobs completed.");
        return result;
    }

    private List<UserModel> getBatchOfUsers(RealmModel realm, List<String> ldapComponentModels, int batchSize,
            int batchNumber) {
        List<UserModel> users = new ArrayList<>();
        KeycloakModelUtils.runJobInTransaction(sessionFactory, kcSession -> {
            kcSession.getContext().setRealm(realm);
            kcSession.users()
                    .searchForUserStream(realm, Map.of(UserModel.ENABLED, "true"))
                    .sorted((u1, u2) -> u1.getId().compareTo(u2.getId()))
                    .filter(u -> u.getFederationLink() != null
                            && (u.getFederationLink().equals(model.getId()))
                            || ldapComponentModels.stream().anyMatch(l -> l.equals(u.getFederationLink())))
                    .skip(batchSize * batchNumber)
                    .limit(batchSize)
                    .forEach(users::add);
        });
        return users;
    }

    private void executeSyncUserJob(UserModel user, String realmId, SynchronizationResult result) {
        KeycloakModelUtils.runJobInTransaction(sessionFactory, jobSession -> {
            ScimSyncJob sync = new ScimSyncJob(jobSession);

            ScimSyncJobQueue job = new ScimSyncJobQueue();

            String action = ScimSyncJob.CREATE_USER;
            if (!user.getFederationLink().equals(model.getId())) {
                action = ScimSyncJob.CREATE_USER_EXTERNAL;
            }

            job.setAction(action);
            job.setId(KeycloakModelUtils.generateId());
            job.setRealmId(realmId);
            job.setComponentId(model.getId());
            job.setUserId(user.getId());

            sync.execute(job, result);
        });
    }

    private SynchronizationResult callSyncJobs() {
        SynchronizationResult result = new SynchronizationResult();

        for (List<ScimSyncJobQueue> jobs = fetchJobs(); !jobs.isEmpty(); jobs = fetchJobs()) {
            jobs.forEach(job -> executeJob(job, result));
        }

        return result;
    }

    private List<ScimSyncJobQueue> fetchJobs() {
        List<ScimSyncJobQueue> jobs = new ArrayList<>();
        KeycloakModelUtils.runJobInTransaction(sessionFactory, kcSession -> {
            EntityManager em = kcSession.getProvider(JpaConnectionProvider.class).getEntityManager();
            log.infof("Fetching new batch of pending jobs...");
            em.createNamedQuery("getPendingJobs", ScimSyncJobQueue.class)
                .setMaxResults(1000)
                .getResultStream()
                .forEach(jobs::add);
        });
        return jobs;
    }

    private void executeJob(ScimSyncJobQueue job, SynchronizationResult result) {
        KeycloakModelUtils.runJobInTransaction(sessionFactory, kcSession -> {
            ScimSyncJob sync = new ScimSyncJob(kcSession);
            sync.execute(job, result);
        });
    }
}
