package dev.suvera.keycloak.scim2.storage.storage;

import java.util.List;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.models.GroupModel;
import org.keycloak.models.RoleModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.jpa.entities.ComponentEntity;
import org.keycloak.storage.user.SynchronizationResult;

import dev.suvera.keycloak.scim2.storage.ex.SyncException;
import dev.suvera.keycloak.scim2.storage.jpa.ScimSyncJobQueue;
import dev.suvera.scim2.schema.ex.ScimException;

/**
 * author: suvera
 * date: 10/15/2020 7:47 PM
 */
public class ScimSyncJob {
    public static final String CREATE_USER = "userCreate";
    public static final String CREATE_USER_EXTERNAL = "userCreateExternal";
    public static final String DELETE_USER = "userDelete";
    public static final String CREATE_GROUP = "groupCreate";
    public static final String UPDATE_GROUP = "groupUpdate";
    public static final String DELETE_GROUP = "groupDelete";
    public static final String JOIN_GROUP = "groupJoin";
    public static final String LEAVE_GROUP = "groupLeave";
    public static final String ADD_ROLE_TO_GROUP = "groupRoleAdd";
    public static final String REMOVE_ROLE_FROM_GROUP = "groupRoleRemove";

    private static final Logger log = Logger.getLogger(ScimSyncJob.class);
    private KeycloakSession session;
    private JobEnqueuer enquerer;
    private ScimSyncJobQueueManager queueManager;

    public ScimSyncJob(KeycloakSession session) {
        this.session = session;
        enquerer = JobEnqueuerFactory.create(session);
        queueManager = ScimSyncJobQueueManagerFactory.create(session);
    }

    public void execute(ScimSyncJobQueue job) {
        execute(job, null);
    }

    public void execute(ScimSyncJobQueue job, SynchronizationResult result) {
        ScimSyncJobModel jobModel = new ScimSyncJobModel(job);
        execute(jobModel, result);
    }

    public void execute(ScimSyncJobModel jobModel, SynchronizationResult result) {
        ScimSyncJobQueue job = queueManager.enqueueJob(jobModel.getJob());
        jobModel.setJob(job);
        jobModel.getMissingKeycloakModelsFromSession(session);

        try {
            log.infof("Executing SCIM sync job %s with action %s", job.getId(), job.getAction());
            executeJob(jobModel, result);
            queueManager.dequeueJob(job);
        } catch (ScimException e) {
            queueManager.increaseRetry(job);
            log.error(e.getMessage(), e);
            if (result != null) {
                result.increaseFailed();
            }
        } catch (SyncException e) {
            queueManager.dequeueJob(job);
            log.info(e.getMessage(), e);
        } catch (Exception e) {
            queueManager.dequeueJob(job);
            log.info(e.getMessage(), e);
        }
    }

    private void executeJob(ScimSyncJobModel jobModel, SynchronizationResult result) throws ScimException, SyncException {
        ScimSyncJobQueue job = jobModel.getJob();

        if (job == null) {
            log.error("Scim sync job is null. Cannot execute job.");
            return;
        }

        String action = job.getAction();

        if (action.equals(CREATE_USER)) {
            createUser(jobModel, result);
        } else if (action.equals(CREATE_USER_EXTERNAL)) {
            createUserExternal(jobModel, result);
        } else if (action.equals(DELETE_USER)) {
            deleteUser(jobModel, result);
        } else if (action.equals(CREATE_GROUP)) {
            createOrUpdateGroup(jobModel, false);
        } else if (action.equals(UPDATE_GROUP)) {
            createOrUpdateGroup(jobModel, true);
        } else if (action.equals(DELETE_GROUP)) {
            deleteGroup(jobModel);
        } else if (action.equals(JOIN_GROUP)) {
            joinGroup(jobModel);
        } else if (action.equals(LEAVE_GROUP)) {
            leaveGroup(jobModel);
        } else if (action.equals(ADD_ROLE_TO_GROUP)) {
            assignRoleToGroup(jobModel);
        } else if (action.equals(REMOVE_ROLE_FROM_GROUP)) {
            unassignRoleFromGroup(jobModel);
        } else {
            log.warnf("Unknown action %s for SCIM sync job %s", action, job.getId());
        }
    }

    private void createUserExternal(ScimSyncJobModel jobModel, SynchronizationResult result) throws ScimException, SyncException {
        UserModel userModel = jobModel.getUser();
        RealmModel realmModel = jobModel.getRealm();

        if (userModel == null) {
            throw new SyncException("Cannot create external user. Could not find user by id: %s", jobModel.getJob().getUserId());
        }

        ComponentEntity componentEntity = ComponentModelUtils
                .getComponents(session.getKeycloakSessionFactory(), realmModel.getId(),
                        SkssStorageProviderFactory.PROVIDER_ID)
                .findFirst()
                .orElse(null);

        if (componentEntity == null) {
            throw new SyncException("Could not create external user. Cannot find appropriate component.");
        }

        jobModel.setComponent(realmModel.getComponent(componentEntity.getId()));

        createUserCommon(jobModel, result);
    }

    private void createUser(ScimSyncJobModel jobModel, SynchronizationResult result) throws ScimException, SyncException {
        UserModel userModel = jobModel.getUser();
        ComponentModel componentModel = jobModel.getComponent();
        RealmModel realmModel = jobModel.getRealm();
        ScimSyncJobQueue job = jobModel.getJob();

        if (userModel == null) {
            throw new SyncException("Could not create user. Could not find user by id: %s", job.getUserId());
        }

        if (userModel.getFederationLink() == null) {
            throw new SyncException("Could not create user. User with username %s does not have a federation link.", userModel.getUsername());
        }

        if (job.getComponentId() != null && !job.getComponentId().equals(userModel.getFederationLink())) {
            throw new SyncException("Could not create user. User with username %s is not managed by federation plugin with id %s.", userModel.getUsername(), job.getComponentId());
        }

        if (componentModel == null) {
            componentModel = realmModel.getComponent(userModel.getFederationLink());
        }

        if (!componentModel.getProviderId().equals(SkssStorageProviderFactory.PROVIDER_ID)) {
            throw new SyncException("Could not create user %s. Federated user component is not of the correct type.", userModel.getUsername());
        }

        createUserCommon(jobModel, result);
    }

    private void createUserCommon(ScimSyncJobModel jobModel, SynchronizationResult result) throws ScimException {
        ComponentModel componentModel = jobModel.getComponent();
        RealmModel realmModel = jobModel.getRealm();
        UserModel userModel = jobModel.getUser();

        ScimClient2 scimClient = ScimClient2Factory.getClient(componentModel);

        ScimUserAdapter scimUserAdapter = new ScimUserAdapter(session, realmModel, componentModel, userModel);
        scimClient.createOrUpdateUser(scimUserAdapter, result);

        if (result != null) {
            // userModel.getGroupsStream().forEach(group -> {
            //     enquerer.enqueueGroupJoinJob(realmModel.getId(), group.getId(), userModel.getId());
            // });

            List<GroupModel> userGroups = userModel.getGroupsStream().collect(Collectors.toList());
            
            session.groups().getGroupsStream(realmModel).forEach(group -> {
                if (userGroups.stream().anyMatch(userGroup -> userGroup.getId().equals(group.getId()))) {
                    enquerer.enqueueGroupJoinJob(realmModel.getId(), group.getId(), userModel.getId());
                } else {
                    enquerer.enqueueGroupLeaveJob(realmModel.getId(), group.getId(), userModel.getId());
                }
            });
        }
    }

    private void deleteUser(ScimSyncJobModel jobModel, SynchronizationResult result) throws ScimException, SyncException {
        ComponentModel componentModel = jobModel.getComponent();
        RealmModel realmModel = jobModel.getRealm();
        ScimSyncJobQueue job = jobModel.getJob();

        if (componentModel == null) {
            if (job.getComponentId() == null) {
                throw new SyncException("Appropriate component is needed to delete user.");
            }

            componentModel = realmModel.getComponent(job.getComponentId());
        }

        if (!componentModel.getProviderId().equals(SkssStorageProviderFactory.PROVIDER_ID)) {
            throw new SyncException("Could not delete user. Federated user component is not of the correct type.");
        }

        ScimClient2 scimClient = ScimClient2Factory.getClient(componentModel);
        scimClient.deleteUser(job.getExternalId());

        if (result != null) {
            result.increaseRemoved();
        }
    }

    private void createOrUpdateGroupOnComponent(ScimSyncJobModel jobModel, ComponentModel componentModel, boolean updateOnly) throws ScimException, SyncException {
        GroupModel groupModel = jobModel.getGroup();
        RealmModel realmModel = jobModel.getRealm();

        if (groupModel == null) {
            throw new SyncException("Create/update group failed. Could not find group by id: " + jobModel.getJob().getGroupId());
        }

        ScimClient2 scimClient = ScimClient2Factory.getClient(componentModel);

        ScimGroupAdapter scimGroupAdapter = new ScimGroupAdapter(session, groupModel, realmModel.getId(),
                componentModel.getId());

        if (updateOnly) {
            scimClient.updateGroup(scimGroupAdapter);
        } else {
            scimClient.createOrUpdateGroup(scimGroupAdapter);
        }
    }

    private String getExternalUserIdByUsername(RealmModel realmModel, ComponentModel componentModel, String username,
            ScimClient2 scimClient) {

        if (username == null) {
            return null;
        }

        username = username.trim();

        UserModel userModel = session.users().getUserByUsername(realmModel, username);

        if (userModel == null) {
            return null;
        }

        ScimUserAdapter userAdapter = new ScimUserAdapter(session, realmModel, componentModel, userModel);
        String externalId = userAdapter.getExternalId();

        if (externalId == null || externalId.isEmpty()) {
            externalId = scimClient.tryToSetExternalUserIdFromOriginalUser(username, userAdapter);
        }

        return externalId;
    }

    private void createOrUpdateGroup(ScimSyncJobModel jobModel, boolean updateOnly) throws ScimException, SyncException {
        ComponentModel componentModel = getComponentModel(jobModel);
        createOrUpdateGroupOnComponent(jobModel, componentModel, updateOnly);
    }

    private ComponentModel getComponentModel(ScimSyncJobModel jobModel) throws ScimException, SyncException {
        ComponentModel componentModel = jobModel.getComponent();
        RealmModel realmModel = jobModel.getRealm();

        if (componentModel == null) {
            List<ComponentModel> components = ComponentModelUtils
                .getComponents(session.getKeycloakSessionFactory(), realmModel, SkssStorageProviderFactory.PROVIDER_ID)
                .collect(Collectors.toList());

            componentModel = components.stream()
                .filter(c -> SkssStorageProviderFactory.PROVIDER_ID.equals(c.getName()))
                .findFirst()
                .orElse(null);
        }

        if (componentModel == null) {
            throw new SyncException("Group could not be synced. Federated user component could not be acquired. ProviderId: " +
                SkssStorageProviderFactory.PROVIDER_ID + ", Realm: " + realmModel.getName()
            );
        }

        return componentModel;
    }

    private void deleteGroup(ScimSyncJobModel jobModel) throws ScimException {
        RealmModel realmModel = jobModel.getRealm();

        for (ComponentModel component : ComponentModelUtils
                .getComponents(session.getKeycloakSessionFactory(), realmModel, SkssStorageProviderFactory.PROVIDER_ID)
                .collect(Collectors.toList())) {
            ScimClient2 scimClient = ScimClient2Factory.getClient(component);

            ScimGroupAdapter scimGroupAdapter = new ScimGroupAdapter(session, jobModel.getJob().getGroupId(), realmModel.getId(),
                    component.getId());
            scimClient.deleteGroup(scimGroupAdapter.getExternalId());
            scimGroupAdapter.removeExternalId();
        }
    }

    private void joinGroup(ScimSyncJobModel jobModel) throws ScimException, SyncException {
        boolean shouldRecreateJob = leaveOrJoinGroup(jobModel, true);

        if (shouldRecreateJob) {
            enquerer.enqueueGroupJoinJob(jobModel.getRealm().getId(), jobModel.getGroup().getId(), jobModel.getUser().getId());
        }
    }

    private void leaveGroup(ScimSyncJobModel jobModel) throws ScimException, SyncException {
        boolean shouldRecreateJob = leaveOrJoinGroup(jobModel, false);

        if (shouldRecreateJob) {
            enquerer.enqueueGroupLeaveJob(jobModel.getRealm().getId(), jobModel.getGroup().getId(), jobModel.getUser().getId());
        }
    }

    private boolean leaveOrJoinGroup(ScimSyncJobModel jobModel, boolean join) throws ScimException, SyncException {
        UserModel userModel = jobModel.getUser();
        ComponentModel componentModel = jobModel.getComponent();
        GroupModel groupModel = jobModel.getGroup();
        ScimSyncJobQueue job = jobModel.getJob();
        RealmModel realmModel = jobModel.getRealm();

        if (userModel == null) {
            throw new SyncException("User with id %s cannot be found. Canceling leave/group action.", job.getUserId());
        }

        if (userModel != null && userModel.getFederationLink() == null) {
            throw new SyncException("User with username %s does not have a federation link. Canceling leave/group action.",
                    userModel.getUsername());
        }

        if (groupModel == null) {
            throw new SyncException("Could not find group by id: %s. Canceling leave/group action.", job.getUserId());
        }

        ScimGroupAdapter scimGroupAdapter = new ScimGroupAdapter(session, groupModel, realmModel.getId(),
                componentModel.getId());

        boolean createJobScheduled = false;

        if (scimGroupAdapter.getExternalId() == null) {
            enquerer.enqueueGroupCreateJob(realmModel.getId(), groupModel.getId());
            createJobScheduled = true;
        }

        ScimUserAdapter scimUserAdapter = new ScimUserAdapter(session, realmModel, componentModel, userModel);

        if (scimUserAdapter.getExternalId() == null) {
            enquerer.enqueueUserCreateJob(realmModel.getId(), componentModel.getId(), userModel.getId());
            createJobScheduled = true;
        }

        if (createJobScheduled) {
            return true;
        }

        ScimClient2 scimClient = ScimClient2Factory.getClient(componentModel);

        if (join) {
            scimClient.joinGroup(scimGroupAdapter, scimUserAdapter);
        } else {
            scimClient.leaveGroup(scimGroupAdapter, scimUserAdapter);
        }

        return false;
    }

    private void assignRoleToGroup(ScimSyncJobModel jobModel) throws ScimException, SyncException {
        boolean shouldRecreateJob = assignOrUnassignRoleOnGroup(jobModel, true);

        if (shouldRecreateJob) {
            enquerer.enqueueGroupJoinJob(jobModel.getRealm().getId(), jobModel.getGroup().getId(), jobModel.getUser().getId());
        }
    }

    private void unassignRoleFromGroup(ScimSyncJobModel jobModel) throws ScimException, SyncException {
        boolean shouldRecreateJob = assignOrUnassignRoleOnGroup(jobModel, false);

        if (shouldRecreateJob) {
            enquerer.enqueueGroupJoinJob(jobModel.getRealm().getId(), jobModel.getGroup().getId(), jobModel.getUser().getId());
        }
    }

    private boolean assignOrUnassignRoleOnGroup(ScimSyncJobModel jobModel, boolean join) throws ScimException, SyncException {
        GroupModel groupModel = jobModel.getGroup();
        RoleModel roleModel = jobModel.getRole();
        ScimSyncJobQueue job = jobModel.getJob();
        RealmModel realmModel = jobModel.getRealm();

        if (groupModel == null) {
            throw new SyncException("Could not find group by id: %s. Canceling assign/unassign role to group action.", job.getUserId());
        }

        if (roleModel == null) {
            throw new SyncException("Could not find role by id: %s. Canceling assign/unassign role to group action.", job.getUserId());
        }

        ComponentModel componentModel = getComponentModel(jobModel);
        ScimGroupAdapter scimGroupAdapter = new ScimGroupAdapter(session, groupModel, realmModel.getId(),
            componentModel.getId());

        ScimClient2 scimClient = ScimClient2Factory.getClient(componentModel);

        if (join) {
            scimClient.assignRoleToGroup(scimGroupAdapter, roleModel);
        } else {
            scimClient.unassignRoleFromGroup(scimGroupAdapter, roleModel);
        }

        return false;
    }
}
