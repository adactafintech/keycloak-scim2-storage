package dev.suvera.keycloak.scim2.storage.events;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.JsonArray;

import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.admin.OperationType;
import org.keycloak.events.admin.ResourceType;
import org.keycloak.models.GroupModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.suvera.keycloak.scim2.storage.storage.ComponentModelUtils;
import dev.suvera.keycloak.scim2.storage.storage.JobEnqueuer;
import dev.suvera.keycloak.scim2.storage.storage.ScimGroupAdapter;
import dev.suvera.keycloak.scim2.storage.storage.SkssStorageProviderFactory;
import liquibase.pro.packaged.nu;
import liquibase.repackaged.org.apache.commons.collections4.Get;

public class ScimEventListener implements EventListenerProvider {
    private static final Logger log = Logger.getLogger(ScimEventListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private KeycloakSession session;
    private JobEnqueuer jobQueue;

    public ScimEventListener(KeycloakSession session, JobEnqueuer jobQueue) {
        this.session = session;
        this.jobQueue = jobQueue;
    }

    @Override
    public void close() {
    }

    @Override
    public void onEvent(Event event) {
        if (event.getType() == EventType.UPDATE_PROFILE) {
            log.infof("Handling event: %s", event.getType());

            jobQueue.enqueueUserUpdateJob(event.getRealmId(), event.getUserId());
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        if (event.getResourceType() == ResourceType.USER) {
            if (event.getOperationType() == OperationType.CREATE) {
                logEventHandlingMessage(event);

                JsonNode representationJson = readJsonString(event.getRepresentation());

                if (representationJson != null) {

                    jobQueue.enqueueUserCreateJob(
                            event.getRealmId(),
                            representationJson.get("username").asText());
                }
            } else if (event.getOperationType() == OperationType.UPDATE) {
                logEventHandlingMessage(event);

                JsonNode representationJson = readJsonString(event.getRepresentation());

                if (representationJson != null) {
                    jobQueue.enqueueUserCreateJob(
                            event.getRealmId(),
                            representationJson.get("federationLink").asText(),
                            representationJson.get("id").asText());
                }
            }
        }

        else if (event.getResourceType() == ResourceType.GROUP) {
            if (event.getOperationType() == OperationType.CREATE) {
                logEventHandlingMessage(event);

                JsonNode representationJson = readJsonString(event.getRepresentation());

                if (representationJson != null) {
                    jobQueue.enqueueGroupCreateJob(
                            event.getRealmId(),
                            representationJson.get("id").asText());
                }
            }

            else if (event.getOperationType() == OperationType.UPDATE) {
                logEventHandlingMessage(event);

                JsonNode representationJson = readJsonString(event.getRepresentation());

                if (representationJson != null) {
                    jobQueue.enqueueGroupUpdateJob(
                            event.getRealmId(),
                            representationJson.get("id").asText());
                }
            }

            else if (event.getOperationType() == OperationType.DELETE) {
                logEventHandlingMessage(event);

                // expected resource path: "groups/118e0637-d562-40ae-a357-e0b8bd71be6d"
                String[] splittedPath = event.getResourcePath().split("/");

                jobQueue.enqueueGroupDeleteJob(
                        event.getRealmId(),
                        splittedPath[splittedPath.length - 1]);
            }
        }

        else if (event.getResourceType() == ResourceType.GROUP_MEMBERSHIP) {
            if (event.getOperationType() == OperationType.CREATE) {
                logEventHandlingMessage(event);

                // expected resource path:
                // "users/f420cd38-d492-4ba8-a452-52f662d171a3/groups/118e0637-d562-40ae-a357-e0b8bd71be6d"
                String[] splittedPath = event.getResourcePath().split("/");
                String userId = splittedPath[1];
                String groupId = splittedPath[splittedPath.length - 1];

                jobQueue.enqueueGroupJoinJob(
                        event.getRealmId(),
                        groupId,
                        userId);
            }

            else if (event.getOperationType() == OperationType.DELETE) {
                logEventHandlingMessage(event);

                // expected resource path:
                // "users/f420cd38-d492-4ba8-a452-52f662d171a3/groups/118e0637-d562-40ae-a357-e0b8bd71be6d"
                String[] splittedPath = event.getResourcePath().split("/");
                String userId = splittedPath[1];
                String groupId = splittedPath[splittedPath.length - 1];

                jobQueue.enqueueGroupLeaveJob(
                        event.getRealmId(),
                        groupId,
                        userId);
            }
        }

        if (event.getResourceType() == ResourceType.REALM && event.getResourcePath().startsWith("groups", 0)) {
            logEventHandlingMessage(event);

            JsonNode representationJson = readJsonString(event.getRepresentation());

            if (representationJson != null) {

                // find local group
                String groupName = representationJson.get("name").asText();
                RealmModel realmModel = session.realms().getRealm(event.getRealmId());
                GroupModel groupModel = session.groupLocalStorage()
                        .searchForGroupByNameStream(realmModel, groupName, null, null)
                        .findFirst()
                        .orElse(null);

                // find migration data if present
                if (groupModel != null) {
                    List<String> migrationDataValues = groupModel.getAttributes().get("migration_data");
                    if (migrationDataValues != null && migrationDataValues.size() > 0) {
                        JsonNode migrationDataNode = readJsonString(migrationDataValues.get(0));
                        String externalId = migrationDataNode.get("id").asText();

                        // remove `migration_data` attribute
                        groupModel.removeAttribute("migration_data");

                        // set external id on group
                        for (ComponentModel component : ComponentModelUtils
                                .getComponents(session.getKeycloakSessionFactory(), realmModel,
                                        SkssStorageProviderFactory.PROVIDER_ID)
                                .collect(Collectors.toList())) {

                            ScimGroupAdapter scimGroupAdapter = new ScimGroupAdapter(session, groupModel,
                                    realmModel.getId(), component.getId());
                            scimGroupAdapter.setExternalId(externalId);
                        }
                    }
                }
            }
        }
    }

    private JsonNode readJsonString(String jsonString) {
        try {
            return objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            log.errorf("Cannot read a JSON string: %s", e.getMessage(), e);
            return null;
        }
    }

    private void logEventHandlingMessage(AdminEvent event) {
        log.infof("Handling admin event: %s, %s", event.getResourceType(), event.getOperationType());
    }
}
