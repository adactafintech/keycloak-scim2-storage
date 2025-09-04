package dev.suvera.keycloak.scim2.storage.storage;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import jakarta.mail.Session;

import org.apache.commons.lang3.StringUtils;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.models.GroupModel;
import org.keycloak.models.RoleModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.utils.KeycloakModelUtils;
import org.keycloak.protocol.oid4vc.model.Role;
import org.keycloak.storage.user.SynchronizationResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import dev.suvera.scim2.client.Scim2Client;
import dev.suvera.scim2.client.Scim2ClientBuilder;
import dev.suvera.scim2.schema.ScimConstant;
import dev.suvera.scim2.schema.data.ExtensionRecord;
import dev.suvera.scim2.schema.data.group.GroupRecord;
import dev.suvera.scim2.schema.data.group.GroupRecord.GroupMember;
import dev.suvera.scim2.schema.data.misc.ListResponse;
import dev.suvera.scim2.schema.data.misc.PatchRequest;
import dev.suvera.scim2.schema.data.misc.PatchResponse;
import dev.suvera.scim2.schema.data.user.UserRecord;
import dev.suvera.scim2.schema.data.user.UserRecord.UserAddress;
import dev.suvera.scim2.schema.data.user.UserRecord.UserEmail;
import dev.suvera.scim2.schema.data.user.UserRecord.UserName;
import dev.suvera.scim2.schema.data.user.UserRecord.UserPhoneNumber;
import dev.suvera.scim2.schema.data.user.UserRecord.UserRole;
import dev.suvera.scim2.schema.enums.PatchOp;
import dev.suvera.scim2.schema.ex.ScimException;

/**
 * author: suvera
 * date: 10/15/2020 11:25 AM
 */
@SuppressWarnings({ "FieldCanBeLocal", "unused" })
public class ScimClient2 {
    private static final Logger log = Logger.getLogger(ScimClient2.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ComponentModel componentModel;
    private Scim2Client scimService = null;
    private ScimException scimException = null;

    public int clientHashCode = -1;

    public ScimClient2(ComponentModel componentModel) {
        this.componentModel = componentModel;

        String endPoint = componentModel.get("endPoint");
        String authorityUrl = componentModel.get("authorityUrl");
        String username = componentModel.get("username");
        String password = componentModel.get("password");
        String clientId = componentModel.get("clientId");
        String clientSecret = componentModel.get("clientSecret");

        this.clientHashCode = getClientHashFromModel(componentModel);

        log.info("SCIM 2.0 endPoint: " + endPoint);
        endPoint = StringUtils.stripEnd(endPoint, " /");

        String resourceTypesJson = null;
        String schemasJson = null;

        ClassLoader classLoader = getClass().getClassLoader();
        InputStream isResourceTypes = classLoader.getResourceAsStream("ResourceTypes.json");
        if (isResourceTypes == null) {
            log.error("file not found! ResourceTypes.json");
            throw new IllegalArgumentException("file not found! ResourceTypes.json");
        } else {
            resourceTypesJson = inputStreamToString(isResourceTypes);
            resourceTypesJson = resourceTypesJson.replaceAll("\\{SCIM_BASE}", endPoint);
        }

        InputStream isSchemas = classLoader.getResourceAsStream("Schemas.json");
        if (isSchemas == null) {
            log.error("file not found! Schemas.json");
            throw new IllegalArgumentException("file not found! Schemas.json");
        } else {
            schemasJson = inputStreamToString(isSchemas);
            schemasJson = schemasJson.replaceAll("\\{SCIM_BASE}", endPoint);
        }

        Scim2ClientBuilder builder = new Scim2ClientBuilder(endPoint)
                .allowSelfSigned(true)
                .resourceTypes(resourceTypesJson)
                .schemas(schemasJson)
                .clientSecret(authorityUrl, username, password, clientId, clientSecret);

        /*
         * if (bearerToken != null && !bearerToken.isEmpty()) {
         * builder.bearerToken(bearerToken);
         * } else {
         * builder.usernamePassword(username, password);
         * }
         */

        try {
            scimService = builder.build();
        } catch (ScimException e) {
            scimException = e;
        }
    }

    public static int getClientHashFromModel(ComponentModel componentModel) {
        String endPoint = componentModel.get("endPoint");
        String authorityUrl = componentModel.get("authorityUrl");
        String username = componentModel.get("username");
        String password = componentModel.get("password");
        String clientId = componentModel.get("clientId");
        String clientSecret = componentModel.get("clientSecret");

        return Objects.hash(endPoint, authorityUrl, username, password, clientId, clientSecret);
    }

    private String inputStreamToString(InputStream is) {
        return new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
    }

    public void validate() throws ScimException {
        if (scimException != null) {
            throw scimException;
        }
    }

    private UserRecord buildScimUser(ScimUserAdapter userAdapter) {
        UserModel userModel = userAdapter.getLocalUserModel();
        UserRecord user = new UserRecord();

        user.setUserName(userModel.getUsername());

        List<String> defaultClaims = Arrays.asList("firstName", "lastName", "username", "email");
        Map<String, String> claims = new HashMap<>();

        ExtensionRecord er = new ExtensionRecord();
        userModel.getAttributes().forEach((k, v) -> {
            if (k.equalsIgnoreCase("partycode")) {
                er.setData("partyCode", v.get(0));
            }
            else if (k.equalsIgnoreCase("blocked")) {
                boolean blocked = v.get(0).equals("true");
                er.setData("blocked", blocked);
            }
            else if (!defaultClaims.contains(k)) {
                claims.put(k, v.get(0));
            }
        });

        er.setData("claims", claims);
        user.setExtensions(ScimConstant.URN_ADINSURE_USER, er);

        UserRecord.UserName name = new UserRecord.UserName();
        name.setGivenName(userModel.getFirstName() == null ? userModel.getUsername()
                : userModel.getFirstName());
        name.setFamilyName(userModel.getLastName());
        user.setName(name);

        if (isAttributeNotNull(userModel, "honorificPrefix")) {
            name.setHonorificPrefix(userModel.getFirstAttribute("honorificPrefix"));
        }
        if (isAttributeNotNull(userModel, "honorificSuffix")) {
            name.setHonorificSuffix(userModel.getFirstAttribute("honorificSuffix"));
        }

        user.setName(name);

        if (userModel.getEmail() != null) {
            UserRecord.UserEmail email = new UserRecord.UserEmail();
            email.setType("work");
            email.setPrimary(true);
            email.setValue(userModel.getEmail());

            user.setEmails(Collections.singletonList(email));
        } else {
            user.setEmails(Collections.emptyList());
        }

        user.setSchemas(ImmutableSet.of(ScimConstant.URN_USER));
        user.setExternalId(userModel.getId());
        user.setActive(userModel.isEnabled());

        List<UserRecord.UserGroup> groups = new ArrayList<>();
        userAdapter.getScimGroupsStream().forEach(groupAdapter -> {
            try {
                createOrUpdateGroup(groupAdapter);
            } catch (ScimException e) {
                log.error("", e);
            }

            UserRecord.UserGroup grp = new UserRecord.UserGroup();
            grp.setDisplay(groupAdapter.getGroupModel().getName());
            grp.setValue(groupAdapter.getGroupModel().getId());
            grp.setType("direct");

            groups.add(grp);
        });

        List<UserRecord.UserRole> roles = userModel.getRoleMappingsStream().map(roleModel -> {
            UserRecord.UserRole role = new UserRecord.UserRole();
            role.setDisplay(roleModel.getName());
            role.setValue(roleModel.getName());
            role.setType("direct");
            role.setPrimary(false);
        
            return role;
        }).collect(Collectors.toList());
        user.setRoles(roles);

        if (isAttributeNotNull(userModel, "title")) {
            user.setTitle(userModel.getFirstAttribute("title"));
        }
        if (isAttributeNotNull(userModel, "displayName")) {
            user.setDisplayName(userModel.getFirstAttribute("displayName"));
        } else {
            user.setDisplayName((strVal(name.getGivenName()) + " " + strVal(name.getFamilyName())).trim());
        }

        if (isAttributeNotNull(userModel, "nickName")) {
            user.setNickName(userModel.getFirstAttribute("nickName"));
        }

        if (isAttributeNotNull(userModel, "addresses_primary")) {
            List<UserRecord.UserAddress> addresses = new ArrayList<>();
            try {
                UserRecord.UserAddress addr = objectMapper.readValue(
                        userModel.getFirstAttribute("addresses_primary"),
                        UserRecord.UserAddress.class);
                addresses.add(addr);
            } catch (JsonProcessingException e) {
                log.error("", e);
            }

            user.setAddresses(addresses);
        } else {
            user.setAddresses(Collections.emptyList());
        }

        if (isAttributeNotNull(userModel, "phoneNumbers_primary")) {
            List<UserRecord.UserPhoneNumber> phones = new ArrayList<>();
            try {
                UserRecord.UserPhoneNumber phone = objectMapper.readValue(
                        userModel.getFirstAttribute("phoneNumbers_primary"),
                        UserRecord.UserPhoneNumber.class);
                phones.add(phone);
            } catch (JsonProcessingException e) {
                log.error("", e);
            }

            user.setPhoneNumbers(phones);
        } else {
            user.setPhoneNumbers(Collections.emptyList());
        }

        user.setIms(Collections.emptyList());
        user.setPhotos(Collections.emptyList());
        user.setEntitlements(Collections.emptyList());
        user.setX509Certificates(Collections.emptyList());

        try {
            log.info("Scim User: " + new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(user));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return user;
    }

    private boolean isAttributeNotNull(UserModel userModel, String name) {
        String val = userModel.getFirstAttribute(name);

        return !(val == null || val.isEmpty() || val.equals("null"));
    }

    private String strVal(String val) {
        return (val == null ? "" : val);
    }

    public void createUser(ScimUserAdapter userModel) throws ScimException {
        if (scimService == null) {
            return;
        }

        UserRecord scimUser = buildScimUser(userModel);
        UserRecord createdUser = scimService.createUser(scimUser);

        userModel.setExternalId(createdUser.getId());
        log.infof("User record with username %s and id %s successfully synced to SCIM service provider.", createdUser.getUserName(), createdUser.getId());
    }

    public void createOrUpdateUser(ScimUserAdapter scimUser, SynchronizationResult result) throws ScimException {
        if (scimService == null) {
            return;
        }

        UserRecord user = null;
        try {
            user = findUserByUsername(scimUser.getUsername());
        } catch (ScimException e) {
            user = null;
        }

        if (user == null) {
            createUser(scimUser);
            if (result != null) {
                result.increaseAdded();
            }
        } else {
            updateUser(scimUser, user);
            if (result != null) {
                result.increaseUpdated();
            }
        }
    }

    public UserRecord findUserByUsername(String username) throws ScimException {
        if (scimService == null) {
            return null;
        }

        ListResponse<UserRecord> users = scimService.filterUser("userName", username);

        return users.getResources().stream().findFirst().orElse(null);
    }

    private void updateUser(ScimUserAdapter userModel, UserRecord originalUser) throws ScimException {
        UserRecord scimUser = buildScimUser(userModel);
        scimUser.setId(originalUser.getId());

        PatchRequest<UserRecord> patchRequest = UserRecordPatchBuilder.buildPatchRequest(scimUser, originalUser);

        PatchResponse<UserRecord> response = scimService.patchUser(scimUser.getId(), patchRequest);

        userModel.setExternalId(response.getResource().getId());
    }

    public void updateUser(ScimUserAdapter userModel) throws ScimException {
        if (scimService == null) {
            return;
        }

        UserRecord user = getUser(userModel);
        if (user == null) {
            return;
        }

        updateUser(userModel, user);
    }

    public UserRecord getUser(ScimUserAdapter userModel) throws ScimException {
        if (scimService == null) {
            return null;
        }

        String id = userModel.getExternalId();

        if (id == null) {
            log.infof("User with %s does not exist in the SCIM provider.", userModel.getUsername());
            return null;
        }

        return scimService.readUser(id);
    }

    public void deleteUser(String skssId) throws ScimException {
        if (scimService == null) {
            return;
        }
        if (skssId != null) {
            scimService.deleteUser(skssId);
        }
    }

    public boolean assignRoleToUser(ScimUserAdapter userModel, RoleModel roleModel) throws ScimException {
        return userRolePatchRequest(userModel, roleModel, PatchOp.ADD);
    }

    public boolean unassignRoleFromUser(ScimUserAdapter userModel, RoleModel roleModel) throws ScimException {
        return userRolePatchRequest(userModel, roleModel, PatchOp.REMOVE);
    }

    private boolean userRolePatchRequest(ScimUserAdapter userModel, RoleModel roleModel, PatchOp operation) throws ScimException {
        if (scimService == null) {
            return false;
        }

        String roleName = roleModel.getName();
        String externalUserId = userModel.getExternalId();

        if (roleName != null && externalUserId != null) {
            PatchRequest<UserRecord> patchRequest = new PatchRequest<>(UserRecord.class);
            
            UserRole role = new UserRole();
            role.setDisplay(roleModel.getName());
            role.setValue(roleModel.getName());
            
            String roleJson = null;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                roleJson = objectMapper.writeValueAsString(role);
            } catch (JsonProcessingException e) {
                log.error("JSON processing error", e);
            }

            Map<String, String> map = new HashMap<>();
            List<Map<String, String>> userRoles = new ArrayList<>();
            map.put("value", roleJson);    
            userRoles.add(map);

            patchRequest.addOperation(operation, "roles", userRoles);

            PatchResponse<UserRecord> response = scimService.patchUser(externalUserId, patchRequest);
            return response.getStatus() == 200;
        }

        return false;
    }

    public void createOrUpdateGroup(ScimGroupAdapter scimGroup) throws ScimException {
        if (scimService == null) {
            return;
        }

        GroupRecord originalGroupRecord = null;
        try {
            originalGroupRecord = findGroupByGroupName(scimGroup.getGroupModel().getName());
        } catch (ScimException e) {
            originalGroupRecord = null;
        }

        if (originalGroupRecord == null) {
            createGroup(scimGroup);
        } else {
            if (scimGroup.getExternalId() == null) {
                // if there is no external id, just set it and do not replace group data
                scimGroup.setExternalId((originalGroupRecord.getId()));
                // TODO: maybe we need to patch group here
            } else {
                updateGroup(scimGroup, originalGroupRecord);
            }
        }
    }

    public GroupRecord findGroupByGroupName(String name) throws ScimException {
        if (scimService == null) {
            return null;
        }

        ListResponse<GroupRecord> users = scimService.filterGroup("displayName", name);

        return users.getResources().stream().findFirst().orElse(null);
    }

    public void createGroup(ScimGroupAdapter groupModel) throws ScimException {
        if (scimService == null) {
            return;
        }

        GroupRecord groupRecord = new GroupRecord();
        groupRecord.setDisplayName(groupModel.getGroupModel().getName());
        
        groupRecord = scimService.createGroup(groupRecord);

        groupModel.setExternalId(groupRecord.getId());
    }

    public String tryToSetExternalGroupIdFromOriginalGroup(String groupName, ScimGroupAdapter scimGroupAdapter) {
        String externalId = null;

        try {
            GroupRecord groupRecord = findGroupByGroupName(groupName);
            if (groupRecord != null) {
                externalId = groupRecord.getId();
                scimGroupAdapter.setExternalId(groupRecord.getId());
            }
        } catch (ScimException e) {
        }

        return externalId;
    }

    public String tryToSetExternalUserIdFromOriginalUser(String username, ScimUserAdapter userAdapter) {
        String externalId = null;

        try {
            UserRecord externalUserRecord = findUserByUsername(username);
            if (externalUserRecord != null) {
                externalId = externalUserRecord.getId();
                userAdapter.setExternalId(externalId);
            }
        } catch (ScimException e) {
        }

        return externalId;
    }

    private void updateGroupName(ScimGroupAdapter groupModel, GroupRecord groupRecord) throws ScimException {
        groupRecord.setDisplayName(groupModel.getGroupModel().getName());

        groupRecord = scimService.replaceGroup(groupRecord.getId(), groupRecord);

        groupModel.setExternalId(groupRecord.getId());
    }

    private void updateGroup(ScimGroupAdapter groupModel, GroupRecord originalGroupRecord) throws ScimException {
        String externalGroupId = groupModel.getExternalId();

        if (externalGroupId != null) {
            PatchRequest<GroupRecord> patchRequest = new PatchRequest<>(GroupRecord.class);

            PatchResponse<GroupRecord> response = scimService.patchGroup(externalGroupId, patchRequest);

            if (response.getStatus() >= 200 && response.getStatus() <= 299) {
                log.infof("Group %s patch request succedded with http status code %d.",
                        groupModel.getGroupModel().getName(), response.getStatus());
            } else {
                log.errorf("Group %s update failed with http status code %d.", groupModel.getGroupModel().getName(),
                        response.getStatus());
            }
        }
    }

    public void updateGroup(ScimGroupAdapter groupModel) throws ScimException {
        if (scimService == null) {
            return;
        }

        String externalId = groupModel.getExternalId();
        if (externalId == null || externalId.isEmpty()) {
            externalId = tryToSetExternalGroupIdFromOriginalGroup(groupModel.getGroupModel().getName(), groupModel);
        }

        if (externalId == null) {
            log.infof("Group %s does not exist in the SCIM2 provider", groupModel.getGroupModel().getName());
            return;
        }

        GroupRecord originalGroupRecord = scimService.readGroup(externalId);
        if (originalGroupRecord != null) {
            updateGroupName(groupModel, originalGroupRecord);
            updateGroup(groupModel, originalGroupRecord);
        }
    }

    public boolean joinGroup(ScimGroupAdapter groupModel, ScimUserAdapter userModel) throws ScimException {
        if (scimService == null) {
            return false;
        }

        String externalUserId = userModel.getExternalId();
        String externalGroupId = groupModel.getExternalId();

        if (externalUserId != null && externalGroupId != null) {
            PatchRequest<GroupRecord> patchRequest = new PatchRequest<>(GroupRecord.class);
            GroupMember groupMember = new GroupMember();
            groupMember.setDisplay(userModel.getUsername());
            groupMember.setValue(userModel.getExternalId());
            patchRequest.addOperation(PatchOp.ADD, "members", Arrays.asList(groupMember));

            PatchResponse<GroupRecord> response = scimService.patchGroup(externalGroupId, patchRequest);
            return response.getStatus() == 200;
        }

        return false;
    }

    public boolean leaveGroup(ScimGroupAdapter groupModel, ScimUserAdapter userModel) throws ScimException {
        if (scimService == null) {
            return false;
        }

        String externalUserId = userModel.getExternalId();
        String externalGroupId = groupModel.getExternalId();

        if (externalUserId != null && externalGroupId != null) {
            PatchRequest<GroupRecord> patchRequest = new PatchRequest<>(GroupRecord.class);

            GroupMember groupMember = new GroupMember();
            groupMember.setDisplay(userModel.getUsername());
            groupMember.setValue(userModel.getExternalId());
            patchRequest.addOperation(PatchOp.REMOVE, "members", Arrays.asList(groupMember));
            
            PatchResponse<GroupRecord> response = scimService.patchGroup(externalGroupId, patchRequest);
            return response.getStatus() == 200;
        }

        return false;
    }

    public void deleteGroup(String id) throws ScimException {
        if (scimService == null) {
            return;
        }

        if (id != null) {
            scimService.deleteGroup(id);
        }
    }

    public boolean assignRoleToGroup(ScimGroupAdapter groupModel, RoleModel roleModel) throws ScimException {
        if (scimService == null) {
            return false;
        }

        String roleName = roleModel.getName();
        String externalGroupId = groupModel.getExternalId();

        if (roleName != null && externalGroupId != null) {
            PatchRequest<GroupRecord> patchRequest = new PatchRequest<>(GroupRecord.class);
            
            patchRequest.addOperation(PatchOp.ADD, "roles", roleName);

            PatchResponse<GroupRecord> response = scimService.patchGroup(externalGroupId, patchRequest);
            return response.getStatus() == 200;
        }

        return false;
    }

    public boolean unassignRoleFromGroup(ScimGroupAdapter groupModel, RoleModel roleModel) throws ScimException {
        if (scimService == null) {
            return false;
        }

        String roleName = roleModel.getName();
        String externalGroupId = groupModel.getExternalId();

        if (roleName != null && externalGroupId != null) {
            PatchRequest<GroupRecord> patchRequest = new PatchRequest<>(GroupRecord.class);

            patchRequest.addOperation(
                PatchOp.REMOVE,
                String.format("roles[value eq \"%s\"]", roleName),
                null);

            PatchResponse<GroupRecord> response = scimService.patchGroup(externalGroupId, patchRequest);
            return response.getStatus() == 200;
        }

        return false;
    }
}
