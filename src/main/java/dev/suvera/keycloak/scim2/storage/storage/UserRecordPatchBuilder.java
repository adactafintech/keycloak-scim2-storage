package dev.suvera.keycloak.scim2.storage.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

import dev.suvera.scim2.schema.data.misc.PatchRequest;
import dev.suvera.scim2.schema.data.user.UserRecord;
import dev.suvera.scim2.schema.data.user.UserRecord.UserClaim;
import dev.suvera.scim2.schema.enums.PatchOp;
import dev.suvera.scim2.schema.data.ExtensionRecord;

public class UserRecordPatchBuilder {
    private UserRecordPatchBuilder() { /* static class */ }

    public static PatchRequest<UserRecord> buildPatchRequest(UserRecord modifiedRecord, UserRecord originalRecord) {
        PatchRequest<UserRecord> patchRequest = new PatchRequest<>(UserRecord.class);

        addOperation(patchRequest, originalRecord.getName(), modifiedRecord.getName(), "name");
        addOperation(patchRequest, originalRecord.getDisplayName(), modifiedRecord.getDisplayName(), "displayName");
        addOperation(patchRequest, originalRecord.getNickName(), modifiedRecord.getNickName(), "nickName");
        addOperation(patchRequest, originalRecord.getProfileUrl(), modifiedRecord.getProfileUrl(), "profileUrl");
        addOperation(patchRequest, originalRecord.getTitle(), modifiedRecord.getTitle(), "title");
        addOperation(patchRequest, originalRecord.getUserType(), modifiedRecord.getUserType(), "userType");
        addOperation(patchRequest, originalRecord.getPreferredLanguage(), modifiedRecord.getPreferredLanguage(), "preferredLanguage");
        addOperation(patchRequest, originalRecord.getLocale(), modifiedRecord.getLocale(), "locale");
        addOperation(patchRequest, originalRecord.getTimezone(), modifiedRecord.getTimezone(), "timezone");
        addOperation(patchRequest, originalRecord.getPassword(), modifiedRecord.getPassword(), "password");

        if (originalRecord.isActive() != modifiedRecord.isActive()) {
            patchRequest.addOperation(PatchOp.REPLACE, "active", modifiedRecord.isActive());
        }

        List<UserClaim> modifiedClaims = modifiedRecord.getClaims();

        Collection<ExtensionRecord> records = originalRecord.getExtensions().values();
        List<UserClaim> existingClaims = new ArrayList<UserClaim>();
        String existingPartyCode = "";

        ExtensionRecord existingClaimsRecord = records.iterator().next();

        JsonNode existingPartyCodeRecord = existingClaimsRecord.asJsonNode().get("partyCode");
        if(existingPartyCodeRecord != null){
            existingPartyCode = existingPartyCodeRecord.textValue();
        }

        for (UserClaim modifiedClaim : modifiedClaims) {
            String key = modifiedClaim.getAttributeKey();

            JsonNode existingClaimsObject = existingClaimsRecord.asJsonNode().get("claims");
            JsonNode claimValueNode = null;
            String claimValue = "";

            if(existingClaimsObject != null)
            {
                claimValueNode = existingClaimsObject.get(key);
            }
            
            if(claimValueNode != null){
                claimValue = claimValueNode.asText();
            }

            if(!claimValue.isEmpty()){
                UserClaim claim = new UserClaim();
                claim.setAttributeKey(key);
                claim.setAttributeValue(claimValue);

                existingClaims.add(claim);
            }
        }

        /*
        JsonNode originalRecordFields = record.asJsonNode().get("claims");
        
        if(originalRecordFields != null){
            Iterator<Entry<String, JsonNode>> originalRecordFieldsIter =  originalRecordFields.fields();
            while(originalRecordFieldsIter.hasNext())
            {
                Entry<String, JsonNode> claimField = originalRecordFieldsIter.next();

                UserClaim claim = new UserClaim();
                claim.setAttributeKey(claimField.getKey());
                claim.setAttributeValue(claimField.getValue().toString());

                existingClaims.add(claim);
            }

        }
        */

        addOperation(patchRequest, existingPartyCode, modifiedRecord.getPartyCode(), "partyCode");

        addListValueOperations(patchRequest, existingClaims, modifiedRecord.getClaims(), t -> t.getAttributeKey(), v -> v.getAttributeValue(), "claims[type eq %s].value");
        addListValueOperations(patchRequest, originalRecord.getEmails(), modifiedRecord.getEmails(), t -> t.getType(), v -> v.getValue(), "emails[type eq %s].value");
        addListValueOperations(patchRequest, originalRecord.getPhoneNumbers(), modifiedRecord.getPhoneNumbers(), t -> t.getType(), v -> v.getValue(), "phoneNumbers[type eq %s].value");
        addListValueOperations(patchRequest, originalRecord.getAddresses(), modifiedRecord.getAddresses(), t -> t, v -> v, "addreses[type eq %s]");

        return patchRequest;
    }

    private static <T> void addListValueOperations(PatchRequest<UserRecord> patch, List<T> values1, List<T> values2,
            Function<T, Object> typeProp, Function<T, Object> valueProp, String path) {
        if (values2 != null) {
            for (T val2 : values2) {
                T existingEntry = null;
                if (values1 != null) {
                    existingEntry = values1
                            .stream()
                            .filter(v -> typeProp.apply(v).equals(typeProp.apply(val2)))
                            .findFirst().orElse(null);

                    if (existingEntry != null) {
                        values1.remove(existingEntry);
                    }
                }
                
                addOperation(patch, existingEntry == null ? null : valueProp.apply(existingEntry),
                        valueProp.apply(val2), String.format(path, typeProp.apply(val2)));
            }
        }

        if (values1 != null) {
            values1.forEach(v -> patch.addOperation(PatchOp.REMOVE, String.format(path, typeProp.apply(v)), valueProp.apply(v)));
        }
    }

    private static void addOperation(PatchRequest<UserRecord> patch, Object val1, Object val2, String path) {
        if (isNullOrEmpty(val1)) {
            if (!isNullOrEmpty(val2)) {
                patch.addOperation(PatchOp.ADD, path, val2);
            }
        } else {
            if (isNullOrEmpty(val2)) {
                patch.addOperation(PatchOp.REMOVE, path, val2);
            } else if (!val1.equals(val2)) {
                patch.addOperation(PatchOp.REPLACE, path, val2);
            }
        }

    }

    private static boolean isNullOrEmpty(Object val) {
        return val == null || (val instanceof String && ((String) val).trim().isEmpty());
    }
}
