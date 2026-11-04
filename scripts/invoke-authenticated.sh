#!/bin/bash

# Manual smoke test from the original upstream project (2020): gets an admin token with the
# password grant, then calls the plugin's "sp-auth" realm REST endpoint (/realms/master/skss/sp-auth)
# once without and once with the token, expecting 401 and then 201, and finally lists the
# registered service providers.
#
# Kept for reference only: that endpoint was removed from the plugin in October 2020, and the
# /auth URL prefix is from the WildFly-based Keycloak. It does not work with the current plugin.

export DIRECT_GRANT_RESPONSE=$(curl -i --request POST http://localhost:8080/auth/realms/master/protocol/openid-connect/token --header "Accept: application/json" --header "Content-Type: application/x-www-form-urlencoded" --data "grant_type=password&username=admin&password=admin&client_id=admin-cli")

echo -e "\n\nSENT RESOURCE-OWNER-PASSWORD-CREDENTIALS-REQUEST. OUTPUT IS:\n\n";
echo $DIRECT_GRANT_RESPONSE;

export ACCESS_TOKEN=$(echo $DIRECT_GRANT_RESPONSE | grep "access_token" | sed 's/.*\"access_token\":\"\([^\"]*\)\".*/\1/g');
echo -e "\n\nACCESS TOKEN IS \"$ACCESS_TOKEN\"";

echo -e "\n\nSENDING UN-AUTHENTICATED REQUEST. THIS SHOULD FAIL WITH 401: ";
curl -i --request POST http://localhost:8080/auth/realms/master/skss/sp-auth --data "{ \"name\": \"scim2 sp\", \"endPoint\": \"http://localhost:9090/scim2\", \"username\": \"scim2\", \"password\": \"scim2\" }" --header "Content-type: application/json"

echo -e "\n\nSENDING AUTHENTICATED REQUEST. THIS SHOULD SUCCESSFULLY CREATE COMPANY AND SUCCESS WITH 201: ";
curl -i --request POST http://localhost:8080/auth/realms/master/skss/sp-auth --data "{ \"name\": \"scim2 sp\", \"endPoint\": \"http://localhost:9090/scim2\", \"username\": \"scim2\", \"password\": \"scim2\" }" --header "Content-type: application/json" --header "Authorization: Bearer $ACCESS_TOKEN";

echo -e "\n\nSEARCH SERVICE PROVIDERS: ";
curl -i --request GET http://localhost:8080/auth/realms/master/skss/sp-auth --header "Accept: application/json" --header "Authorization: Bearer $ACCESS_TOKEN";
