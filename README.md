# Keycloak SCIM 2.0 outbound user provisioning

**Keycloak** is an open source Identity and Access management system for modern applications and services.

more info https://github.com/keycloak/keycloak


This is extension to keycloak, where it provides capability of user provisioning to external scim service providers from keycloak identity server.


## Installation

Download the jar for your Keycloak version from
[GitHub Releases](https://github.com/adactafintech/keycloak-scim2-storage/releases/latest).
Each release has one `scim2-provisioning-keycloak-<keycloak-version>-jar-with-dependencies.jar`
per supported Keycloak version (currently 24.0.4, 26.2.5 and 26.5.6).

```
# Take a backup of your Keycloak DB, this extension adds new tables.

# Copy the jar into the Keycloak providers directory
cp -f scim2-provisioning-keycloak-26.5.6-jar-with-dependencies.jar /opt/keycloak/providers/

# Rebuild and restart Keycloak
/opt/keycloak/bin/kc.sh build
```

### Building from source

```
mvn clean package -Dkeycloak.version=26.5.6
# -> target/scim2-provisioning-keycloak-26.5.6-jar-with-dependencies.jar

# Integration tests (need Docker; they start Keycloak with the built jar)
mvn verify -Dkeycloak.version=26.5.6 -P run-integration-tests
```

The `keycloak-24`, `keycloak-26` and `keycloak-26-5-6` profiles still work for local builds.

### Releases

A release is published when a merge to `master` changes the version in `pom.xml`
to one that is not tagged yet. The supported Keycloak versions are listed in
`.github/workflows/build.yml`.

### Is your server is compliant to SCIM 2.0?

Here is the tool to test the compliance level  https://github.com/suvera/scim2-compliance-test-utility


## User Provisioning Steps 

1) Login to Keyclock as "admin"

2) Select "User Federation" Add SCIM 2.0 Service Provider 

![User Federation](https://suvera.github.io/assets/images/scim2_storage_01.png)


3) Add new SCIM 2.0 User federation 

    - **Bearer Token** Authentication also supported,  though it's not shown in the screenshot.

![Add SCIM 2.0 User Federation](https://suvera.github.io/assets/images/scim2_storage_02.png)


4) Now go to "Users -> Add New User" 

![Add New User](https://suvera.github.io/assets/images/scim2_storage_03.png)


5) That's it!, User will be added to your SCIM 2.0 Service provider too. 

