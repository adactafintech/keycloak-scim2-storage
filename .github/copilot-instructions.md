# Keycloak SCIM 2.0 Outbound Provisioning — Copilot Instructions

## Build & Test

```bash
# Standard build (tests skipped by default)
mvn clean install

# Build targeting a specific Keycloak version
mvn clean package -P keycloak-24
mvn clean package -P keycloak-26
mvn clean package -P keycloak-26-5-6

# Run integration tests (requires Docker)
mvn test -P run-integration-tests

# Run a single integration test class
mvn test -P run-integration-tests -Dtest=UserSyncTests

# Build the deployable fat jar
mvn package
# Output: target/scim2-provisioning-keycloak-<version>-jar-with-dependencies.jar
```

Tests are **skipped by default** in the surefire config. The integration tests use Arquillian + Testcontainers and spin up a real Keycloak Docker container, so Docker must be available.

## Architecture

This is a **Keycloak User Federation SPI extension** that propagates user/group lifecycle events from Keycloak to an external SCIM 2.0 service provider.

### Two package namespaces

- **`dev.suvera.keycloak.scim2.storage.*`** — Keycloak SPI integration (event listeners, JPA entities, sync job queue, storage provider)
- **`dev.suvera.scim2.*`** — Self-contained SCIM 2.0 schema model and HTTP client library (schemas, resource types, records, `Scim2Client` interface)

### Data flow

1. Keycloak user/group events are captured by `ScimEventListener` (admin events) and trigger calls to `JobEnqueuer`.
2. `JobEnqueuer` persists a `ScimSyncJobQueue` JPA entity (table `SCIM_SYNC_JOB_QUEUE`) representing a pending action.
3. `ScimSyncRunner` periodically polls pending jobs (where `processed` is 0–2) and executes them via `ScimSyncJob`.
4. `ScimSyncJob` resolves the job type and calls `ScimClient2` (backed by `Scim2ClientImpl` over OkHttp) to perform the SCIM operation.
5. On success the job is dequeued; on `ScimException` the `processed` counter is incremented (max retries = 2); other exceptions remove the job.

### SPI registration

All Keycloak SPI providers are registered via Java SPI in `src/main/resources/META-INF/services/`:
- `UserStorageProviderFactory` → `SkssStorageProviderFactory` (provider ID: `skss-scim2-storage`)
- `EventListenerProviderFactory` → `ScimEventListenerProviderFactory`
- `JpaEntityProviderFactory` → `SkssJpaEntityProviderFactory`
- `LDAPStorageMapperFactory` → `LDAPEventMapperFactory`
- `IdentityProviderMapper` → `IdentityProviderEventMapper`

### Database schema

Managed via Liquibase (`src/main/resources/META-INF/skss-changelog.xml`). Custom tables:
- `SCIM_SYNC_JOB_QUEUE` — async job queue with retry tracking
- `FED_GROUP_ATTRIBUTE` — group attribute storage for federation-linked groups

## Key Conventions

- **Fat jar deployment**: The artifact is a `jar-with-dependencies` fat jar deployed to `$KEYCLOAK_HOME/providers/`. Keycloak itself provides `keycloak-*` dependencies at runtime (all scoped `provided` in pom.xml).
- **SCIM client caching**: `ScimClient2Factory` maintains a `ConcurrentHashMap` of `ScimClient2` instances keyed on `ComponentModel.getId()`. Clients are invalidated when the component's config hash changes.
- **Lombok on JPA entities**: `ScimSyncJobQueue` and `FederatedGroupAttributeEntity` use `@Data`/`@ToString` — avoid adding Lombok annotations that conflict with JPA (e.g., `@EqualsAndHashCode` on `@Entity`).
- **Job deduplication**: `enqueueJob` checks for an existing job with the same `userId`/`groupId`/`action` triple before persisting, preventing duplicate queued operations.
- **Multi-version support**: Keycloak version is set via Maven profiles. The default version in `pom.xml` is `26.5.6`. When adding APIs, check compatibility across supported versions.
- **javax → jakarta migration**: A `transform-jakarta` Maven profile exists to transform the `dev.suvera.keycloak` package if needed. Source code uses `jakarta.*` namespaces; a stub `javax.mail.internet` is in-tree to satisfy legacy dependencies.
- **Logging**: Uses `org.jboss.logging.Logger` (not SLF4J or Log4j directly) in Keycloak-facing classes, and Log4j 2 for the standalone SCIM client layer.
