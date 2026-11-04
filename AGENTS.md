# Keycloak SCIM 2.0 Outbound Provisioning — Agent Instructions

This is the single instructions file for AI coding agents (Claude Code, GitHub Copilot and others that read `AGENTS.md`). Do not add a `CLAUDE.md` or `.github/copilot-instructions.md` next to it: Claude Code reads `AGENTS.md` only when there is no `CLAUDE.md`.

## Build & Test

These are the commands CI runs (`.github/workflows/build.yml`), once per supported Keycloak version.

```bash
# Build the deployable jar for one Keycloak version (tests are skipped by default)
mvn -B clean package -Dkeycloak.version=26.5.6
# Output: target/scim2-provisioning-keycloak-26.5.6-jar-with-dependencies.jar

# Build and run all tests (unit + integration) against one Keycloak version; requires Docker
mvn -B clean verify -Dkeycloak.version=26.5.6 -P run-integration-tests

# Run a single test class
mvn verify -Dkeycloak.version=26.5.6 -P run-integration-tests -Dtest=UserSyncTests
```

- Supported Keycloak versions: **24.0.4, 26.2.5, 26.5.6**. The single source of truth is the matrix in
  `.github/workflows/build.yml`; the default `keycloak.version` in `pom.xml` is 26.5.6. Check every change
  against all supported versions.
- Use `-Dkeycloak.version=<version>`. The old `keycloak-24` / `keycloak-26` / `keycloak-26-5-6` profiles
  still exist but CI does not use them.
- Tests are **skipped by default**. The `run-integration-tests` profile enables them and runs them in the
  `integration-test` phase (after `package`), because `UserSyncTests` mounts
  `target/*-jar-with-dependencies.jar` into a real Keycloak container via Testcontainers. So use
  `verify`, not `test`.
- Unit tests (`ScimHttpTransportTests`, `SkssStorageProviderFactoryTests`) use MockServer and do not need
  Docker, but they also only run with the profile.

## CI/CD

- `ci.yml` (pull requests): the build matrix plus a "Release preview" job that says whether merging will
  publish a release, and fails if the `pom.xml` version is lower than the highest tag.
- `release.yml` (push to `master`): when the `pom.xml` version is not tagged yet, creates the tag and a GitHub
  Release with one jar per Keycloak version.

## Architecture

This is a **Keycloak User Federation SPI extension** that propagates user/group lifecycle events from Keycloak to an external SCIM 2.0 service provider.

### Two package namespaces

- **`dev.suvera.keycloak.scim2.storage.*`** — Keycloak SPI integration (event listeners, JPA entities, sync job queue, storage provider)
- **`dev.suvera.scim2.*`** — Self-contained SCIM 2.0 schema model and HTTP client library (schemas, resource types, records, `Scim2Client` interface)

### Data flow

1. Keycloak user/group events are captured by `ScimEventListener` (admin events) and trigger calls to `JobEnqueuer`.
2. `JobEnqueuer` persists a `ScimSyncJobQueue` JPA entity (table `SCIM_SYNC_JOB_QUEUE`) representing a pending action.
3. `ScimSyncRunner` periodically polls pending jobs (where `processed` is 0–2) and executes them via `ScimSyncJob`.
4. `ScimSyncJob` resolves the job type and calls `ScimClient2` (backed by `Scim2ClientImpl`, which sends requests through `ScimHttpTransport` on `java.net.http.HttpClient`) to perform the SCIM operation.
5. On success the job is dequeued; on `ScimException` the `processed` counter is incremented (max retries = 2); other exceptions remove the job.

### SCIM HTTP client

- `Scim2ClientBuilder` builds the client: HTTP/1.1, connect and request timeouts (provider settings, defaults 10 s / 100 s), optional trust-all TLS (`allowSelfSigned`, an opt-in provider setting that applies to that client only).
- `ScimHttpTransport` adds the `Authorization` header and retries once with refreshed credentials after a 401. `OAuthTokenAuthorizer` gets and caches the token from `<authorityUrl>/protocol/openid-connect/token` (password grant when username and password are set, otherwise client credentials).

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

- **Fat jar deployment**: The artifact is a `jar-with-dependencies` jar deployed to `/opt/keycloak/providers/` (then `kc.sh build`). Libraries that every supported Keycloak version has on its runtime classpath are `provided` (all `keycloak-*`, Jackson, Guava, commons-lang3/io/collections4, JBoss Logging, …); only the rest is bundled. Before changing a scope, check the real runtime classpath of every supported version (`kc.sh build`, then `lib/quarkus/quarkus-application.dat`), not just the jars on disk.
- **maven-assembly-plugin stays at 3.6.0**: 3.7.0+ ignores the project's dependency mediation and bundles transitive copies of provided libraries.
- **SCIM client caching**: `ScimClient2Factory` maintains a `ConcurrentHashMap` of `ScimClient2` instances keyed on `ComponentModel.getId()`. Clients are rebuilt when the component's config hash (`ScimClient2.getClientHashFromModel`) changes; include new client-relevant settings in that hash.
- **Provider settings**: defined in `SkssStorageProviderFactory`; secrets use `ProviderConfigProperty.PASSWORD` with `.secret(true)` so Keycloak masks them in the admin API and exports. Validate settings in `validateConfiguration`.
- **Lombok on JPA entities**: `ScimSyncJobQueue` and `FederatedGroupAttributeEntity` use `@Data`/`@ToString` — avoid adding Lombok annotations that conflict with JPA (e.g., `@EqualsAndHashCode` on `@Entity`).
- **Job deduplication**: `enqueueJob` checks for an existing job with the same `userId`/`groupId`/`action` triple before persisting, preventing duplicate queued operations.
- **Namespaces**: Jakarta EE APIs are used under `jakarta.*` (no `javax.*` Jakarta EE imports; JDK packages such as `javax.net.ssl` are fine).
- **Logging**: Uses `org.jboss.logging.Logger` everywhere (provided by Keycloak); no SLF4J or Log4j.
- **Releases**: bump the `<version>` in `pom.xml` to publish a release on merge to `master`.
