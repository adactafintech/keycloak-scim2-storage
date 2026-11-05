#!/usr/bin/env bash
# Reads and extends the list of supported Keycloak versions, used by keycloak-version-check.yml.
#
# Usage (from the repository root):
#   .github/scripts/keycloak-versions.sh list           prints the versions in the build.yml matrix, one per line
#   .github/scripts/keycloak-versions.sh add <version>  adds a version to the build.yml matrix (the single source of
#                                                       truth) and a keycloak-X-Y-Z profile to pom.xml for local builds
#
# Fails (exit 1) when a file does not have the expected shape, so the workflow never opens a half-edited PR.
set -euo pipefail

BUILD_YML=.github/workflows/build.yml
POM=pom.xml
MATRIX_KEY='^[[:space:]]*keycloak:[[:space:]]*$'
MATRIX_ITEM='^[[:space:]]*- [0-9]+[.][0-9]+[.][0-9]+[[:space:]]*$'

fail() {
  echo "$*" >&2
  exit 1
}

# The items of the first "keycloak:" list in build.yml (the build matrix).
list() {
  awk -v key="$MATRIX_KEY" -v item="$MATRIX_ITEM" '
    !found && $0 ~ key { found = 1; inlist = 1; next }
    inlist && $0 ~ item { v = $2; sub(/\r$/, "", v); print v; n++; next }
    inlist { exit }
    END { if (!n) exit 1 }
  ' "$BUILD_YML" || fail "$BUILD_YML: no keycloak: matrix with versions"
}

add() {
  local version=$1 versions sorted tmp
  versions=$(list)
  if grep -qxF "$version" <<< "$versions"; then
    fail "$version is already in $BUILD_YML"
  fi
  sorted=$(printf '%s\n' $versions "$version" | sort -V)

  # Rewrite the matrix items in version order, keeping their indentation.
  tmp=$(mktemp)
  awk -v key="$MATRIX_KEY" -v item="$MATRIX_ITEM" -v versions="$sorted" '
    !found && $0 ~ key { found = 1; inlist = 1; print; next }
    inlist && $0 ~ item {
      if (!printed) {
        match($0, /^[[:space:]]*/)
        indent = substr($0, 1, RLENGTH)
        eol = /\r$/ ? "\r" : ""
        n = split(versions, vs, "\n")
        for (i = 1; i <= n; i++) print indent "- " vs[i] eol
        printed = 1
      }
      next
    }
    { inlist = 0; print }
  ' "$BUILD_YML" > "$tmp"
  mv "$tmp" "$BUILD_YML"

  # Add a profile for local builds before </profiles>, unless it is already there (e.g. added by hand).
  local id="keycloak-${version//./-}" indent
  if ! grep -q "<id>$id</id>" "$POM"; then
    indent=$(grep -m1 '^[[:space:]]*</profiles>' "$POM" | sed 's/<.*//') || fail "$POM: no </profiles>"
    sed -i "/^[[:space:]]*<\/profiles>/i\\
$indent    <profile>\\
$indent        <id>$id</id>\\
$indent        <properties>\\
$indent            <keycloak.version>$version</keycloak.version>\\
$indent        </properties>\\
$indent    </profile>" "$POM"
  fi
}

case "${1:-}:${2:-}" in
  list:)
    list
    ;;
  add:*)
    [[ ${2:-} =~ ^[0-9]+\.[0-9]+\.[0-9]+$ && $# -eq 2 ]] || fail "usage: $0 list | add <major.minor.patch>"
    add "$2"
    ;;
  *)
    fail "usage: $0 list | add <major.minor.patch>"
    ;;
esac
