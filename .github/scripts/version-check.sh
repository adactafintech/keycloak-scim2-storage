#!/usr/bin/env bash
# Compares the pom version against existing tags in version order (2.12 > 2.9).
#
# Usage: .github/scripts/version-check.sh <pomVersion> [tag...]
# Prints "highest=<tag>" (empty when there are no tags) and exits 1 when pomVersion < highest tag.
#
# Ordering is GNU sort -V with "-" read as "~", so a qualifier sorts before its release
# (3.0-rc1 < 3.0 < 3.1), as in Maven.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: $0 <pomVersion> [tag...]" >&2; exit 1; }
pom=$1
shift

# Sorts versions ascending.
version_sort() {
  tr '-' '~' | sort -V | tr '~' '-'
}

highest=""
if [ $# -gt 0 ]; then
  highest=$(printf '%s\n' "$@" | version_sort | tail -n 1)
fi
echo "highest=$highest"

if [ -n "$highest" ] && [ "$pom" != "$highest" ] \
   && [ "$(printf '%s\n' "$pom" "$highest" | version_sort | tail -n 1)" = "$highest" ]; then
  exit 1
fi
