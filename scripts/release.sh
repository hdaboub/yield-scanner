#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: $0 <version> [--push]

Examples:
  $0 v0.1.0
  $0 v0.1.1 --push

Notes:
- Version must match: vMAJOR.MINOR.PATCH (example: v1.2.3)
- Run from repo root with a clean working tree.
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 2
fi

VERSION="$1"
PUSH="false"
if [[ "${2:-}" == "--push" ]]; then
  PUSH="true"
fi

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Invalid version: $VERSION" >&2
  echo "Expected format: vMAJOR.MINOR.PATCH" >&2
  exit 2
fi

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Not in a git repository" >&2
  exit 2
fi

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree is not clean. Commit or stash changes first." >&2
  exit 2
fi

if git rev-parse "$VERSION" >/dev/null 2>&1; then
  echo "Tag already exists: $VERSION" >&2
  exit 2
fi

git tag -a "$VERSION" -m "Release $VERSION"
echo "Created tag $VERSION at $(git rev-parse --short HEAD)"

if [[ "$PUSH" == "true" ]]; then
  git push origin main
  git push origin "$VERSION"
  echo "Pushed main and tag $VERSION"
else
  echo "Tag created locally only."
  echo "Push with: git push origin main && git push origin $VERSION"
fi
