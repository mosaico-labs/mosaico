#!/bin/bash
#
# Prints the changelog of mosaicod or the Python SDK from the conventional
# commits since the previous release, using git-cliff and scripts/workflows/cliff.toml.

set -euo pipefail

FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$( cd -- "${FILE_DIR}/../.." &> /dev/null && pwd )
VERSION_TOOL="${FILE_DIR}/version.sh"

show_help() {
    cat << EOF
Mosaico changelog tool. Prints the changelog of a component since its previous release.

Usage: changelog.sh (--mosaicod | --py) [<range>]

Options:
    --mosaicod                  Changelog of mosaicod
    --py                        Changelog of the Python SDK
    <range>                     Commits to include (default: previous release..HEAD)
    --help                      Show this help message

Requires git-cliff.
EOF
}

COMPONENT=""
RANGE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mosaicod) COMPONENT="mosaicod" ;;
        --py) COMPONENT="py" ;;
        --help|-h) show_help; exit 0 ;;
        -*) echo "Error: unknown option '$1'."; exit 1 ;;
        *) [[ -z "$RANGE" ]] || { echo "Error: too many arguments."; exit 1; }; RANGE="$1" ;;
    esac
    shift
done

if [[ -z "$COMPONENT" ]]; then
    show_help
    exit 1
fi

command -v git-cliff &> /dev/null || { echo "Error: git-cliff is required (https://git-cliff.org/docs/installation)."; exit 1; }

cd "$PROJECT_DIR"

if [[ "$COMPONENT" == "mosaicod" ]]; then
    TAG_PREFIX="v"
    INCLUDE_PATH="mosaicod/**"
else
    TAG_PREFIX="py/v"
    INCLUDE_PATH="mosaico-sdk-py/**"
fi

# The version being released: the current one without -rc or -dev.
CURRENT=$("$VERSION_TOOL" get "--${COMPONENT}")
[[ "$CURRENT" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-rc|-dev)?$ ]] || { echo "Error: unexpected version '$CURRENT'."; exit 1; }
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"
VERSION="${MAJOR}.${MINOR}.${PATCH}"

# Default range: from the previous release of the component to HEAD.
if [[ -z "$RANGE" ]]; then
    if (( PATCH > 0 )); then
        RANGE="${TAG_PREFIX}${MAJOR}.${MINOR}.$((PATCH - 1))..HEAD"
    else
        RANGE="$(git merge-base HEAD "origin/release/v${MAJOR}.$((MINOR - 1))")..HEAD"
    fi
fi

# The GitHub API only knows commit SHAs, not local refs like HEAD or origin/...
FROM=$(git rev-parse --verify --quiet "${RANGE%%..*}^{commit}") || { echo "Error: '${RANGE%%..*}' not found. Run 'git fetch --tags' or pass a range."; exit 1; }
TO=$(git rev-parse --verify --quiet "${RANGE##*..}^{commit}") || { echo "Error: '${RANGE##*..}' not found."; exit 1; }

MOSAICO_COMPONENT="$COMPONENT" git-cliff \
    --config "${FILE_DIR}/cliff.toml" \
    --include-path "$INCLUDE_PATH" \
    --tag-pattern "^${TAG_PREFIX}[0-9]" \
    --tag "${TAG_PREFIX}${VERSION}" \
    "${FROM}..${TO}"
