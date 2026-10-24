#!/bin/bash
#
# Reads, sets and checks the version of mosaicod and the Python SDK, and parses
# release branch names.

set -euo pipefail

FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$( cd -- "${FILE_DIR}/../.." &> /dev/null && pwd )
CARGO_TOML="mosaicod/Cargo.toml"
PYPROJECT="mosaico-sdk-py/pyproject.toml"

show_help() {
    cat << EOF
Mosaico version tool. Reads, sets and checks the version of mosaicod and the Python SDK, and parses release branch names.

Usage: version.sh <COMMAND> [OPTIONS] <ARG>

Commands:

    get [OPTIONS] [<git-ref>]
        Print the versions, read from the working tree or from <git-ref>. Without options, prints one line per component: 'mosaicod <version>' and 'python <version>'.
        --mosaicod              Only print the mosaicod version number
        --py                    Only print the Python SDK version number

    branch [OPTIONS] <branch>
        Print a part of a release-cut/vX.Y or release/vX.Y branch name. Fails for any other branch.
        --kind                  Print 'release-cut' or 'release' (default)
        --major                 Print X
        --minor                 Print Y

    set [OPTIONS] <X.Y.Z[-dev|-rc]>
        Set the version and refresh the lock files. Without options, sets both components.
        --mosaicod              Only set mosaicod
        --py                    Only set the Python SDK

    check [OPTIONS] <X.Y.Z[-dev|-rc]>
        Change nothing. Fail unless the files are on this version and the lock files are in sync. Without options, checks both components.
        --mosaicod              Only check mosaicod
        --py                    Only check the Python SDK

    --help                      Show this help message
EOF
}

die() {
    echo "Error: $1" >&2
    exit 1
}

# The file holding the version of a component. In Cargo.toml the first
# `version = "..."` is the [workspace.package] version shared by every crate.
version_file() {
    case "$1" in
        mosaicod) echo "mosaicod/Cargo.toml" ;;
        py) echo "mosaico-sdk-py/pyproject.toml" ;;
    esac
}

# Prints the version of a component, read from the working tree or from a git ref.
read_version() {
    local file content
    file=$(version_file "$1")
    if [[ -n "${2:-}" ]]; then
        content=$(git -C "$PROJECT_DIR" show "${2}:${file}") || die "cannot read $file at $2."
    else
        content=$(< "${PROJECT_DIR}/${file}")
    fi
    grep -m1 '^version = ' <<< "$content" | cut -d '"' -f 2 || die "no version found in $file."
}

# Refreshes the lock file of a component, or with a second argument only checks
# that it is in sync. In Cargo.lock only the workspace crates are updated.
sync_lock() {
    case "$1" in
        mosaicod) cargo update --workspace --quiet ${2:+--locked} --manifest-path "${PROJECT_DIR}/mosaicod/Cargo.toml" ;;
        py) uv lock --quiet ${2:+--check} --project "${PROJECT_DIR}/mosaico-sdk-py" ;;
    esac
}

# A command, then options and one argument in any order.
COMMAND="${1:-}"
[[ $# -eq 0 ]] || shift
COMPONENTS=(mosaicod py)
FIELD="kind"
ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mosaicod) COMPONENTS=(mosaicod) ;;
        --py) COMPONENTS=(py) ;;
        --kind|--major|--minor) FIELD="${1#--}" ;;
        -*) die "unknown option '$1'." ;;
        *) [[ -z "$ARG" ]] || die "too many arguments."; ARG="$1" ;;
    esac
    shift
done

if [[ "$COMMAND" == set || "$COMMAND" == check ]]; then
    [[ "$ARG" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-dev|-rc)?$ ]] \
        || die "invalid version '$ARG' (expected X.Y.Z, X.Y.Z-dev or X.Y.Z-rc)."
fi

case "$COMMAND" in
    get)
        if [[ ${#COMPONENTS[@]} -eq 1 ]]; then
            read_version "${COMPONENTS[0]}" "$ARG"
        else
            mosaicod=$(read_version mosaicod "$ARG")
            python=$(read_version py "$ARG")
            echo "mosaicod $mosaicod"
            echo "python $python"
        fi
        ;;
    branch)
        [[ "$ARG" =~ ^(release-cut|release)/v([0-9]+)\.([0-9]+)$ ]] \
            || die "'$ARG' is not a release-cut/vX.Y or release/vX.Y branch."
        case "$FIELD" in
            kind) echo "${BASH_REMATCH[1]}" ;;
            major) echo "${BASH_REMATCH[2]}" ;;
            minor) echo "${BASH_REMATCH[3]}" ;;
        esac
        ;;
    set)
        for component in "${COMPONENTS[@]}"; do
            file=$(version_file "$component")
            perl -0pi -e "s/^version = \"[^\"]*\"/version = \"${ARG}\"/m" "${PROJECT_DIR}/${file}"
            sync_lock "$component"
            echo "$file set to $ARG"
        done
        ;;
    check)
        failed=false
        for component in "${COMPONENTS[@]}"; do
            file=$(version_file "$component")
            current=$(read_version "$component")
            if [[ "$current" != "$ARG" ]]; then
                echo "Error: $file is on $current, not $ARG." >&2
                failed=true
            elif ! sync_lock "$component" --check; then
                echo "Error: the lock file of $file is not in sync with $ARG." >&2
                failed=true
            fi
        done
        [ "$failed" = false ] || exit 1
        echo "Version files are on $ARG and lock files are in sync"
        ;;
    --help|-h|"")
        show_help
        ;;
    *)
        die "unknown command '$COMMAND'. Run 'version.sh --help'."
        ;;
esac
