#!/usr/bin/env bash

# Run a development enviroment DB + mosaicod for local testing


# Directory contianing the source code for the SDK
PYTHON_SDK_DIR="mosaico-sdk-py"
# Directory containing the mosaicod source code
MOSAICOD_DIR="mosaicod"
# Directory containing the docker compose.yml file for setup the test infra
DOCKER_DIR="docker/testing"
# This directory will be used to configure mosaicod store and will be deleted at the end of the process
TEST_DIRECTORY="/tmp/__mosaico_dev_env__"
# Log level for mosaicod
RUST_LOG=mosaico=trace
# Database URL (see docker compose if you need to change the params)
MOSAICO_REPOSITORY_DB_URL="postgresql://postgres:password@localhost:6543/mosaico"

# This flag should be always `true`, otherwise a running database with live migration
# is required to compile the code (and also we need to reinstall sqlx at each run).
SQLX_OFFLINE=true

FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(readlink -f "${FILE_DIR}/..")
DATABASE_URL="${MOSAICO_REPOSITORY_DB_URL}"
MOSAICOD_PATH="${PROJECT_DIR}/${MOSAICOD_DIR}"
PYTHON_SDK_PATH="${PROJECT_DIR}/${PYTHON_SDK_DIR}"
DOCKER_PATH="${PROJECT_DIR}/${DOCKER_DIR}"

export DATABASE_URL
export RUST_LOG
export SQLX_OFFLINE
export MOSAICO_REPOSITORY_DB_URL
export RUST_BACKTRACE


COLS=$(tput cols || echo 80 )
if (( "$COLS" < 70 )); then
    COLS=70
fi

RED=$(tput setaf 208)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
BLUE=$(tput setaf 4)
MAGENTA=$(tput setaf 5)
RESET=$(tput sgr0)
BOLD=$(tput bold)
DIM=$(tput dim)

error_handler() {
    echo "${RED}${BOLD}Ops, an error occurred. ${RESET}"
    exit 1
}

title() {
    local text="$1"
    local char="${2:-#}"
    local color="${3:-${MAGENTA}}"

    local style_col="${BOLD}"
    local padding=$(( (COLS - ${#text} - 2) / 2 ))
    local line

    # Build line of padding characters
    printf -v line "%*s" "$COLS" ""
    line=${line// /$char} 

    # Print the title
    printf "%s%s%.*s %s %.*s%s\n" \
        "$color" "$style_col" \
        "$padding" "$line" "$text" "$padding" "$line" "${RESET}"
}

function cleanup() {
    if [ -n "$MOSAICOD_PID" ]; then
        kill "$MOSAICOD_PID" 2>/dev/null
        wait "$MOSAICOD_PID" 2>/dev/null
        echo "${DIM}mosaicod ($MOSAICOD_PID) terminated.${RESET}"
    fi
    
    rm -r ${TEST_DIRECTORY} 2> /dev/null

    cd ${PROJECT_DIR}/docker/testing
    docker compose down -v 2> /dev/null

    title "db + storage deleted" "#" ${YELLOW}
}


trap error_handler ERR
trap cleanup EXIT

mkdir -p "${TEST_DIRECTORY}"


title "development environment" "#" ${GREEN}

title "setup" "-"
echo "MOSAICO_REPOSITORY_DB_URL ${MOSAICO_REPOSITORY_DB_URL}"
echo "DATABASE_URL=${DATABASE_URL}"
echo "SQLX_OFFLINE=${SQLX_OFFLINE}"
cd ${DOCKER_PATH}
title "docker" "." ${BLUE}
docker compose up -d --wait 2> /dev/null
echo "Started ${BOLD}docker/testing${RESET} compose file"

title "mosaicod" "-"
cd ${MOSAICOD_PATH}
title "build" "." ${BLUE}
cargo build
./target/debug/mosaicod run --port 6276 --local-store "${TEST_DIRECTORY}"
MOSAICOD_PID=$!

title "done" "#" ${GREEN}

