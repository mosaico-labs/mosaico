#!/usr/bin/env bash

# Output file for the mosaicod background process
MOSAICOD_OUTPUT="/tmp/mosaicod_e2e_testing.out"
# Directory contianing the source code for the SDK
PYTHON_SDK_DIR="mosaico-sdk-py"
# Directory containing the mosaicod source code
MOSAICOD_DIR="mosaicod"
# Directory containing the docker compose.yml file for setup the test infra
DOCKER_DIR="docker/testing"
# This directory will be used to configure mosaicod store and will be deleted at the end of the process
TEST_DIRECTORY="/tmp/__mosaico_auto_testing__"
# Log level for mosaicod
RUST_LOG=mosaico=trace
# Database URL (see docker compose if you need to change the params)
MOSAICO_REPOSITORY_DB_URL="postgresql://postgres:password@localhost:6543/mosaico"
# USefull for rust crashes
RUST_BACKTRACE=1
# Set colored output 
TERM=xterm-256color

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
export TERM


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
}


trap error_handler ERR
trap cleanup EXIT

mkdir -p "${TEST_DIRECTORY}"


title "running test suite" "#" ${GREEN}

title "setup" "-"
cd ${DOCKER_PATH}
title "docker" "." ${BLUE}
cd ${PYTHON_SDK_PATH}
title "poetry" "." ${BLUE}
poetry install


title "mosaicod (unit tests)" "-"
cd ${MOSAICOD_PATH}
cargo test

title "python sdk (unit tests)" "-"
cd ${PYTHON_SDK_PATH}
poetry run pytest ./src/testing -k unit

title "integration tests" "-"
cd ${MOSAICOD_PATH}
title "mosaicod build" "." ${BLUE}
cargo build
./target/debug/mosaicod run --port 6276 --local-store "${TEST_DIRECTORY}" > "${MOSAICOD_OUTPUT}" 2>&1 &
MOSAICOD_PID=$!
title "mosaicod startup" "." ${BLUE}
echo "starting mosaicod as background service (pid ${MOSAICOD_PID}), output can be found in ${BOLD}${MOSAICOD_OUTPUT}${RESET}"
cd ${PYTHON_SDK_PATH}
title "running integration tests" "." ${BLUE}
poetry run pytest ./src/testing -k integration

title "all done" "#" ${GREEN}

