#!/usr/bin/env bash

set -euo pipefail

# Configuration
MOSAICOD_OUTPUT="/tmp/mosaicod_e2e_testing.out"
PYTHON_SDK_DIR="mosaico-sdk-py"
MOSAICOD_DIR="mosaicod"
DOCKER_DIR="docker/testing"
TEST_DIRECTORY="/tmp/__mosaico_auto_testing__"

# Environment variables (with defaults)
RUST_LOG="mosaico=trace"
MOSAICOD_DB_URL="postgresql://postgres:password@localhost:6543/mosaico"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
SQLX_OFFLINE="true"

# Resolve paths
FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(readlink -f "${FILE_DIR}/..")
DATABASE_URL="${MOSAICOD_DB_URL}"
MOSAICOD_PATH="${PROJECT_DIR}/${MOSAICOD_DIR}"
PYTHON_SDK_PATH="${PROJECT_DIR}/${PYTHON_SDK_DIR}"
DOCKER_PATH="${PROJECT_DIR}/${DOCKER_DIR}"

export DATABASE_URL
export RUST_LOG
export SQLX_OFFLINE
export MOSAICOD_DB_URL
export RUST_BACKTRACE

# Colors (with fallback for non-interactive terminals)
setup_colors() {
    if [ -t 1 ]; then
        COLS=$(tput cols || echo 80)
        RED=$(tput setaf 1)
        GREEN=$(tput setaf 2)
        YELLOW=$(tput setaf 3)
        BLUE=$(tput setaf 4)
        MAGENTA=$(tput setaf 5)
        RESET=$(tput sgr0)
        BOLD=$(tput bold)
        DIM=$(tput dim)
    else
        COLS=80
        RED=""
        GREEN=""
        YELLOW=""
        BLUE=""
        MAGENTA=""
        RESET=""
        BOLD=""
        DIM=""
    fi
    if (( COLS < 70 )); then
        COLS=70
    fi
}

# State tracking
MOSAICOD_PID=""
DOCKER_STARTED=false

# Cleanup function
cleanup() {
    if [ -n "$MOSAICOD_PID" ]; then
        kill "$MOSAICOD_PID" 2>/dev/null || true
        wait "$MOSAICOD_PID" 2>/dev/null || true
        echo "${DIM}mosaicod ($MOSAICOD_PID) terminated.${RESET}"
    fi

    rm -rf "${TEST_DIRECTORY}" 2>/dev/null || true

    if [ "$DOCKER_STARTED" = true ]; then
        cd "${DOCKER_PATH}"
        eval "docker compose down -v $REDIRECT_TO_NULL || true"
    fi

    MOSAICOD_PID=""
    DOCKER_STARTED=false
}

trap cleanup EXIT

# Print title
title() {
    local text="$1"
    local char="${2:-#}"
    local color="${3:-${MAGENTA}}"
    local padding=$(( (COLS - ${#text} - 2) / 2 ))
    local line
    printf -v line "%*s" "$COLS" ""
    line=${line// /$char}
    printf "%s%s%.*s %s %.*s%s\n" \
        "$color" "$BOLD" \
        "$padding" "$line" "$text" "$padding" "$line" "${RESET}"
}

# Error handler
error_handler() {
    echo "${RED}${BOLD}Error occurred. Exiting.${RESET}"
    exit 1
}

trap error_handler ERR

# Show help
show_help() {
    cat << EOF
Mosaico test runner.

Usage: tests.sh [OPTIONS]

Options:
    --mosaicod                  Run mosaicod unit tests (requires Docker for PostgreSQL)
    --sdk-python                Run Python SDK unit tests (no Docker required)
    --full-stack                Run full-stack tests (requires Docker + mosaicod build)
    --full-stack-tls            Run full-stack tests with TLS (requires Docker + mosaicod build)
    --full-stack-api-key        Run full-stack tests on API KEY (requires Docker + mosaicod build)
    --all                       Run all tests (default)
    --help                      Show this help message
EOF
}

start_docker() {
    if [ "$DOCKER_STARTED" = false ]; then
        DOCKER_STARTED=true
        title "docker" "." "${BLUE}"
        cd "${DOCKER_PATH}"
        eval "docker compose up -d --wait $REDIRECT_TO_NULL"
        echo "Started ${BOLD}docker/testing${RESET} compose file"
    fi
}

# Install Python dependencies
install_python_deps() {
    title "poetry" "." "${BLUE}"
    cd "${PYTHON_SDK_PATH}"
    poetry install
}

# Run mosaicod unit tests
run_mosaicod_tests() {
    start_docker
    title "mosaicod (unit tests)" "-"
    cd "${MOSAICOD_PATH}"

    if $VERBOSE; then
        cargo test
    else
        echo "Compiling, it may take some time ..."
        cargo test --quiet
    fi
}

# Run Python SDK unit tests
run_sdk_python_tests() {
    title "python sdk (unit tests)" "-"
    install_python_deps
    cd "${PYTHON_SDK_PATH}"
    poetry run pytest ./src/testing -k unit
}


# Run full_stack tests
#
# Usage:
#  run_full_stack_tests --title "title" --tls
run_full_stack_tests() {

    MOSAICOD_OPTS=""
    PYTEST_OPTS_K="integration and not test_tls_connection"
    PYTEST_OPTS=""
    GENERATE_API_KEY=false
    TITLE="N/A"

    while [ $# -gt 0 ]; do
        case "$1" in
            --tls)
                MOSAICOD_OPTS="--tls"
                PYTEST_OPTS_K="integration"
                PYTEST_OPTS="--tls"

                export MOSAICOD_TLS_CERT_FILE="${MOSAICOD_PATH}/tests/data/cert.pem"
                export MOSAICOD_TLS_PRIVATE_KEY_FILE="${MOSAICOD_PATH}/tests/data/key.pem"
                export MOSAICO_TLS_CERT_FILE="${MOSAICOD_PATH}/tests/data/ca.pem"

                shift 
                ;;
            --api-key)
                MOSAICOD_OPTS="--api-key"
                PYTEST_OPTS_K="integration"
                GENERATE_API_KEY=true

                shift 
                ;;
            --title)
                TITLE="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done

    cleanup
    start_docker
    title "[ ${TITLE} ]" "-"
    install_python_deps

    # Build mosaicod
    title "mosaicod build" "." "${BLUE}"
    cd "${MOSAICOD_PATH}"

    if $VERBOSE; then
        cargo build --bin mosaicod
    else
        echo "Compiling mosaicod, it may take a while ..."
        cargo build --bin mosaicod 2>&1
    fi

    if $GENERATE_API_KEY; then
        MOSAICO_API_KEY_MANAGE=$(RUST_LOG="" ${MOSAICOD_PATH}/target/debug/mosaicod api-key create -p manage)
        PYTEST_OPTS="--api-key ${MOSAICO_API_KEY_MANAGE}"
    fi

    # Create test directory
    mkdir -p "${TEST_DIRECTORY}"

    # Start mosaicod
    title "mosaicod startup" "." "${BLUE}"
    ./target/debug/mosaicod run --port 6276 --local-store "${TEST_DIRECTORY}" ${MOSAICOD_OPTS} > "${MOSAICOD_OUTPUT}" 2>&1 &
    MOSAICOD_PID=$!
    echo "Starting mosaicod as background service (pid ${MOSAICOD_PID})"
    echo "mosaicod logs can be found in ${DIM}${MOSAICOD_OUTPUT}${RESET}"
    echo "Waiting starver to startup ..."
    sleep 1

    # Run integration tests
    title "running integration tests" "." "${BLUE}"
    cd "${PYTHON_SDK_PATH}"

    poetry run pytest ./src/testing -k "${PYTEST_OPTS_K}" ${PYTEST_OPTS}
}

VERBOSE=false
REDIRECT_TO_NULL="2>/dev/null"

# Main
main() {
    setup_colors

    local run_mosaicod=false
    local run_sdk_python=false
    local run_full_stack=false
    local run_full_stack_tls=false
    local run_full_stack_api_key=false
    local run_all=false
    local run_selected=false # true if at least a run option is selected

    while [ $# -gt 0 ]; do
        case "$1" in
            --verbose|-v)
                VERBOSE=true
                REDIRECT_TO_NULL=""
                shift
                ;;
            --mosaicod)
                run_mosaicod=true
                run_selected=true
                shift
                ;;
            --sdk-python)
                run_sdk_python=true
                run_selected=true
                shift
                ;;
            --full-stack)
                run_full_stack=true
                run_selected=true
                shift
                ;;
            --full-stack-tls)
                run_full_stack_tls=true
                run_selected=true
                shift
                ;;
            --full-stack-api-key)
                run_full_stack_api_key=true
                run_selected=true
                shift
                ;;
            --all)
                run_all=true
                run_selected=true
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                echo "${RED}Unknown option: $1${RESET}"
                show_help
                exit 1
                ;;
        esac
    done

    # If no option was specified
    if [[ "$run_selected" == "false" ]]; then
        run_all=true
    fi

    title "test runner" "#" "${GREEN}"

    # Print configuration
    title "[ setup ]" "-"
    echo " * MOSAICOD_DB_URL ${DIM}${MOSAICOD_DB_URL}${RESET}"
    echo " * DATABASE_URL    ${DIM}${DATABASE_URL}${RESET}"
    echo " * SQLX_OFFLINE    ${DIM}${SQLX_OFFLINE}${RESET}"

    # Run selected tests
    if [ "$run_all" = true ]; then
        run_mosaicod_tests
        run_sdk_python_tests
        run_full_stack_tests --title "full-stack tests"
        run_full_stack_tests --title "full-stack tests (TLS)" --tls
        run_full_stack_tests --title "full-stack tests (API KEY)" --api-key
    else
        if [ "$run_mosaicod" = true ]; then
            run_mosaicod_tests
        fi
        if [ "$run_sdk_python" = true ]; then
            run_sdk_python_tests
        fi
        if [ "$run_full_stack" = true ]; then
            run_full_stack_tests --title "full-stack tests" 
        fi
        if [ "$run_full_stack_tls" = true ]; then
            run_full_stack_tests --title "full-stack tests (TLS)" --tls
        fi
        if [ "$run_full_stack_api_key" = true ]; then
            run_full_stack_tests --title "full-stack tests (API KEY)" --api-key
        fi
    fi

    title "all done" "#" "${GREEN}"
}

main "$@"
