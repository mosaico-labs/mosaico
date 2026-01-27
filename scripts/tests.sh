#!/usr/bin/env bash

# Mosaico Test Runner
#
# Usage:
#   ./scripts/tests.sh --mosaicod      # Docker env + mosaicod unit tests
#   ./scripts/tests.sh --sdk-python    # Python SDK unit tests (no docker)
#   ./scripts/tests.sh --integration   # Docker env + integration tests
#   ./scripts/tests.sh --all           # Run all tests (default)
#   ./scripts/tests.sh --help          # Show this help

set -euo pipefail

# Configuration
MOSAICOD_OUTPUT="/tmp/mosaicod_e2e_testing.out"
PYTHON_SDK_DIR="mosaico-sdk-py"
MOSAICOD_DIR="mosaicod"
DOCKER_DIR="docker/testing"
TEST_DIRECTORY="/tmp/__mosaico_auto_testing__"

# Environment variables (with defaults)
RUST_LOG="mosaico=trace"
MOSAICO_REPOSITORY_DB_URL="postgresql://postgres:password@localhost:6543/mosaico"
RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
SQLX_OFFLINE="true"

# Resolve paths
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
        docker compose down -v 2>/dev/null || true
    fi
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
Mosaico Test Runner

Usage: ./scripts/tests.sh [OPTIONS]

Options:
    --mosaicod      Run mosaicod unit tests (requires Docker for PostgreSQL)
    --sdk-python    Run Python SDK unit tests (no Docker required)
    --integration   Run integration tests (requires Docker + mosaicod build)
    --all           Run all tests (default)
    --help          Show this help message

Examples:
    ./scripts/tests.sh --mosaicod       # Run only backend tests
    ./scripts/tests.sh --sdk-python     # Run only Python SDK tests
    ./scripts/tests.sh --integration    # Run only integration tests
    ./scripts/tests.sh                  # Run all tests
EOF
}

start_docker() {
    if [ "$DOCKER_STARTED" = false ]; then
        DOCKER_STARTED=true
        title "docker" "." "${BLUE}"
        cd "${DOCKER_PATH}"
        docker compose up -d --wait 2>/dev/null
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
    title "mosaicod (unit tests)" "-"
    start_docker
    cd "${MOSAICOD_PATH}"
    cargo test --quiet
}

# Run Python SDK unit tests
run_sdk_python_tests() {
    title "python sdk (unit tests)" "-"
    install_python_deps
    cd "${PYTHON_SDK_PATH}"
    poetry run pytest ./src/testing -k unit
}

# Run integration tests
run_integration_tests() {
    title "integration tests" "-"
    start_docker
    install_python_deps

    # Build mosaicod
    title "mosaicod build" "." "${BLUE}"
    cd "${MOSAICOD_PATH}"
    cargo build

    # Create test directory
    mkdir -p "${TEST_DIRECTORY}"

    # Start mosaicod
    title "mosaicod startup" "." "${BLUE}"
    ./target/debug/mosaicod run --port 6276 --local-store "${TEST_DIRECTORY}" > "${MOSAICOD_OUTPUT}" 2>&1 &
    MOSAICOD_PID=$!
    echo "Starting mosaicod as background service (pid ${MOSAICOD_PID})"
    echo "Output: ${DIM}${MOSAICOD_OUTPUT}${RESET}"
    sleep 5

    # Run integration tests
    title "running integration tests" "." "${BLUE}"
    cd "${PYTHON_SDK_PATH}"
    poetry run pytest ./src/testing -k integration
}

# Main
main() {
    setup_colors

    local run_mosaicod=false
    local run_sdk_python=false
    local run_integration=false
    local run_all=false

    # Parse arguments
    if [ $# -eq 0 ]; then
        run_all=true
    else
        while [ $# -gt 0 ]; do
            case "$1" in
                --mosaicod)
                    run_mosaicod=true
                    shift
                    ;;
                --sdk-python)
                    run_sdk_python=true
                    shift
                    ;;
                --integration)
                    run_integration=true
                    shift
                    ;;
                --all)
                    run_all=true
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
    fi

    title "test runner" "#" "${GREEN}"

    # Print configuration
    title "setup" "-"
    echo "MOSAICO_REPOSITORY_DB_URL=${MOSAICO_REPOSITORY_DB_URL}"
    echo "DATABASE_URL=${DATABASE_URL}"
    echo "SQLX_OFFLINE=${SQLX_OFFLINE}"

    # Run selected tests
    if [ "$run_all" = true ]; then
        run_mosaicod_tests
        run_sdk_python_tests
        run_integration_tests
    else
        if [ "$run_mosaicod" = true ]; then
            run_mosaicod_tests
        fi
        if [ "$run_sdk_python" = true ]; then
            run_sdk_python_tests
        fi
        if [ "$run_integration" = true ]; then
            run_integration_tests
        fi
    fi

    title "all done" "#" "${GREEN}"
}

main "$@"
