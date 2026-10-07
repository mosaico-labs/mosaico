#!/bin/bash

# ==============================================================================
# Mosaico SDK - Local Wheel Installation Test
#
# 1. Builds the SDK wheel with poetry
# 2. Creates a fresh virtual env for every Python version
# 3. Installs the wheel found in mosaico-sdk-py/dist
# 4. Runs the console scripts' --help and the test suite via `mosaicolabs.testing`
#
# Usage: test_sdk_publish.sh [options] [-- <mosaicolabs.testing args>]
#   --python "3.10 3.13"  Python versions to test (default: "3.10 3.12 3.13")
#   --skip-build          Reuse the wheel already in dist/
#   --skip-tests          Only install and check the console scripts
#   -h, --help            Show this help
#
# Arguments after `--` are forwarded to `mosaicolabs.testing`, e.g.:
#   test_sdk_publish.sh -- --host localhost --port 6276 -x
# ==============================================================================

set -euo pipefail

# --- Configuration ---
PYTHON_SDK_DIR="mosaico-sdk-py"
PYTHON_VERSIONS=("3.10" "3.12" "3.13")
CLI_SCRIPTS=("mosaicolabs.examples" "mosaicolabs.ros_injector" "mosaico" "mosaicolabs.testing")
# Keep in sync with [tool.poetry.group.dev.dependencies] in pyproject.toml
TEST_DEPS=("pytest>=8.4.2" "ruff>=0.15.8,<0.17.0")

# Resolve paths
FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(readlink -f "${FILE_DIR}/..")
PYTHON_SDK_PATH="${PROJECT_DIR}/${PYTHON_SDK_DIR}"
# The `testing` package is not shipped in the wheel, so it is copied here and
# exposed via PYTHONPATH. This dir sits at the same depth as `src/` so the
# relative paths used in conftest.py (e.g. the TLS cert) still resolve.
TESTING_STAGE_DIR="${PYTHON_SDK_PATH}/.wheel_test"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

SKIP_BUILD=false
SKIP_TESTS=false
TESTING_ARGS=()

usage() {
    sed -n '9,18p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# --- Argument Parsing ---

while [[ $# -gt 0 ]]; do
    case $1 in
        --python)
            read -r -a PYTHON_VERSIONS <<< "$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            TESTING_ARGS=("$@")
            break
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# --- Setup & Cleanup ---

VENVS_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mosaico_wheel_test.XXXXXX")

cleanup() {
    rm -rf "$VENVS_DIR" "$TESTING_STAGE_DIR"
}
trap cleanup EXIT

# --- Steps ---

build_wheel() {
    echo -e "${CYAN}--- Building wheel with poetry ---${NC}"
    cd "${PYTHON_SDK_PATH}"
    rm -rf dist/
    poetry build --format wheel
    cd "${PROJECT_DIR}"
}

find_wheel() {
    local wheels=("${PYTHON_SDK_PATH}"/dist/*.whl)
    if [[ ! -f "${wheels[0]}" ]]; then
        echo -e "${RED}Error: no wheel found in ${PYTHON_SDK_PATH}/dist${NC}" >&2
        exit 1
    fi
    if [[ ${#wheels[@]} -gt 1 ]]; then
        echo -e "${RED}Error: multiple wheels found in ${PYTHON_SDK_PATH}/dist${NC}" >&2
        exit 1
    fi
    echo "${wheels[0]}"
}

stage_testing_package() {
    rm -rf "$TESTING_STAGE_DIR"
    mkdir -p "$TESTING_STAGE_DIR"
    cp -R "${PYTHON_SDK_PATH}/src/testing" "$TESTING_STAGE_DIR/"
    find "$TESTING_STAGE_DIR" -name "__pycache__" -type d -prune -exec rm -rf {} +
}

# Runs every step for one Python version inside a fresh venv.
test_python_version() {
    local ver=$1
    local wheel=$2
    local venv="${VENVS_DIR}/py${ver}"
    local py="${venv}/bin/python"

    echo -e "${CYAN}Creating fresh venv...${NC}"
    "python${ver}" -m venv "$venv"
    "$py" -m pip install --quiet --upgrade pip

    echo -e "${CYAN}Installing $(basename "$wheel")[cli]...${NC}"
    "$py" -m pip install --no-cache-dir "${wheel}[cli]" "${TEST_DEPS[@]}"

    # Make sure the SDK is imported from the venv and not from the sources
    if ! "$py" -c "import sys, mosaicolabs; assert mosaicolabs.__file__.startswith(sys.prefix), mosaicolabs.__file__"; then
        echo -e "${RED}mosaicolabs is not imported from the venv${NC}"
        return 1
    fi

    echo -e "${CYAN}Verifying console scripts...${NC}"
    for script in "${CLI_SCRIPTS[@]}"; do
        if ! "${venv}/bin/${script}" --help > /dev/null; then
            echo -e "${RED}  ${script} --help FAILED${NC}"
            return 1
        fi
        echo -e "${GREEN}  ${script} --help OK${NC}"
    done

    if [[ "$SKIP_TESTS" == false ]]; then
        echo -e "${CYAN}Running mosaicolabs.testing ${TESTING_ARGS[*]-}...${NC}"
        # Run from the staging dir so nothing from src/ ends up on sys.path
        (cd "$TESTING_STAGE_DIR" && "${venv}/bin/mosaicolabs.testing" ${TESTING_ARGS[@]+"${TESTING_ARGS[@]}"})
    fi
}

# --- Main ---

if [[ "$SKIP_BUILD" == false ]]; then
    build_wheel
fi
WHEEL=$(find_wheel)
echo -e "${CYAN}Using wheel: ${WHEEL}${NC}"

stage_testing_package
export PYTHONPATH="$TESTING_STAGE_DIR"

PASSED=()
FAILED=()
SKIPPED=()

for VER in "${PYTHON_VERSIONS[@]}"; do
    echo -e "\n${YELLOW}=== Python ${VER} ===${NC}"

    if ! command -v "python${VER}" &> /dev/null; then
        echo -e "${RED}python${VER} not found. Skipping.${NC}"
        SKIPPED+=("$VER")
        continue
    fi

    # Run in a subshell so `set -e` aborts only the current version
    # (not inside an `if`, which would disable `set -e`)
    set +e
    (set -e; test_python_version "$VER" "$WHEEL")
    RC=$?
    set -e

    if [[ $RC -eq 0 ]]; then
        echo -e "${GREEN}Python ${VER}: SUCCESS${NC}"
        PASSED+=("$VER")
    else
        echo -e "${RED}Python ${VER}: FAILED${NC}"
        FAILED+=("$VER")
    fi
done

echo -e "\n${CYAN}--- Summary ---${NC}"
[[ ${#PASSED[@]} -gt 0 ]] && echo -e "${GREEN}Passed:  ${PASSED[*]}${NC}"
[[ ${#SKIPPED[@]} -gt 0 ]] && echo -e "${YELLOW}Skipped: ${SKIPPED[*]}${NC}"
[[ ${#FAILED[@]} -gt 0 ]] && echo -e "${RED}Failed:  ${FAILED[*]}${NC}"

if [[ ${#FAILED[@]} -gt 0 || ${#PASSED[@]} -eq 0 ]]; then
    exit 1
fi
echo -e "${GREEN}Local wheel tests completed successfully!${NC}"
