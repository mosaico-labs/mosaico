#!/bin/bash

# ==============================================================================
# Mosaico SDK - Test Publication & Installation Script
# Flags: --local, --remote, --all
# ==============================================================================

set -e

# --- Configuration ---
PYTHON_SDK_DIR="mosaico-sdk-py"
PACKAGE_NAME="mosaicolabs" # The name for pip install
PYTHON_VERSIONS=("3.10" "3.12" "3.13")
TEST_PYPI_URL="https://test.pypi.org/simple/"

# Resolve paths
FILE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_DIR=$(readlink -f "${FILE_DIR}/..")
PYTHON_SDK_PATH="${PROJECT_DIR}/${PYTHON_SDK_DIR}"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

# --- Helper Functions ---

# Function to run Local Smoke Tests
run_local_tests() {
    echo -e "${CYAN}--- Starting LOCAL Installation Tests ---${NC}"
    cd "${PYTHON_SDK_PATH}"
    
    echo -e "${CYAN}Building local wheel...${NC}"
    rm -rf dist/
    poetry build

    cd "${PROJECT_DIR}" # Move out for isolation

    for VER in "${PYTHON_VERSIONS[@]}"; do
        echo -e "\n${YELLOW}Testing Local Wheel on Python $VER${NC}"
        
        if ! command -v "python$VER" &> /dev/null; then
            echo -e "${RED}Error: python$VER not found. Skipping.${NC}"
            continue
        fi

        VENV_NAME=".venv_local_test_$VER"
        "python$VER" -m venv "$VENV_NAME"
        "$VENV_NAME/bin/pip" install --upgrade pip
        
        if "$VENV_NAME/bin/pip" install --no-cache-dir ${PYTHON_SDK_PATH}/dist/*.whl; then
            echo -e "${GREEN}Verifying CLI scripts...${NC}"
            "$VENV_NAME/bin/mosaicolabs.examples" --help > /dev/null
            "$VENV_NAME/bin/mosaicolabs.ros_injector" --help > /dev/null
            echo -e "${GREEN}Python $VER: LOCAL SUCCESS${NC}"
        else
            echo -e "${RED}Python $VER: LOCAL INSTALLATION FAILED${NC}"
            rm -rf "$VENV_NAME"
            exit 1
        fi
        rm -rf "$VENV_NAME"
    done
    echo -e "${GREEN} Local tests completed successfully!${NC}\n"
}

# Function to run Remote TestPyPI Tests
run_remote_tests() {
    echo -e "${CYAN}--- Starting REMOTE TestPyPI Tests ---${NC}"
    
    if [ -z "$TEST_PYPI_TOKEN" ]; then
        echo -e "${RED}Error: TEST_PYPI_TOKEN environment variable is not set.${NC}"
        exit 1
    fi

    cd "${PYTHON_SDK_PATH}"
    BASE_VERSION=$(poetry version -s)

    # 1. Version Bumping Calculation
    echo -e "${CYAN}Calculating next revision for TestPyPI...${NC}"
    N=1
    while true; do
        TEMP_VERSION="${BASE_VERSION}.rc${N}"
        # Check simple index for version presence
        VERSION_EXISTS=$(curl -s $TEST_PYPI_URL$PACKAGE_NAME/ | grep "$TEMP_VERSION" || true)
        if [ -z "$VERSION_EXISTS" ]; then
            echo -e "${GREEN}Targeted Version: $TEMP_VERSION${NC}"
            break
        else
            echo -e "${GREEN}Version: $TEMP_VERSION${NC} already exists on TestPyPi."
            N=$((N+1))
        fi
    done

    # 2. Cleanup Trap
    cleanup() {
        echo -e "${CYAN}Resetting pyproject.toml to $BASE_VERSION...${NC}"
        cd "${PYTHON_SDK_PATH}" 
        poetry version "$BASE_VERSION"
    }
    trap cleanup EXIT

    # 3. Publish
    echo -e "${CYAN}Publishing $TEMP_VERSION to TestPyPI...${NC}"
    poetry version "$TEMP_VERSION"
    poetry config pypi-token.testpypi "$TEST_PYPI_TOKEN"
    poetry build
    poetry publish -r testpypi

    echo -e "${YELLOW}Waiting 60s for TestPyPI indexing...${NC}"
    sleep 60

    # 4. Remote Validation
    cd "${PROJECT_DIR}" # Move out for isolation
    for VER in "${PYTHON_VERSIONS[@]}"; do
        echo -e "\n${YELLOW}Testing Remote Install on Python $VER${NC}"
        
        if ! command -v "python$VER" &> /dev/null; then
            echo -e "${RED}python$VER not found. Skipping.${NC}"
            continue
        fi

        VENV_NAME=".venv_remote_test_$VER"
        "python$VER" -m venv "$VENV_NAME"
        
        if "$VENV_NAME/bin/pip" install --no-cache-dir \
            --index-url "$TEST_PYPI_URL" \
            --extra-index-url https://pypi.org/simple \
            "$PACKAGE_NAME==$TEMP_VERSION"; then
            
            "$VENV_NAME/bin/mosaicolabs.examples" --help > /dev/null
            "$VENV_NAME/bin/mosaicolabs.ros_injector" --help > /dev/null
            echo -e "${GREEN}Python $VER: REMOTE SUCCESS${NC}"
        else
            echo -e "${RED}Python $VER: REMOTE INSTALLATION FAILED${NC}"
            exit 1
        fi
        rm -rf "$VENV_NAME"
    done
    echo -e "${GREEN} Remote staging complete for $TEMP_VERSION!${NC}"
}

# --- Argument Parsing ---

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 [--local] [--remote] [--all]"
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --local)
            run_local_tests
            shift
            ;;
        --remote)
            run_remote_tests
            shift
            ;;
        --all)
            run_local_tests
            run_remote_tests
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--local] [--remote] [--all]"
            exit 1
            ;;
    esac
done