.PHONY: clean clean_all install_protoc

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckdb_hudi

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.3

all: configure debug

# Detect the operating system running the Makefile to establish the bootstrap command
ifeq ($(OS),Windows_NT)
    BOOTSTRAP_PROTOC := where protoc >nul 2>nul || choco install protoc -y
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        BOOTSTRAP_PROTOC := command -v protoc >/dev/null 2>&1 || (sudo apt-get update && sudo apt-get install -y protobuf-compiler)
    endif
    ifeq ($(UNAME_S),Darwin)
        BOOTSTRAP_PROTOC := command -v protoc >/dev/null 2>&1 || brew install protobuf
    endif
endif

# Define a dedicated target for installing protoc (Make sure to use a literal Tab before the command!)
install_protoc:
	$(BOOTSTRAP_PROTOC)

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

# Attach the protoc installation target as a dependency before the actual compilation targets fire
debug: install_protoc build_extension_library_debug build_extension_with_metadata_debug
release: install_protoc build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean