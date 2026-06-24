.PHONY: clean clean_all install_protoc

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckdb_hudi

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1

# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.3

UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Linux)
    BOOTSTRAP_PROTOC := apt-get update && apt-get install -y protobuf-compiler
else ifeq ($(UNAME_S),Darwin)
    BOOTSTRAP_PROTOC := brew install protobuf
else
    BOOTSTRAP_PROTOC := echo "Unsupported OS: $(UNAME_S). Please install protobuf-compiler manually." && exit 1
endif

# Use sudo if available, otherwise run without (e.g. in rootless containers)
SUDO := $(shell command -v sudo 2>/dev/null && echo "sudo" || echo "")

all: configure debug

install_protoc:
	@which protoc > /dev/null 2>&1 && echo "protoc already installed, skipping" || $(SUDO) $(BOOTSTRAP_PROTOC)

include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

build_extension_library_release: install_protoc
build_extension_library_debug: install_protoc

configure: venv platform extension_version

debug: install_protoc build_extension_library_debug build_extension_with_metadata_debug
release: install_protoc build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean