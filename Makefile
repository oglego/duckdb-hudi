.PHONY: clean clean_all install_protoc
PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXTENSION_NAME=duckdb_hudi
# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# Note: currently extension-template-rs requires this, as duckdb-rs relies on unstable C API functionality
USE_UNSTABLE_C_API=1
# Target DuckDB version
TARGET_DUCKDB_VERSION=v1.5.3

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# protoc version to install when not already present
PROTOC_VERSION := 25.3

ifeq ($(UNAME_S),Linux)
  ifeq ($(UNAME_M),x86_64)
    PROTOC_ARCH := linux-x86_64
  else ifeq ($(UNAME_M),aarch64)
    PROTOC_ARCH := linux-aarch_64
  else
    PROTOC_ARCH := linux-x86_64
  endif
else ifeq ($(UNAME_S),Darwin)
  PROTOC_ARCH := osx-universal_binary
else ifneq (,$(findstring MINGW,$(UNAME_S)))
  PROTOC_ARCH := win64
else ifneq (,$(findstring MSYS,$(UNAME_S)))
  PROTOC_ARCH := win64
else ifneq (,$(findstring CYGWIN,$(UNAME_S)))
  PROTOC_ARCH := win64
else
  PROTOC_ARCH := unknown
endif

PROTOC_ZIP := protoc-$(PROTOC_VERSION)-$(PROTOC_ARCH).zip
PROTOC_URL := https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/$(PROTOC_ZIP)

# Windows release zips ship protoc.exe, not protoc
ifneq (,$(findstring win,$(PROTOC_ARCH)))
  PROTOC_BIN_IN_ZIP := bin/protoc.exe
else
  PROTOC_BIN_IN_ZIP := bin/protoc
endif

# Use sudo if available, otherwise run without (e.g. in rootless containers)
SUDO := $(shell command -v sudo 2>/dev/null && echo "sudo" || echo "")

all: configure debug

install_protoc:
	@which protoc > /dev/null 2>&1 && echo "protoc already installed, skipping" || ( \
        echo "Installing protoc $(PROTOC_VERSION) for $(PROTOC_ARCH)..." && \
        if [ "$(PROTOC_ARCH)" = "unknown" ]; then \
            echo "Unsupported OS/arch: $(UNAME_S)/$(UNAME_M). Please install protobuf-compiler manually." && exit 1; \
        fi && \
        curl -fsSL -o /tmp/$(PROTOC_ZIP) $(PROTOC_URL) && \
        $(SUDO) unzip -o /tmp/$(PROTOC_ZIP) -d /usr/local $(PROTOC_BIN_IN_ZIP) && \
        $(SUDO) unzip -o /tmp/$(PROTOC_ZIP) -d /usr/local 'include/*' && \
        $(SUDO) chmod +x /usr/local/$(PROTOC_BIN_IN_ZIP) 2>/dev/null; \
        rm -f /tmp/$(PROTOC_ZIP) \
    )

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