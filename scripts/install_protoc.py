#!/usr/bin/env python3
"""Download protoc into build/protoc and print the absolute path of the binary.

The Makefile uses this on Windows runners, where `unzip` is usually missing and /usr/local is
not on the PATH seen by cargo's build scripts. Progress goes to stderr; only the path goes to
stdout, so the result can be captured with $(shell ...) and exported as PROTOC (which
prost-build honours). Nothing outside the repository is touched and no sudo is needed.
"""
import io
import os
import platform
import stat
import sys
import urllib.request
import zipfile

VERSION = "25.3"
URL = "https://github.com/protocolbuffers/protobuf/releases/download/v{v}/protoc-{v}-{arch}.zip"
DEST = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "build", "protoc")
)


def release_arch():
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Windows":
        return "win64"
    if system == "Darwin":
        return "osx-universal_binary"
    if system == "Linux":
        return "linux-aarch_64" if machine in ("aarch64", "arm64") else "linux-x86_64"
    sys.exit("install_protoc.py: unsupported platform {} {}".format(system, machine))


def main():
    exe = "protoc.exe" if platform.system() == "Windows" else "protoc"
    protoc = os.path.join(DEST, "bin", exe)
    if not os.path.isfile(protoc):
        url = URL.format(v=VERSION, arch=release_arch())
        print("Downloading {}".format(url), file=sys.stderr)
        with urllib.request.urlopen(url, timeout=120) as resp:
            data = resp.read()
        # Extract bin/ and include/ (protoc finds its well-known .proto files via ../include).
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(DEST)
        if not os.path.isfile(protoc):
            sys.exit("install_protoc.py: {} not found after extraction".format(protoc))
        os.chmod(protoc, os.stat(protoc).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    print(protoc)


if __name__ == "__main__":
    main()
