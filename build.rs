use std::env;
use std::path::PathBuf;
use std::fs::{self, File};
use std::io::{Cursor, Read};

fn main() {
    // Only run this bootstrap logic inside the GitHub Actions environment
    if env::var("GITHUB_ACTIONS").is_err() {
        return;
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let protoc_dir = out_dir.join("protoc_sandbox");
    let protoc_bin = if cfg!(target_os = "windows") {
        protoc_dir.join("bin").join("protoc.exe")
    } else {
        protoc_dir.join("bin").join("protoc")
    };

    if !protoc_bin.exists() {
        fs::create_dir_all(&protoc_dir).unwrap();

        // Pin a reliable cross-platform version of protoc
        let version = "27.2"; 
        let os = if cfg!(target_os = "linux") {
            "linux-x86_64"
        } else if cfg!(target_os = "macos") {
            "osx-universal_binary"
        } else if cfg!(target_os = "windows") {
            "win64"
        } else {
            panic!("Unsupported target OS for protoc automatic download");
        };

        let url = format!(
            "https://github.com/protocolbuffers/protobuf/releases/download/v{}/protoc-{}-{}.zip",
            version, version, os
        );

        // Download using standard internal channel hooks
        let response = ureq::get(&url).call().into_reader();
        let mut zip_data = Vec::new();
        response.read_to_end(&mut zip_data).unwrap();

        // Extract zip payload manually into the sandbox directory
        let mut archive = zip::ZipArchive::new(Cursor::new(zip_data)).unwrap();
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).unwrap();
            let outpath = protoc_dir.join(file.mangled_name());

            if file.name().ends_with('/') {
                fs::create_dir_all(&outpath).unwrap();
            } else {
                if let Some(p) = outpath.parent() {
                    if !p.exists() {
                        fs::create_dir_all(p).unwrap();
                    }
                }
                let mut outfile = File::create(&outpath).unwrap();
                std::io::copy(&mut file, &mut outfile).unwrap();
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&protoc_bin, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    // Set the PROTOC environment variable globally for all downstream build scripts
    println!("cargo:rustc-env=PROTOC={}", protoc_bin.to_str().unwrap());
    println!("cargo:rerun-if-env-changed=PROTOC");
}