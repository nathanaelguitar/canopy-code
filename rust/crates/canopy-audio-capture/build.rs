use std::env;
use std::path::PathBuf;

fn main() {
    napi_build::setup();

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let source = manifest_dir.join("native/miniaudio_ffi.cc");
    let header = manifest_dir.join("native/miniaudio.h");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .file(&source)
        .include(manifest_dir.join("native"))
        .warnings(false)
        .compile("canopy_audio_capture");

    println!("cargo:rerun-if-changed={}", source.display());
    println!("cargo:rerun-if-changed={}", header.display());

    match env::var("CARGO_CFG_TARGET_OS").as_deref() {
        Ok("macos") => {
            for framework in [
                "CoreAudio",
                "AudioToolbox",
                "AudioUnit",
                "CoreFoundation",
                "AVFoundation",
                "Foundation",
            ] {
                println!("cargo:rustc-link-lib=framework={framework}");
            }
        }
        Ok("linux") => {
            for library in ["dl", "pthread", "m"] {
                println!("cargo:rustc-link-lib={library}");
            }
        }
        Ok("windows") => {
            for library in ["winmm", "ole32", "uuid", "ksuser"] {
                println!("cargo:rustc-link-lib={library}");
            }
        }
        _ => {}
    }
}
