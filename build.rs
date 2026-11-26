//! The build identity `rift version` prints and the daemon's hello carries: the release (else the
//! crate version), then the short commit, `-dirty` over uncommitted work in this crate, `unknown`
//! outside a git checkout; it never fails the build. On macOS, the Info.plist the binary carries
//! in `__TEXT,__info_plist`, which names it in the microphone's prompt (docs/index.html#identity).

use std::{env, fs, path::Path, process::Command};

fn main() {
    let plist = Path::new(&env::var_os("OUT_DIR").unwrap_or_default()).join("Info.plist");
    if env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "macos")
        && fs::write(
            &plist,
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleIdentifier</key>
  <string>dev.gigantic.rift</string>
  <key>CFBundleName</key>
  <string>rift</string>
  <key>NSMicrophoneUsageDescription</key>
  <string>rift records the microphone onto its audio stream while its ears are on.</string>
</dict>
</plist>
"#,
        )
        .is_ok()
    {
        println!(
            "cargo::rustc-link-arg-bins=-Wl,-sectcreate,__TEXT,__info_plist,{}",
            plist.display()
        );
    }
    let git = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|out| out.status.success())
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
    };
    // a commit moves the worktree's HEAD log, staging its index; an edit touches src
    for watched in git(&["rev-parse", "--absolute-git-dir"])
        .map(|dir| [format!("{dir}/logs/HEAD"), format!("{dir}/index")])
        .into_iter()
        .flatten()
        .chain(["Cargo.toml".to_owned(), "src".to_owned()])
    {
        println!("cargo::rerun-if-changed={watched}");
    }
    let build = git(&["rev-parse", "--short", "HEAD"]).map_or_else(
        || "unknown".to_owned(),
        |hash| {
            if git(&["status", "--porcelain", "--", "."]).is_some_and(|status| !status.is_empty()) {
                format!("{hash}-dirty")
            } else {
                hash
            }
        },
    );
    println!("cargo::rustc-env=RIFT_BUILD={build}");
    // the release's `YYYY.M.N`, which the release workflow stamps; a checkout's build the crate's
    println!("cargo::rerun-if-env-changed=RIFT_VERSION");
    println!(
        "cargo::rustc-env=RIFT_RELEASE={}",
        env::var("RIFT_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_owned())
    );
}
