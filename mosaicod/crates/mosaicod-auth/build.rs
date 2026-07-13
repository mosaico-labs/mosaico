fn main() {
    let version = rustc_version::version().expect("cannot read rustc version");
    println!("cargo:rustc-env=RUSTC_VERSION={version}")
}
