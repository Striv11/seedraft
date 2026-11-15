//! Keeps `--help` and the README CLI reference in sync: every flag the binary advertises must
//! be documented, and every documented flag must be advertised.

use std::collections::BTreeSet;
use std::process::Command;

fn flags_in(text: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for word in text.split(|c: char| !(c == '-' || c.is_ascii_alphanumeric())) {
        if word.starts_with("--")
            && word[2..].starts_with(|c: char| c.is_ascii_lowercase())
            && word != "--help"
        {
            out.insert(word.to_string());
        }
    }
    out
}

#[test]
fn readme_documents_exactly_the_flags_in_help() {
    let out = Command::new(env!("CARGO_BIN_EXE_seedraft"))
        .arg("--help")
        .output()
        .expect("run seedraft --help");
    assert!(out.status.success());
    let help = flags_in(&String::from_utf8(out.stdout).unwrap());
    assert!(help.len() >= 10, "help lists too few flags: {help:?}");

    let readme = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md")).unwrap();
    let start = readme.find("## CLI reference").expect("CLI reference section");
    let section = &readme[start..];
    let end = section[3..].find("\n## ").map_or(section.len(), |i| i + 3);
    let documented = flags_in(&section[..end]);

    assert_eq!(help, documented, "README CLI reference and `--help` disagree");
}
