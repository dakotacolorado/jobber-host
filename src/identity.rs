//! The host's id and own token: registered on each start, and kept in the
//! state file, which only the host's user can read.

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    /// The service it registered with: a state file is no use against another.
    pub service: String,
    pub id: String,
    pub token: String,
}

/// The identity kept at `path`, or None when there is no file yet. A file
/// others can read is narrowed to the user first, and said so.
pub fn load(path: &Path, service: &str) -> Result<Option<Identity>, String> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let mode = fs::metadata(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .permissions()
        .mode();
    if mode & 0o077 != 0 {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("{}: {e}", path.display()))?;
        crate::log!("{} was mode {:o}: made 600", path.display(), mode & 0o777);
    }
    let identity: Identity =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    if identity.service != service {
        return Err(format!(
            "{} is host {} of {}, not {service}: remove it to register with {service}",
            path.display(),
            identity.id,
            identity.service
        ));
    }
    Ok(Some(identity))
}

/// Written whole to a file made 600 from the start, then renamed over the
/// old, so the token is never readable by others, even briefly, and a crash
/// leaves the old file or the new, never half of one.
pub fn save(path: &Path, identity: &Identity) -> Result<(), String> {
    let at = |e: std::io::Error| format!("{}: {e}", path.display());
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(at)?;
    }
    let tmp = path.with_extension("tmp");
    let _ = fs::remove_file(&tmp);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .map_err(at)?;
    file.write_all(serde_json::to_string_pretty(identity).unwrap().as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(at)?;
    fs::rename(&tmp, path).map_err(at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("jh-identity-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn me() -> Identity {
        Identity {
            service: "https://jobber".into(),
            id: "r1".into(),
            token: "r1.s".into(),
        }
    }

    #[test]
    fn it_is_kept_where_only_its_user_can_read_it() {
        let dir = dir("keep");
        let path = dir.join("state/host.json");
        assert_eq!(load(&path, "https://jobber").unwrap(), None);
        save(&path, &me()).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(load(&path, "https://jobber").unwrap(), Some(me()));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_file_others_can_read_is_narrowed() {
        let dir = dir("narrow");
        let path = dir.join("host.json");
        save(&path, &me()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        load(&path, "https://jobber").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_file_for_another_service_is_refused() {
        let dir = dir("other");
        let path = dir.join("host.json");
        save(&path, &me()).unwrap();
        let err = load(&path, "http://localhost:3005").unwrap_err();
        assert!(err.contains("remove it to register"), "{err}");
        fs::remove_dir_all(dir).unwrap();
    }
}
