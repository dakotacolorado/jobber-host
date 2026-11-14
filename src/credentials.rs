//! The credentials a job is sent: only those its `jobs.toml` names, only for
//! its run, as environment variables podman is told by name alone, so no value
//! is ever on a command line.
//!
//! [`Source`] is the one place they come from. Today that is [`Host`]: the
//! host's own AWS and GitHub logins, the ones agents on it use. Credentials
//! issued per run or per host (by jobber, say) are another `Source`, given
//! the run to choose by; no job definition changes for it.

use serde::Deserialize;
use std::process::Stdio;

use crate::service::Assignment;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` when
    /// there is one, and `AWS_REGION`/`AWS_DEFAULT_REGION` when one is set.
    Aws,
    /// `GH_TOKEN` and `GITHUB_TOKEN`, for `gh` and the GitHub API.
    Github,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Aws => "aws",
            Kind::Github => "github",
        }
    }
}

/// An environment variable a job is sent, and whether its value is secret
/// (masked in the log). A region is not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Var {
    pub name: std::borrow::Cow<'static, str>,
    pub value: String,
    pub secret: bool,
}

pub trait Source: Send + Sync {
    /// What `run`'s job is sent for `kind`, or why it cannot be.
    fn get(&self, run: &Assignment, kind: Kind) -> Result<Vec<Var>, String>;
}

/// Everything `run`'s job is sent for the kinds it names, or why one cannot be.
pub fn gather(source: &dyn Source, run: &Assignment, kinds: &[Kind]) -> Result<Vec<Var>, String> {
    let mut vars = Vec::new();
    for &kind in kinds {
        let got = source.get(run, kind).map_err(|e| {
            format!(
                "it needs {} credentials, which this host cannot give: {e}",
                kind.name()
            )
        })?;
        if got.is_empty() {
            return Err(format!(
                "it needs {} credentials, which this host has none of",
                kind.name()
            ));
        }
        vars.extend(got);
    }
    Ok(vars)
}

/// The host's own credentials, read when a job needs them (so a login renewed
/// since jobber-host started is the one sent): AWS from the AWS CLI's own
/// resolution (`aws configure export-credentials`), GitHub from `gh auth token`.
pub struct Host;

impl Host {
    fn output(program: &str, args: &[&str]) -> Result<String, String> {
        let mut cmd = std::process::Command::new(program);
        cmd.env_clear();
        for key in ["PATH", "HOME", "USER", "XDG_CONFIG_HOME"] {
            if let Ok(v) = std::env::var(key) {
                cmd.env(key, v);
            }
        }
        let out = cmd
            .args(args)
            .stdin(Stdio::null())
            .output()
            .map_err(|e| format!("{program}: {e}"))?;
        if !out.status.success() {
            // Its stderr says why (not logged in, no profile); it holds no secret.
            let why = String::from_utf8_lossy(&out.stderr);
            return Err(format!(
                "`{program} {}` failed ({}): {}",
                args.join(" "),
                out.status,
                why.trim()
            ));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Exported {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

/// AWS's variables from `aws configure export-credentials --format process`'s
/// JSON and the configured region, if any.
pub fn aws_vars(exported: &str, region: Option<&str>) -> Result<Vec<Var>, String> {
    let e: Exported = serde_json::from_str(exported)
        .map_err(|e| format!("aws configure export-credentials: unreadable output: {e}"))?;
    let mut vars = vec![
        Var {
            name: "AWS_ACCESS_KEY_ID".into(),
            value: e.access_key_id,
            secret: true,
        },
        Var {
            name: "AWS_SECRET_ACCESS_KEY".into(),
            value: e.secret_access_key,
            secret: true,
        },
    ];
    if let Some(token) = e.session_token.filter(|t| !t.is_empty()) {
        vars.push(Var {
            name: "AWS_SESSION_TOKEN".into(),
            value: token,
            secret: true,
        });
    }
    if let Some(region) = region.filter(|r| !r.is_empty()) {
        for name in ["AWS_REGION", "AWS_DEFAULT_REGION"] {
            vars.push(Var {
                name: name.into(),
                value: region.to_string(),
                secret: false,
            });
        }
    }
    Ok(vars)
}

impl Source for Host {
    fn get(&self, _run: &Assignment, kind: Kind) -> Result<Vec<Var>, String> {
        match kind {
            Kind::Aws => {
                let exported = Host::output(
                    "aws",
                    &["configure", "export-credentials", "--format", "process"],
                )?;
                // No region configured is not an error: the job may name its own.
                let region = Host::output("aws", &["configure", "get", "region"]).ok();
                aws_vars(&exported, region.as_deref())
            }
            Kind::Github => {
                let token = Host::output("gh", &["auth", "token"])?;
                if token.is_empty() {
                    return Ok(Vec::new());
                }
                Ok(["GH_TOKEN", "GITHUB_TOKEN"]
                    .into_iter()
                    .map(|name| Var {
                        name: name.into(),
                        value: token.clone(),
                        secret: true,
                    })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_is_sent_its_keys_its_session_token_and_region() {
        let vars = aws_vars(
            r#"{"Version":1,"AccessKeyId":"AKIAEXAMPLE","SecretAccessKey":"s3cr3t","SessionToken":"tok","Expiration":"2026-11-06T00:00:00Z"}"#,
            Some("us-east-1"),
        )
        .unwrap();
        let names: Vec<_> = vars.iter().map(|v| (&*v.name, v.secret)).collect();
        assert_eq!(
            names,
            [
                ("AWS_ACCESS_KEY_ID", true),
                ("AWS_SECRET_ACCESS_KEY", true),
                ("AWS_SESSION_TOKEN", true),
                ("AWS_REGION", false),
                ("AWS_DEFAULT_REGION", false)
            ]
        );
        let plain = aws_vars(
            r#"{"Version":1,"AccessKeyId":"A","SecretAccessKey":"S"}"#,
            None,
        )
        .unwrap();
        assert_eq!(plain.len(), 2);
        assert!(aws_vars("not json", None).is_err());
    }

    struct Fixed;
    impl Source for Fixed {
        fn get(&self, _: &Assignment, kind: Kind) -> Result<Vec<Var>, String> {
            match kind {
                Kind::Aws => Err("not logged in".into()),
                Kind::Github => Ok(Vec::new()),
            }
        }
    }

    #[test]
    fn a_job_needing_what_the_host_lacks_is_told_which() {
        let run = crate::executor::tests::assignment("hello", None, None, 256);
        assert!(gather(&Fixed, &run, &[]).unwrap().is_empty());
        let e = gather(&Fixed, &run, &[Kind::Aws]).unwrap_err();
        assert!(e.contains("aws") && e.contains("not logged in"), "{e}");
        let e = gather(&Fixed, &run, &[Kind::Github]).unwrap_err();
        assert!(e.contains("github credentials, which this host has none of"));
    }
}
