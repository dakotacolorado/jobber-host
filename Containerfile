# The image jobber-host's own release jobs run in (its routine's jobs.toml, dakotacolorado/release-routines dakotacolorado/jobber-host/), built on base-rust.
ARG BASE=localhost/base-rust:1
FROM ${BASE}
