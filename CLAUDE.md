# jobber-host

Runs jobber's runs on a computer. One crate, one binary, `jobber-host`,
ported from job-runner's hosts mode; nothing is shared with job-runner at
runtime or in code, and it installs beside one without touching it.

## What it does, and nothing more

Measure the machine, register with jobber what it offers (its CPUs and
memory, cgroup limits included, less the config's `reserve`), hold the one
connection jobber pushes runs over, accept what fits its budget, run each in
its repo's podman image, stream its log, report its end. What a run runs is its
repo's `jobs.toml`; what it is sent is jobber's to decide. jobber's
`openapi.json` and `docs/PROTOCOL.md` are the protocol; this follows their
shapes. It speaks that protocol only: there is no other mode, no claiming,
and no run on the host itself.

`src/service.rs` is jobber's HTTP half (register, log appends, completions,
the HTTP heartbeat), `src/connection.rs` the WebSocket it is pushed runs over,
`src/heartbeat.rs` what a heartbeat says, `src/executor.rs` how a run runs.

## Writing code here

The code carries the facts; prose carries only what the code cannot. Keep a
comment for a constraint, a trade-off or a reason something is not the obvious
way; delete one that restates the line beneath it.

Admission is `src/budget.rs`, pure and unit-tested: change it there, with a
test. The computer will be suspended and unplugged without warning; anything
that assumes it keeps running is wrong, and jobber's lease is what recovers a
run whose host vanished.

The banner (`src/banner.rs`) is jobber's mark and tagline: change them in
jobber first, then here, and keep the README's opening block the same (a test
holds it).

## Tests

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
Tests use a fake jobber in-process: none needs the network. Everything else,
the podman tests included, is the `test` skill (`.claude/skills/test/SKILL.md`).
