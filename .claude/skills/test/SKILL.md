---
name: test
description: How to run jobber-host's checks and its podman tests. Use before submitting a change to jobber-host, and when reviewing one.
---

# Testing jobber-host

Run every command from the repo root, each on its own, and read its exit
status (`echo $?`). Never pipe one into `tail` or `head`: the pipe reports
their status, so a failure reads as a pass.

## The checks

An author runs these at the PR's head before submitting it, and a reviewer at
the commit reviewed. Each must exit 0. They need Rust stable (rustup's minimal
profile with `clippy` and `rustfmt`) and a C linker for `ring`; a machine
without `cc` can use Zig: `cc` and `ar` scripts on PATH that run
`zig cc -target x86_64-linux-gnu` (dropping any `--target=*` argument) and
`zig ar`. None reads a deployed service, but unset the session's prod
`*_URL`/`*_TOKEN` pairs anyway:

```sh
env -u DS_URL -u DS_TOKEN -u WT_URL -u WT_TOKEN -u WS_URL -u WS_TOKEN -u AS_URL -u AS_TOKEN -u JS_URL -u JS_TOKEN -u AG_URL -u AG_TOKEN cargo build --locked --release
env -u DS_URL -u DS_TOKEN -u WT_URL -u WT_TOKEN -u WS_URL -u WS_TOKEN -u AS_URL -u AS_TOKEN -u JS_URL -u JS_TOKEN -u AG_URL -u AG_TOKEN cargo test --locked
env -u DS_URL -u DS_TOKEN -u WT_URL -u WT_TOKEN -u WS_URL -u WS_TOKEN -u AS_URL -u AS_TOKEN -u JS_URL -u JS_TOKEN -u AG_URL -u AG_TOKEN cargo clippy --locked --all-targets -- -D warnings
env -u DS_URL -u DS_TOKEN -u WT_URL -u WT_TOKEN -u WS_URL -u WS_TOKEN -u AS_URL -u AS_TOKEN -u JS_URL -u JS_TOKEN -u AG_URL -u AG_TOKEN cargo fmt --check
sh -n scripts/install.sh
```

`cargo test` needs no network: jobber is faked in-process, over HTTP and its
WebSocket. Where `shellcheck` is installed, `shellcheck scripts/install.sh`
passes too.

## The podman tests (by hand)

Ten more, ignored by `cargo test`, need rootless podman, the network
(busybox from docker.io) and a systemd user session: an image that will not
build fails its run with the build's output in the log, a run writes in its
workspace and it is deleted after, a run is sent only the credentials given
it and reads them masked, a run with no output, CPU or network is ended as
stalled at its `stall` limit and a silent busy one or a quiet chatty one past
it is not (about 90s), a failed run's reason carries its last line, a run
whose container monitor (conmon) is killed, as the OOM killer may do in its
place, is failed with exit 137 at once, a run past its slice's memory is
killed (exit 137) and failed, inside that slice, and a run canceled in a
heartbeat's answer is stopped and completed canceled, and one its answer says
has ended is stopped and not completed. The slice test runs in a throwaway
slice capped at 300M; make it first, and remove it after:

```sh
printf '[Slice]\nMemoryMax=300M\nMemorySwapMax=0\nCPUWeight=20\n' > ~/.config/systemd/user/jobbertest.slice
systemctl --user daemon-reload
JOBBER_HOST_TEST_SLICE=jobbertest.slice env -u DS_URL -u DS_TOKEN -u WT_URL -u WT_TOKEN -u WS_URL -u WS_TOKEN -u AS_URL -u AS_TOKEN -u JS_URL -u JS_TOKEN -u AG_URL -u AG_TOKEN cargo test --locked -- --ignored
rm ~/.config/systemd/user/jobbertest.slice && systemctl --user daemon-reload
```

A pass: `10 passed`, exit 0. On a Mac, podman runs every container in its
podman machine, so make and remove the slice there
(`podman machine ssh "..."` around the same two lines) and run the tests from
the Mac.

## By hand, against a local jobber

To see a change run end to end: start jobber from its repo on a port no one
holds, with a scratch database and scratch `AUTH_TOKEN` and `RUNNER_TOKEN`,
and wait for its banner. Write a scratch config whose `service` is it,
`registration_token_file` holds the `RUNNER_TOKEN`, and `state_file` and
`work_dir` are under a scratch directory, with a `cgroup_parent` of `""` or a
slice you made. Run `cargo run -- --config <it>` in the foreground, submit runs
to jobber (`POST /runs` with the `AUTH_TOKEN`), and stop it with Ctrl+C,
which stops and reports what it runs.
