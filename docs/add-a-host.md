# Adding a computer to jobber

A computer runs jobber's runs once jobber-host is installed there, as the
user the runs should run as. On Linux it is a systemd user service,
`jobber-host.service`, its containers in `jobber.slice`, running without a
login (lingering). On macOS it is a LaunchAgent,
`com.github.dakotacolorado.jobber-host`, its containers in the podman
machine.

Everything it installs is its own: `~/.local/bin/jobber-host`,
`~/.config/jobber-host/`, `~/.local/state/jobber-host/`,
`~/.local/share/jobber-host/`, `jobber-host.service` and `jobber.slice`.

## Before

- **podman**, rootless: `sudo apt-get install -y podman uidmap` (the install
  script does it when it may use sudo). On Ubuntu 24.04 it has to be
  `/usr/bin/podman`: AppArmor lets only that path make user namespaces. The
  user needs `/etc/subuid` and `/etc/subgid` entries (Ubuntu adds them).
- **cgroup v2 with memory delegated** to the user's systemd:
  `cat /sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers`
  lists `memory` and `cpu`.
- **Rust's cargo and a C linker**, to build it (the test skill says how).
- **git access to run repos**: `gh auth login` as this user (the install
  script then sets `git_credential_helper = "!gh auth git-credential"`), or
  a helper of your own in the config.
- **jobber's registration token**, its `RUNNER_TOKEN`, in a file only you can
  read, e.g.:

  ```sh
  (umask 077; printf '%s\n' "$RUNNER_TOKEN" > /tmp/runner-token)
  ```

## Install

From a checkout of jobber-host's latest release tag
(`git fetch --tags && git tag -l 'v*-*' --sort=-v:refname`, then
`git checkout` the first):

```sh
scripts/install.sh --service https://<your-jobber> --registration-token-file /tmp/runner-token
rm /tmp/runner-token
```

It keeps back 2 CPUs and 4096 MiB for everything else on the computer and
offers jobber the rest of what it measures (`--reserve-cpu N` and
`--reserve-memory MIB` choose otherwise). To change that later, edit `reserve`
in `~/.config/jobber-host/config.toml` and run `scripts/install.sh` again:
`jobber.slice` is sized to what it offers, and it registers that again.

To upgrade, check out a newer release tag and run `scripts/install.sh`. It
rebuilds, restarts, and keeps the config and identity, so jobber sees the same
host with its new version.

## Check

```sh
systemctl --user status jobber-host     # active, in jobber.slice
journalctl --user -u jobber-host -n 5   # "registered as host <id>", "connected to jobber"
```

Then submit a run on jobber's page, or by its API; its host is this one, and
its log ends `[jobber-host] succeeded with exit 0`. The first run builds
`localhost/base-<name>:<n>` from its tag and the repo's image, which takes a
minute or two; later runs reuse both.

## GPUs

A host offers only the GPUs its config's `gpus` names, as podman CDI device
names; it gives each run asking for GPUs its own, as `--device`, and no other
run has them meanwhile. For NVIDIA:

1. **The NVIDIA Container Toolkit**, and a CDI spec of the machine's GPUs:
   `sudo nvidia-ctk cdi generate --output=/etc/cdi/nvidia.yaml`, then
   `nvidia-ctk cdi list` names them.
2. **Podman reads the spec**:
   `podman run --rm --device nvidia.com/gpu=all ubuntu nvidia-smi -L` lists
   them. Ubuntu 24.04's podman 4.9 cannot read a spec of `cdiVersion` 0.7.0
   (`unresolvable CDI devices`, and in `--log-level=debug` `unknown field
   "additionalGids"`): delete each `additionalGids` list, which grants only
   `/dev/dri`'s groups and not CUDA's world-writable `/dev/nvidia*`, and set
   `cdiVersion: 0.6.0`. Do it again whenever the spec is generated again.
3. **Name them** in `~/.config/jobber-host/config.toml`, by index rather than
   `all`, so runs can hold them one at a time:
   `gpus = ["nvidia.com/gpu=0"]`, then run `scripts/install.sh` again. Its log
   says `GPUs nvidia.com/gpu=0`, and jobber lists the host's capacity with
   `1 GPU`.

A mac's podman machine has no GPU to give.

## A mac

A mac runs containers in a `podman machine`, a Linux VM, under launchd; it has
no systemd, so nothing is capped by a slice. The machine is the cap: it holds
every run and image build together, and its own 512 MiB or so.

1. **podman**: `brew install podman`.
2. **The machine**, sized for the runs, e.g. `podman machine init --cpus 8
   --memory 6144 --disk-size 100 && podman machine start`. On an arm64 mac,
   turn on Rosetta first (`[machine]` and `rosetta = true` in
   `~/.config/containers/containers.conf`); runs are `platform = "linux/amd64"`
   and get `job_memory_overhead = 512` there.
3. **Install** with the same command. jobber-host measures the Mac, not the
   machine, so the first install keeps back what the machine does not have;
   the script refuses, saying what to do, while the machine is missing,
   stopped, or smaller than what it would offer and 512 MiB.
4. **Check**: `tail ~/Library/Logs/jobber-host.log` says
   `containers in podman's default cgroup` and `connected to jobber`.

## Remove

`scripts/install.sh --uninstall` stops and removes the unit (or agent), the
slice and the binary, and keeps the config and identity, so a reinstall is the
same host. `--uninstall --purge` removes those too; the host stays listed in
jobber, unused.
