# jobber-host

```
     ██╗ ██████╗ ██████╗ ██████╗ ███████╗██████╗       ██╗  ██╗ ██████╗ ███████╗████████╗      ▄▄▄▄▄▄▄▄▄▄
     ██║██╔═══██╗██╔══██╗██╔══██╗██╔════╝██╔══██╗      ██║  ██║██╔═══██╗██╔════╝╚══██╔══╝      █ █ ██ █ █
     ██║██║   ██║██████╔╝██████╔╝█████╗  ██████╔╝█████╗███████║██║   ██║███████╗   ██║         ▀▀▀▀▀▀▀▀▀▀
██   ██║██║   ██║██╔══██╗██╔══██╗██╔══╝  ██╔══██╗╚════╝██╔══██║██║   ██║╚════██║   ██║     █▀█▀▀█▀█  █▀█▀▀█▀█
╚█████╔╝╚██████╔╝██████╔╝██████╔╝███████╗██║  ██║      ██║  ██║╚██████╔╝███████║   ██║     █ █  █ █  █ █  █ █
 ╚════╝  ╚═════╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝      ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝     ▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀
```

**Submit code to run on whatever computers you have.**

jobber-host runs [jobber](https://github.com/dakotacolorado/jobber)'s runs on
a computer. It offers jobber the machine's CPUs and memory, takes the runs
jobber sends it, runs each in its repo's rootless podman image, and streams
the log back.

## Install it

You need Linux with rootless podman, or a Mac with a podman machine, and
cargo. From a checkout of the latest release tag:

```sh
git clone https://github.com/dakotacolorado/jobber-host && cd jobber-host
git checkout "$(git tag -l 'v*-*' --sort=-v:refname | head -n 1)"
scripts/install.sh --service https://<your-jobber> --registration-token-file <file holding jobber's RUNNER_TOKEN>
```

This installs `~/.local/bin/jobber-host`, writes its config, and runs it as a
service: the systemd user service `jobber-host.service` on Linux, or the
LaunchAgent `com.github.dakotacolorado.jobber-host` on macOS. Run the script
again from a newer tag to upgrade. `--uninstall` removes it, and `--purge`
also removes its config and identity. See
[docs/add-a-host.md](docs/add-a-host.md) for more.

To run it in the foreground instead:

```sh
cargo run --release -- --config /path/to/config.toml
```

Ctrl+C or SIGTERM stops the runs it has, reports them, and exits.

## Configure it

Every setting is in `config.toml`, at `$XDG_CONFIG_HOME/jobber-host/` or
`~/.config/jobber-host/`, or the path given by `--config` or
`JOBBER_HOST_CONFIG`.

| Setting | Default | What it does |
|---|---|---|
| `service` | required | jobber's address. |
| `reserve` | required | CPUs and MiB kept for the rest of the machine, e.g. `{ cpu = 2, memory = 4096 }`. It offers the rest. |
| `registration_token_file` | `~/.config/jobber-host/registration-token` | File holding jobber's `RUNNER_TOKEN`. |
| `name` | the hostname | Its name in jobber, which keeps its id. |
| `labels` | none | Labels it registers with. |
| `build_memory` | `1024` | MiB an image build may use. |
| `job_memory_overhead` | `0` | MiB each container gets beyond its run's memory. |
| `stop_grace_seconds` | `10` | Seconds between SIGTERM and SIGKILL when a run is stopped. |
| `cgroup_parent` | `jobber.slice` | The systemd user slice containers run in; `""` for podman's default. |
| `platform` | the host's own | Platform every image is pulled, built and run as, e.g. `linux/amd64`. |
| `git_credential_helper` | your git config's | Credential helper for private repos, e.g. `!gh auth git-credential`. |
| `host_runs` | none | Repos (`owner/name`) whose `onHost` runs may run on this machine outside a container. |
| `gpus` | none | GPUs it offers, as podman CDI device names, e.g. `["nvidia.com/gpu=0"]`. Each is given whole to one run at a time. |
| `gpu_memory` | `nvidia-smi`'s | MiB of memory of its smallest GPU, registered so runs needing larger ones go elsewhere. |
| `base_owner` | `dakotacolorado` | GitHub owner whose `base-<name>` repos base images are built from. |
| `state_file` | `~/.local/state/jobber-host/host.json` | Its id and token, mode 600. |
| `work_dir` | `~/.local/share/jobber-host` | Where run checkouts are made. |
| `slice.memory_max` | what it offers | The slice's systemd `MemoryMax`, e.g. `"8G"`. |
| `slice.cpu_weight` | `20` | The slice's systemd `CPUWeight`. |

`jobber-host --slice-unit` prints the slice's systemd unit.

## How a run runs

jobber-host clones the run's repo at its commit, reads the job from the repo's
`jobs.toml`, builds the repo's `Containerfile` (cached by the hash of its
inputs), and runs the job's command with the checkout at `/work`, capped at
the CPUs and memory the run asked for. The checkout and container are removed
after.

```toml
# jobs.toml
[defaults]
memory = 512      # MiB, unless the run requests otherwise
timeout = 600     # seconds

[jobs.hello]
run = ["python", "jobs/hello.py"]
stall = 120       # seconds without output, CPU or network before it is ended; 0 for never

[jobs.train]
run = ["python", "train.py"]
gpu = 1           # whole GPUs, each its container's own (`--device`)
gpu_memory = 16384  # MiB each must have; a smaller host's GPUs fail it
```

A run's **GPUs** are what it was submitted with, `requirements.gpu`: jobber
never reads `jobs.toml`, so it schedules a run only by that, on a host with
that many free. A job whose `jobs.toml` asks for more GPUs than its run holds
fails, saying to submit it with `requirements.gpu`; one whose `gpu_memory`
is more than this host's GPUs have fails, saying to submit it with
`requirements.gpuMemory`, which jobber places only on a host whose GPUs are
that large, and on the smallest such. See
[docs/add-a-host.md](docs/add-a-host.md#gpus) to offer a machine's GPUs.

A run's **secrets** reach it as environment variables of their names. They are
masked in its log and never written to disk.

An **onHost** run, for a repo in `host_runs`, runs on the machine itself as
jobber-host's user, in its checkout, with no CPU or memory cap and no stall
limit. On cancel or timeout its process group gets SIGTERM, then SIGKILL after
`stop_grace_seconds`. A run from any other repo is failed unrun.

## Develop it

```sh
cargo build --locked --release
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --check
```

## License

MIT. See [LICENSE](./LICENSE).
