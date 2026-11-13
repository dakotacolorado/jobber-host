# jobber-host

**Submit code to run on whatever computers you have available.**

jobber-host runs [jobber](https://github.com/dakotacolorado/jobber)'s runs on
a computer. It measures the machine, registers with jobber what it offers,
holds one connection jobber pushes runs over, runs each in its repo's rootless
podman image, streams its log back and reports how it ended.

## How a run runs

A run names a repo, a commit and a job. jobber-host clones the repo at that
commit, reads the job from the repo's `jobs.toml`, builds the repo's
`Containerfile` (once for each hash of what goes into it), and runs the job's
command in it with the checkout mounted at `/work`, capped at the CPUs and
memory jobber admitted it with. The log goes to jobber as it is written; the
checkout and container are removed after.

```toml
# jobs.toml
[defaults]
memory = 512      # MiB, unless the run requests otherwise
timeout = 600     # seconds

[jobs.hello]
run = ["python", "jobs/hello.py"]
stall = 120       # seconds without output, CPU or network before it is ended; 0 for never
```

## Config

`~/.config/jobber-host/config.toml` (or `$XDG_CONFIG_HOME/jobber-host/`),
else the file `--config PATH` or `JOBBER_HOST_CONFIG` names. Every setting is
in it:

```toml
service = "https://jobber.example:8445"       # jobber's address
registration_token_file = "/home/me/.config/jobber-host/registration-token"  # jobber's RUNNER_TOKEN
name = "laptop"                                # default: the hostname; keeps the host's id
labels = { arch = "x86_64" }
# What it keeps back for everything else on the machine: it offers the rest of
# the CPUs and memory it measures (cgroup limits included) each time it starts.
reserve = { cpu = 2, memory = 4096 }
build_memory = 1024                            # MiB an image build may use
job_memory_overhead = 0                        # MiB each container gets beyond its run's memory
cgroup_parent = "jobber.slice"                 # the systemd user slice containers run in; "" for podman's default
platform = "linux/amd64"                       # optional: every image pulled, built and run as this
git_credential_helper = "!gh auth git-credential"  # optional, for private repos

[slice]                                        # the slice's unit, as `jobber-host --slice-unit` writes it
memory_max = "8G"                              # default: what it offers
cpu_weight = 20
```

Its id and own token are kept in `~/.local/state/jobber-host/host.json`
(`state_file`), mode 600, and each run's checkout under
`~/.local/share/jobber-host/` (`work_dir`). It registers again on every start,
with what it offers then and its version; its name keeps its id.

## Run it by hand

```sh
cargo run --release -- --config /path/to/config.toml
```

It runs in the foreground, logging to stderr. Ctrl+C (or SIGTERM) declines
new runs, stops those it runs and reports them, then exits.

## Development

`CLAUDE.md` says how the code is laid out; the `test` skill
(`.claude/skills/test/SKILL.md`) gives the checks.

## License

MIT
