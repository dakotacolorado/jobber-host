#!/bin/sh
# install.sh -- install, upgrade or remove jobber-host on this computer: on Linux as
# a systemd user service whose containers run in jobber.slice, on macOS as a
# LaunchAgent whose containers run in the podman machine.
#
#   scripts/install.sh --service URL --registration-token-file PATH [--reserve-cpu N] [--reserve-memory MIB]   first install
#   scripts/install.sh                                     upgrade: rebuild from this checkout, restart
#   scripts/install.sh --uninstall [--purge]               stop and remove; --purge also the config and identity
#
# Run from a checkout of jobber-host at a release (a v<version>-<n> tag). Builds with
# cargo, installs ~/.local/bin/jobber-host, writes ~/.config/jobber-host/config.toml
# if there is none, then jobber.slice and jobber-host.service (Linux), or the
# LaunchAgent (macOS). A job-runner on the same computer, its jobs.slice and its
# unit or agent, is left as it is. Says each thing it does.
set -eu

name=jobber-host
bin="$HOME/.local/bin/$name"
conf_dir="${XDG_CONFIG_HOME:-$HOME/.config}/$name"
conf="$conf_dir/config.toml"
units="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
unit=$name.service
slice=jobber.slice
state="${XDG_STATE_HOME:-$HOME/.local/state}/$name"
say() { echo "install: $*"; }
os=$(uname -s)
label=com.github.dakotacolorado.$name
agent="$HOME/Library/LaunchAgents/$label.plist"
domain="gui/$(id -u)"

service="" token_file="" uninstall="" purge="" reserve_cpu="" reserve_memory=""
while [ $# -gt 0 ]; do
  case $1 in
  --service) service=${2:?--service needs a URL}; shift 2 ;;
  --registration-token-file) token_file=${2:?--registration-token-file needs a path}; shift 2 ;;
  --reserve-cpu) reserve_cpu=${2:?--reserve-cpu needs a number of CPUs}; shift 2 ;;
  --reserve-memory) reserve_memory=${2:?--reserve-memory needs MiB}; shift 2 ;;
  --uninstall) uninstall=1; shift ;;
  --purge) purge=1; shift ;;
  *) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
  esac
done

forget() {
  if [ -n "$purge" ]; then
    rm -rf "$conf_dir" "$state"
    say "removed $conf_dir and $state: the host stays registered in jobber, unused"
  else
    say "kept $conf_dir and $state, so a reinstall is the same host"
  fi
}

if [ -n "$uninstall" ] && [ "$os" = Darwin ]; then
  # bootout returns before jobber-host has stopped and reported its runs.
  launchctl bootout "$domain/$label" 2>/dev/null || true
  while launchctl print "$domain/$label" >/dev/null 2>&1; do sleep 1; done
  say "stopped $label"
  rm -f "$agent" "$bin"
  say "removed $agent and $bin; the podman machine is left as it is"
  forget
  exit 0
elif [ -n "$uninstall" ]; then
  if systemctl --user disable --now "$unit" 2>/dev/null; then say "stopped and disabled $unit"; fi
  rm -f "$units/$unit" "$units/$slice" "$bin"
  systemctl --user daemon-reload
  say "removed $units/$unit, $units/$slice and $bin"
  forget
  exit 0
fi

cd "$(dirname "$0")/.."

if [ "$os" = Darwin ]; then
  command -v podman >/dev/null || { say "podman is missing: run 'brew install podman', then this again" >&2; exit 1; }
elif ! command -v podman >/dev/null; then
  if sudo -n true 2>/dev/null; then
    say "installing podman"
    sudo apt-get install -y podman uidmap
  else
    say "podman is missing and installing it needs root: run 'sudo apt-get install -y podman uidmap', then this again" >&2
    exit 1
  fi
fi
command -v cargo >/dev/null || { say "cargo is missing: install Rust (rustup) to build $name" >&2; exit 1; }

# A release is a v<version>-<n> tag (scripts/release); any other checkout is
# built too, named by its commit, for trying a change on this computer.
if release=$(git describe --tags --exact-match --match 'v*-*' HEAD 2>/dev/null); then
  say "building $name $release"
else
  release=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
  say "building $name $release, which is no release tag: 'git fetch --tags && git checkout <latest v*-* tag>' installs a release"
fi
JOBBER_HOST_RELEASE=$release cargo build --locked --release --quiet
mkdir -p "$(dirname "$bin")"
# Beside the running one, then renamed over it, so a running binary is never written to.
install -m 755 target/release/$name "$bin.new"
mv "$bin.new" "$bin"
say "installed $bin ($("$bin" --version | tail -n 1))"

if [ "$os" = Darwin ]; then
  # Every container runs in the podman machine, while jobber-host measures the
  # Mac: it keeps back what the machine does not have, so it offers no more
  # than the machine holds, less 512 MiB for the machine's own.
  machine=$(podman machine inspect --format '{{.Name}} {{.State}} {{.Resources.Memory}} {{.Resources.CPUs}} {{.Rosetta}}' 2>/dev/null | head -n 1) || machine=""
  [ -n "$machine" ] || {
    say "there is no podman machine: make one sized for the runs, e.g. 'podman machine init --cpus 8 --memory 6144 && podman machine start', then this again" >&2
    exit 1
  }
  # shellcheck disable=SC2086 # its words are the fields asked for
  set -- $machine
  [ "$2" = running ] || { say "podman machine $1 is $2: 'podman machine start', then this again" >&2; exit 1; }
  mac_memory=$(( $(sysctl -n hw.memsize) / 1048576 ))
  mac_cpus=$(sysctl -n hw.ncpu)
  fit_memory=$((mac_memory - $3 + 512))
  fit_cpu=$((mac_cpus - $4))
  [ "$fit_cpu" -ge 0 ] || fit_cpu=0
  [ "$(uname -m)" != arm64 ] || [ "$5" = true ] ||
    say "podman machine $1 runs amd64 images through qemu, many times slower than Rosetta: put '[machine]' and 'rosetta = true' in ~/.config/containers/containers.conf, then 'podman machine stop && podman machine start'" >&2
  say "runs run in podman machine $1: $3 MiB and $4 CPUs"
fi

if [ ! -f "$conf" ]; then
  [ -n "$service" ] && [ -n "$token_file" ] || {
    say "no $conf yet: give --service (jobber's address) and --registration-token-file (a file holding jobber's RUNNER_TOKEN)" >&2
    exit 2
  }
  mkdir -p "$conf_dir"
  install -m 600 "$token_file" "$conf_dir/registration-token"
  helper=""
  if command -v gh >/dev/null && gh auth status >/dev/null 2>&1; then helper='git_credential_helper = "!gh auth git-credential"'; fi
  umask 077
  if [ "$os" = Darwin ]; then
    # Every image is amd64, as on the Linux hosts, run through Rosetta on an
    # arm64 mac, where node needs more memory than on Linux for the same job.
    cat >"$conf" <<EOF
service = "$service"
labels = { arch = "x86_64" }
# Kept back from runs: the Mac's own, and what its podman machine does not have.
reserve = { cpu = ${reserve_cpu:-$fit_cpu}, memory = ${reserve_memory:-$fit_memory} }
build_memory = 1024
cgroup_parent = ""
platform = "linux/amd64"
job_memory_overhead = 512
$helper
EOF
  else
    cat >"$conf" <<EOF
service = "$service"
labels = { arch = "$(uname -m)" }
# Kept back from runs for everything else on this computer: it offers jobber
# the rest of the CPUs and memory it measures each time it starts.
reserve = { cpu = ${reserve_cpu:-2}, memory = ${reserve_memory:-4096} }
build_memory = 1024
cgroup_parent = "$slice"
$helper

[slice]
cpu_weight = 20
EOF
  fi
  say "wrote $conf: edit its reserve for this computer, then run this again"
else
  [ -z "$service$token_file$reserve_cpu$reserve_memory" ] ||
    say "kept $conf as it is: the options given are only for a first install; edit it instead" >&2
  say "kept $conf"
fi

if [ "$os" = Darwin ]; then
  grep -q '^cgroup_parent *= *""' "$conf" || say "$conf has no cgroup_parent = \"\": the podman machine has no $slice, so set it, or runs will not start" >&2
  offered=$("$bin" --config "$conf" --slice-unit | sed -n 's/^MemoryMax=\([0-9]*\)M$/\1/p')
  [ -z "$offered" ] || [ "$offered" -le $(($3 - 512)) ] || {
    say "it would offer $offered MiB, more than podman machine $1's $3 MiB less 512 of its own: raise reserve's memory in $conf to at least $fit_memory, or the machine's memory" >&2
    exit 1
  }

  mkdir -p "$(dirname "$agent")" "$HOME/Library/Logs"
  log="$HOME/Library/Logs/$name.log"
  # launchd gives a bare PATH and does not start the podman machine at login:
  # the agent starts it if stopped, then jobber-host, and launchd runs the
  # agent again whenever it exits, a machine that would not start included. A
  # machine it started is in its process group, which launchd otherwise kills
  # with it on every restart and reinstall.
  cat >"$agent" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key><array>
    <string>/bin/sh</string><string>-c</string>
    <string>(podman machine inspect --format '{{.State}}' | grep -qx running || podman machine start) &amp;&amp; exec '$bin' --config '$conf'</string>
  </array>
  <key>EnvironmentVariables</key><dict>
    <key>PATH</key><string>$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>AbandonProcessGroup</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>ExitTimeOut</key><integer>90</integer>
  <key>StandardOutPath</key><string>$log</string>
  <key>StandardErrorPath</key><string>$log</string>
</dict>
</plist>
EOF
  say "wrote $agent"
  # Booted out and in again, not kickstarted, so launchd reads the plist just written.
  launchctl bootout "$domain/$label" 2>/dev/null || true
  while launchctl print "$domain/$label" >/dev/null 2>&1; do sleep 1; done
  launchctl bootstrap "$domain" "$agent"
  say "(re)started $label: it runs from login and again whenever it exits; its log: tail -f $log"
  exit 0
fi

mkdir -p "$units"
# Sized to what it offers now, as measured with the config's reserve.
"$bin" --config "$conf" --slice-unit >"$units/$slice.new"
mv "$units/$slice.new" "$units/$slice"
say "wrote $units/$slice: $(grep -E '^(MemoryMax|CPUQuota|CPUWeight)=' "$units/$slice" | tr '\n' ' ')"

cat >"$units/$unit" <<EOF
[Unit]
Description=jobber-host: runs jobber's runs in podman, in $slice
After=network-online.target

[Service]
ExecStart=$bin --config $conf
# SIGTERM to jobber-host alone: it stops and reports its runs itself.
KillMode=mixed
TimeoutStopSec=90
Restart=always
RestartSec=10
Environment=PATH=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=default.target
EOF
say "wrote $units/$unit"

systemctl --user daemon-reload
systemctl --user enable "$unit" >/dev/null 2>&1
systemctl --user restart "$unit"
say "enabled and (re)started $unit; its log: journalctl --user -u $name -f"
if [ "$(loginctl show-user "$USER" -p Linger --value 2>/dev/null)" != yes ]; then
  if loginctl enable-linger "$USER" 2>/dev/null; then
    say "enabled lingering, so it runs without a login"
  else
    say "could not enable lingering: it stops at logout until 'loginctl enable-linger $USER' is run"
  fi
fi
