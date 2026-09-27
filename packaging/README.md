# Packaging

Two unit files and two environment files. Static text, checked in — not generated
by a `build.rs`, because a build script writes only into `OUT_DIR`, runs before
anyone has chosen an install path, and would have to guess `ExecStart=`. What
actually varies per node goes in the environment file, which is whatever
provisions the node's job.

```sh
# On a compute node
install -m644 cs-agent.service /etc/systemd/system/
install -m644 -D agent.env.example /etc/clusterservices/agent.env
$EDITOR /etc/clusterservices/agent.env          # CS_SERVER at least
systemctl enable --now cs-agent

# On the head
install -m644 cs-server.service /etc/systemd/system/
install -m644 -D server.env.example /etc/clusterservices/server.env
systemctl enable --now cs-server
```

Both expect the binaries at `/usr/bin`; `cargo build --release` puts them in
`target/release`.

## What the units encode

Everything here is a contract from `CLAUDE.md`, not a preference:

| Setting | Why |
|---|---|
| `Restart=on-failure` + `RestartForceExitStatus=75` | **75 is the restart mechanism.** `cs-ctl restart` on a whole agent makes it shut down gracefully and exit 75; systemd starts it again. Nothing ever re-executes itself. |
| Exit 0 is *not* restarted | `cs-ctl shutdown` means it. With `Restart=always` the two verbs would be indistinguishable. |
| `TimeoutStopSec=20s` | The engine's own shutdown deadline is 10s and bounds everything under it, including the join of plugin worker threads. Room to finish, then insist. |
| `Slice=system.slice` (agent) | So `cs-plugin-selfmon` measures the agent, and so it never sits under `slurmstepd.scope` looking like a job. |
| `After=slurmd.service`, not `Requires=` | An agent that starts before there are jobs finds none and says so. One that refuses to start because slurmd was late is missing for the restart you wanted to watch. |
| `DynamicUser=yes` (head) | It binds two unprivileged ports and reads nothing. `StateDirectory=` is where storage goes when there is any. |
| Root, for now (agent) | Cgroup files are world-readable on a normal node, so your own `User=` works today — NVML per-process attribution, which the GPU plugin needs, generally will not. |

## Ports

| Port | Who | Notes |
|---|---|---|
| 7777 | agents → head | `CS_LISTEN` |
| 7788 | `cs-ctl` → head | `CS_ADMIN`. **Loopback by default.** Nothing in this protocol authenticates anybody, so reaching this port is the entire authorization for restarting every node the head serves. |
