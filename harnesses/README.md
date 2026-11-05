# Prebuilt harness images

Each directory here is a **pure agent image**: the agent CLI, its runtime
dependencies, and a small `ax-harness` launcher. None of them contains the AX
runner.

## The in-container contract

| Who | Provides |
|---|---|
| AX (ActorTemplate) | The image, a durable `/workspace`, and the runner as a **read-only OCI image volume** at `/opt/ax` (`v1alpha1.RunnerVolumeImage`). The container command is always `/opt/ax/ax-task-runner`. The template carries **no per-task data**, so every task on an image shares one template and its warm snapshots. |
| AX (ax-server) | After the actor starts, `POST /v1alpha1/assignment` on port 80 with the Task and Workspace. This is retried on every resume and is idempotent. |
| Runner | Persists the assignment at `/workspace/.ax/assignment.json` (mode `0600`), prepares the workspace once, writes `spec.config.prompt` to `$AX_GOAL_FILE` and `spec.config` to `$AX_CONFIG_FILE` under `/run/ax`, then runs `spec.command`, or `/usr/local/bin/ax-harness` when `spec.command` is empty. |
| Image | `git`, CA certificates, the agent, and an executable `/usr/local/bin/ax-harness` that reads the goal and config files and runs the agent headlessly in the working directory. |

`/readyz` returns 200 while the runner waits for its assignment, so that
Substrate's wakeup probe passes. `/readyz?check=workspace` returns 503 until
the workspace is ready.

## Harnesses

| Harness | Agent | Auth |
|---|---|---|
| `antigravity` | `agy -p` (Antigravity CLI) | `GEMINI_API_KEY`, or ADC against `GOOGLE_CLOUD_PROJECT` (Vertex AI) |

## Build

```bash
make push-runner-volume                         # then pin the digest in v1alpha1.RunnerVolumeImage
make push-harness-image HARNESS=antigravity     # then pin the digest in the Task's spec.image
```

CI does the same in `.github/workflows/harness-images.yaml`, and also checks
that the harness image does not contain the runner.

## Known gaps (prototype)

- Credentials still reach the sandbox as `spec.env`. They are no longer stored
  in Substrate templates or snapshots, but they are visible to the agent. The
  target is Substrate egress credential injection.
- Shared image templates are not garbage-collected when the last task that
  uses them is deleted.
- The assignment endpoint trusts whoever reaches port 80 first. It needs to
  check the caller's identity (atenet-router or ax-server).
