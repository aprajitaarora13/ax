#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end demo of a pure prebuilt Antigravity image on a Kind cluster from
# `make e2e-setup`: two tasks with different goals on one image share one
# ActorTemplate, the runner comes from a read-only volume, and each task's goal
# reaches the agent as a file.
#
#   GOOGLE_CLOUD_PROJECT=my-project ./examples/antigravity/demo.sh
#
# Requires kubectl-ate on PATH (go build ./cmd/kubectl-ate in the Substrate
# checkout) and Application Default Credentials for Vertex AI.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck disable=SC1091
source "${REPO_ROOT}/.tmp/e2e.env"
AX="${AX_BIN}"
: "${GOOGLE_CLOUD_PROJECT:?set GOOGLE_CLOUD_PROJECT to a project with Vertex AI enabled}"
export GOOGLE_CLOUD_PROJECT
export AX_GOOGLE_CREDENTIALS_JSON="$(cat "${HOME}/.config/gcloud/application_default_credentials.json")"

step() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

render() { # render <task-name> <prompt-sed-expression>
  envsubst < "${REPO_ROOT}/examples/antigravity/task.yaml" \
    | sed -e "s/name: antigravity-review/name: $1/" -e "$2"
}

step "Apply two tasks on the same pure Antigravity image, with different goals"
render antigravity-review 's/^//' | "${AX}" apply -f -
render antigravity-history 's/explain what the/summarize the most recent changes to/' \
  | awk 'f; /^---/{f=1}' | "${AX}" apply -f -

step "Allow sandbox egress (Substrate denies it by default)"
for t in antigravity-review antigravity-history; do
  kubectl ate create egress-policy "${t}" -a default -f - <<'EOF' 2>/dev/null || true
rules:
- tls_passthrough: {hostnames: ["*"], ports: {numbers: [443]}}
- http: {hostnames: ["*"], ports: {all: {}}}
EOF
done

step "Resume both tasks"
"${AX}" resume task antigravity-review
"${AX}" resume task antigravity-history
"${AX}" get tasks

step "One shared ActorTemplate for both tasks, with no per-task env"
kubectl ate get actor-templates -a default 2>/dev/null | awk 'NR==1 || /ax-img-tmpl-/'

step "Inside the pure image: runner from the read-only volume, goal as a file"
"${AX}" ssh antigravity-review -- sh -c 'echo "PID 1: $(tr "\0" " " < /proc/1/cmdline)"; ls -l /opt/ax /usr/local/bin; touch /opt/ax/x 2>&1 | head -1; echo "--- $AX_GOAL_FILE"; cat /run/ax/goal.md' < /dev/null

step "Wait for the agent to finish, then read its answers"
for t in antigravity-review antigravity-history; do
  for _ in $(seq 1 90); do
    # ax-harness writes the answer after agy exits; wait for real content.
    if "${AX}" ssh "${t}" -- sh -c '[ "$(wc -c < /workspace/ax-output.md)" -gt 1 ]' < /dev/null 2>/dev/null; then break; fi
    sleep 10
  done
  echo "--- ${t}: /workspace/ax-output.md"
  "${AX}" ssh "${t}" -- cat /workspace/ax-output.md < /dev/null
done
