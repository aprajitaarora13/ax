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

# Pure Antigravity agent image: no AX runner inside.
#
# AX mounts the runner read-only at /opt/ax and runs it as PID 1. The runner
# writes the task's goal and config to files and starts /usr/local/bin/ax-harness
# with AX_GOAL_FILE and AX_CONFIG_FILE pointing at them. The image only has to
# provide the agent, git, CA certificates, and that launcher.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        bash ca-certificates curl git jq procps python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# The installer drops a self-contained binary; install it on the default PATH.
RUN curl -fsSL https://antigravity.google/cli/install.sh | bash -s -- --dir /usr/local/bin \
    && agy --version

COPY ax-harness /usr/local/bin/ax-harness
RUN chmod 0755 /usr/local/bin/ax-harness
