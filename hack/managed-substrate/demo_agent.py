#!/usr/bin/env python3
"""Demo agent run inside an AX task on a Managed Agent Substrate microVM.

It does real work over the task's Gateway egress allowlist:
  1. Reads the latest commits of github.com/google/ax (api.github.com).
  2. Asks Gemini to turn them into release notes
     (generativelanguage.googleapis.com).
  3. Writes /workspace/report.md and /workspace/agent.log.
It also records what happens for a host the Gateway does not allow.
"""
import json
import re
import os
import subprocess
import time
import traceback
import urllib.error
import urllib.request

WS = "/workspace"
LOG = os.path.join(WS, "agent.log")


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def http_json(url, data=None, headers=None, timeout=60):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers or {})
    if body is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def trust_agent_gateway_ca():
    """Trusts the CA that inspects this sandbox's HTTPS egress.

    ACE microVM egress goes through Google Cloud Agent Gateway TLS inspection,
    and Managed Agent Substrate gives the sandbox no copy of its CA (the
    system_info trust-bundle volume AX normally mounts is unsupported on ACE).
    PROTOTYPE ONLY: take the root of the chain the gateway presents
    (trust on first use) and add it to the system bundle.
    """
    out = subprocess.run(["curl", "-sk", "-o", "/dev/null", "-w", "%{certs}", "--max-time", "10",
                          "https://api.github.com"], capture_output=True, text=True).stdout
    pems = re.findall(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", out, re.S)
    if not pems:
        log("no certificate chain seen; using system trust store")
        return
    bundle = "/tmp/egress-ca-bundle.pem"
    with open("/etc/ssl/certs/ca-certificates.crt") as src, open(bundle, "w") as dst:
        dst.write(src.read() + "\n" + pems[-1] + "\n")
    os.environ["SSL_CERT_FILE"] = bundle
    os.environ["CURL_CA_BUNDLE"] = bundle
    issuer = re.findall(r"Issuer:([^\n]*)", out)
    log(f"trusting egress inspection CA: {issuer[-1].strip() if issuer else 'unknown'}")


def main():
    log("agent starting")
    trust_agent_gateway_ca()

    # Egress check: a host that is not in the Gateway allowlist.
    r = subprocess.run(
        ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "8", "https://example.org"],
        capture_output=True, text=True)
    log(f"unlisted host example.org (not in Gateway allowlist): curl exit={r.returncode} "
        f"http={r.stdout or '-'} err={r.stderr.strip()[:200]}")

    # Step 1: real data from GitHub.
    commits = http_json("https://api.github.com/repos/google/ax/commits?per_page=5",
                        headers={"User-Agent": "ax-managed-demo"})
    titles = [f"{c['sha'][:7]} {c['commit']['message'].splitlines()[0]}" for c in commits]
    log(f"fetched {len(titles)} commits from api.github.com")

    # Step 2: Gemini writes release notes from them (Gemini Developer API).
    key = os.environ["AX_DEMO_GEMINI_KEY"]
    prompt = ("Write short release notes (a title plus 3-5 bullets) for these recent commits "
              "to the open-source AX agent executor:\n" + "\n".join(titles))
    notes, model = None, None
    for model in ["gemini-3.8-flash", "gemini-3.5-flash", "gemini-3-flash"]:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        for attempt in range(6):
            try:
                resp = http_json(url, data={"contents": [{"parts": [{"text": prompt}]}]},
                                 headers={"x-goog-api-key": key}, timeout=120)
                notes = resp["candidates"][0]["content"]["parts"][0]["text"]
                break
            except urllib.error.HTTPError as e:
                log(f"{model} attempt {attempt + 1}: HTTP {e.code}")
                if e.code not in (429, 500, 503):
                    break
                time.sleep(10 * (attempt + 1))
        if notes:
            break
    if not notes:
        raise RuntimeError("no Gemini model answered")
    log(f"Gemini ({model}) returned {len(notes)} chars")

    with open(os.path.join(WS, "report.md"), "w") as f:
        f.write("# AX release notes (written by Gemini inside a Managed Agent Substrate microVM)\n\n")
        f.write(notes.strip() + "\n\n## Source commits\n\n")
        f.write("\n".join(f"- {t}" for t in titles) + "\n")
    with open(os.path.join(WS, "state-proof.txt"), "w") as f:
        f.write(f"written-before-suspend at {int(time.time())}\n")
    log("wrote report.md and state-proof.txt; done")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log("agent failed:\n" + traceback.format_exc())
