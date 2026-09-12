"""Task-scoped SSH client. Host, user and credential come from VPS_HOST, VPS_USER (default root) and VPS_PASSWORD."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

import paramiko

HOST = os.environ.get("VPS_HOST", "")
USER = os.environ.get("VPS_USER", "root")
WORK = Path(__file__).resolve().parent
PIN_FILE = WORK / "known_hosts"


def original_password() -> str:
    """从环境变量取 SSH 口令，不落盘、不进任何 json 产物。"""
    password = os.environ.get("VPS_PASSWORD", "")
    if not password:
        raise RuntimeError("VPS_PASSWORD is not set; export it before running the deployment tooling")
    return password


class PinFirstObservedKey(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        if hostname != HOST or PIN_FILE.exists():
            raise paramiko.SSHException("Unexpected or changed host key; connection stopped")
        client.get_host_keys().add(hostname, key.get_name(), key)
        client.save_host_keys(str(PIN_FILE))
        fingerprint = base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
        (WORK / "ssh-host-key.json").write_text(
            json.dumps({"host": hostname, "key_type": key.get_name(),
                        "fingerprint": "SHA256:" + fingerprint,
                        "trust": "First observed key; no previous user or system known-host entry existed"}, indent=2),
            encoding="utf-8",
        )


def connect() -> paramiko.SSHClient:
    if not HOST:
        raise RuntimeError("VPS_HOST is not set; export it before running the deployment tooling")
    client = paramiko.SSHClient()
    user_ssh = Path(os.environ["USERPROFILE"]) / ".ssh"
    global_ssh = Path(os.environ.get("ProgramData", r"C:\ProgramData")) / "ssh"
    for path in (user_ssh / "known_hosts", user_ssh / "known_hosts2",
                 global_ssh / "ssh_known_hosts", global_ssh / "ssh_known_hosts2"):
        if path.is_file():
            client.load_system_host_keys(str(path))
    if PIN_FILE.is_file():
        client.load_host_keys(str(PIN_FILE))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        client.set_missing_host_key_policy(PinFirstObservedKey())
    password = original_password()
    try:
        client.connect(HOST, port=22, username=USER, password=password,
                       allow_agent=False, look_for_keys=False,
                       timeout=12, banner_timeout=12, auth_timeout=12)
    finally:
        password = None
    return client


def run(client: paramiko.SSHClient, command: str, timeout: int = 20) -> dict:
    stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    stdin.close()
    output = stdout.read().decode("utf-8", errors="replace")
    errors = stderr.read().decode("utf-8", errors="replace")
    return {"exit_code": stdout.channel.recv_exit_status(), "stdout": output, "stderr": errors}


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    commands = {
        "architecture": "uname -m",
        "os": "cat /etc/os-release",
        "identity": "id",
        "disk": "df -h / /opt",
        "listeners": "ss -ltnp",
        "init": "ps -p 1 -o comm=",
        "web_service_units": "systemctl list-unit-files --no-legend --no-pager nginx.service apache2.service httpd.service caddy.service douyu-danmaku.service",
        "running_services": "systemctl list-units --type=service --state=running --no-pager --plain",
        "tools": "command -v python3 curl nginx caddy ufw firewall-cmd iptables nft getenforce",
        "app_path": "if test -e /opt/douyu-danmaku || test -L /opt/douyu-danmaku; then stat -c '%F %U %G %a %n' /opt/douyu-danmaku; readlink -f /opt/douyu-danmaku; else printf 'absent\\n'; fi",
        "app_user": "getent passwd douyu-danmaku",
        "ufw": "if command -v ufw >/dev/null 2>&1; then ufw status verbose; fi",
        "firewalld": "if command -v firewall-cmd >/dev/null 2>&1; then firewall-cmd --state; firewall-cmd --list-all; fi",
        "iptables": "if command -v iptables >/dev/null 2>&1; then iptables -S; fi",
        "nftables": "if command -v nft >/dev/null 2>&1; then nft list ruleset; fi",
        "selinux": "if command -v getenforce >/dev/null 2>&1; then getenforce; fi",
    }
    with connect() as client:
        evidence = {name: run(client, command) for name, command in commands.items()}
    (WORK / "remote-inspection.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2))
