from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
WORK = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "deployment" / "vps-deployment"))
from ssh_client import connect

REMOTE = r'''
import json, os, pathlib, platform, pwd, grp, re, shlex, stat, subprocess
unit = "douyu-danmaku.service"
keys = ["LoadState", "ActiveState", "SubState", "MainPID", "NRestarts", "User", "Group", "WorkingDirectory", "FragmentPath", "DropInPaths", "EnvironmentFiles", "UnitFileState", "ExecStart", "ExecMainStartTimestamp", "CapabilityBoundingSet", "AmbientCapabilities"]
proc = subprocess.run(["systemctl", "show", unit] + ["--property=" + key for key in keys], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10, check=True)
props = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
raw_start = props.pop("ExecStart", "")
match = re.search(r"(?:^|\{ )path=([^;]+?) ;", raw_start)
if not match:
    raise RuntimeError("Could not resolve configured application executable")
exe = match.group(1).strip()
pid = int(props["MainPID"])
if pid <= 0:
    raise RuntimeError("Existing application has no running process")
process_path = pathlib.Path("/proc") / str(pid)
args = process_path.joinpath("cmdline").read_bytes().rstrip(b"\0").decode().split("\0")
environment = dict(item.split(b"=", 1) for item in process_path.joinpath("environ").read_bytes().split(b"\0") if b"=" in item)
safe_args = [args[0]]
i = 1
while i < len(args):
    arg = args[i]
    if arg in ("-addr", "--addr", "-room", "--room") and i + 1 < len(args):
        safe_args.extend((arg, args[i + 1]))
        i += 2
    elif any(arg.startswith(prefix) for prefix in ("-addr=", "--addr=", "-room=", "--room=")):
        safe_args.append(arg)
        i += 1
    else:
        safe_args.append(arg.split("=", 1)[0] + "=<redacted>" if arg.startswith("-") else "<redacted>")
        i += 1
def metadata(path):
    entry = pathlib.Path(path)
    item = entry.lstat()
    return {"path": str(entry), "real_path": str(entry.resolve()), "is_symlink": entry.is_symlink(), "is_regular": stat.S_ISREG(item.st_mode), "bytes": item.st_size, "mode": oct(stat.S_IMODE(item.st_mode)), "uid": item.st_uid, "gid": item.st_gid, "owner": pwd.getpwuid(item.st_uid).pw_name, "group": grp.getgrgid(item.st_gid).gr_name}
app_dir = str(pathlib.Path(exe).parent)
settings_path = str(pathlib.Path(app_dir) / "douyu-danmaku.settings.json")
settings = json.loads(pathlib.Path(settings_path).read_text())
files = [app_dir, exe, settings_path, props["FragmentPath"]]
files.extend(shlex.split(props.get("DropInPaths", "")))
listeners = subprocess.run(["ss", "-ltnp", "sport", "=", ":80"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=8, check=True).stdout.strip()
print(json.dumps({"architecture": platform.machine(), "system": platform.system(), "service": props, "executable": exe, "actual_executable": os.readlink(str(process_path / "exe")), "start_args": safe_args, "password_environment_override": bool(environment.get(b"DANMAKU_PASSWORD")), "application_environment_keys": sorted(key.decode() for key in environment if key.startswith(b"DANMAKU_")), "settings": {"defaultRoom": settings.get("defaultRoom")}, "metadata": [metadata(path) for path in files], "listener": listeners}, ensure_ascii=False))
'''

def remote_python(client, script: str, timeout: int = 30):
    stdin, stdout, stderr = client.exec_command("python3 -", timeout=timeout)
    stdin.write(script)
    stdin.flush()
    stdin.channel.shutdown_write()
    output = stdout.read().decode("utf-8", errors="replace")
    errors = stderr.read().decode("utf-8", errors="replace")
    code = stdout.channel.recv_exit_status()
    stdin.close()
    stdout.close()
    stderr.close()
    if code:
        raise RuntimeError("Remote inspection failed (exit %d): %s" % (code, errors[-3000:]))
    return json.loads(output)

def main():
    sys.stdout.reconfigure(encoding="utf-8")
    if not (ROOT / "deployment" / "vps-deployment" / "known_hosts").is_file():
        raise RuntimeError("Existing pinned host-key history is missing")
    with connect() as client:
        evidence = remote_python(client, REMOTE)
    (WORK / "remote-before.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
