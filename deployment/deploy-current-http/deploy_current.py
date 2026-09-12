from __future__ import annotations

from datetime import datetime, timezone
import http.cookiejar
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import socket
import stat
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from inspect_remote import ROOT, WORK, REMOTE, remote_python
from ssh_client import connect, run, HOST

UNIT = "douyu-danmaku.service"
BASE = "http://" + HOST
ROOM = "231059"
PROJECT = ROOT
ARTIFACT = PROJECT / "douyu-danmaku-linux-amd64"
DESTINATION = "/opt/douyu-danmaku/douyu-danmaku-linux-amd64"
SETTINGS = "/opt/douyu-danmaku/douyu-danmaku.settings.json"
UNIT_PATH = "/etc/systemd/system/douyu-danmaku.service"
STAMP = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
BACKUP = DESTINATION + ".rollback-" + STAMP
CANDIDATE = str(PurePosixPath(DESTINATION).parent / (".douyu-danmaku-candidate-" + STAMP))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def opener():
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPCookieProcessor(jar))
    return client, jar


def request(client, path, data=None, timeout=8):
    headers = {"Origin": BASE} if data is not None else {}
    payload = urllib.parse.urlencode(data).encode() if data is not None else None
    req = urllib.request.Request(BASE + path, data=payload, headers=headers)
    try:
        response = client.open(req, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        return response.status, dict(response.headers), body


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def login(client, jar, password):
    status, headers, _ = request(client, "/login", {"password": password, "next": "/"})
    issued = any(cookie.name == "danmaku_session" for cookie in jar)
    require(status == 303 and issued and headers.get("Location") == "/", "Existing application password did not authenticate")
    return {"status": status, "session_issued": issued, "location": headers.get("Location")}


def read_remote(sftp, path):
    with sftp.open(path, "rb") as source:
        return source.read()


def checked(client, command, timeout=25):
    result = run(client, command, timeout=timeout)
    require(result["exit_code"] == 0, "Remote application operation failed: " + result["stderr"][-1200:])
    return result


def service_state(client):
    result = checked(client, "systemctl show douyu-danmaku.service --property=ActiveState --property=SubState --property=MainPID --property=NRestarts")
    return dict(line.split("=", 1) for line in result["stdout"].splitlines() if "=" in line)


def unwrap(payload):
    """负载包在 {roomId, generation, data} 信封里。早期单房间版是平铺的，
    照旧读顶层字段会得到一串 null。"""
    if isinstance(payload, dict) and "data" in payload and {"roomId", "generation"} & payload.keys():
        return payload.get("data") or {}
    return payload


def snapshot_status(data):
    room = data.get("room") or {}
    return {key: data.get(key) for key in ("phase", "message", "endpoint", "attempt", "loginOK", "groupSent", "heartbeats", "packets", "types", "events", "updatedAt", "lastReceived")} | {"room": {key: room.get(key) for key in ("input", "id", "name", "owner", "live")}}


def open_stream(client):
    """订阅房间。房间只在有人订阅后才被创建，/api/status 是只读的，
    所以这一步必须先于状态轮询，并在轮询期间一直保持连接。"""
    route = "/events?room=" + ROOM
    response = client.open(BASE + route, timeout=5)
    content_type = response.headers.get("Content-Type", "")
    if response.status != 200 or not content_type.startswith("text/event-stream"):
        response.close()
        raise RuntimeError("Authenticated SSE endpoint did not open")
    return route, response


def sse_probe(response, route, restart_time):
    result = {"route": route, "events_seen": 0, "status": response.status,
              "content_type": response.headers.get("Content-Type", "")}
    try:
        deadline = time.monotonic() + 10
        event_name, data_lines = "message", []
        while time.monotonic() < deadline:
            try:
                raw = response.readline()
            except (TimeoutError, socket.timeout):
                break
            if not raw:
                break
            line = raw.decode("utf-8").rstrip("\r\n")
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
            elif not line and data_lines:
                event = unwrap(json.loads("\n".join(data_lines)))
                result["events_seen"] += 1
                if event_name == "status":
                    result["current_status"] = {"phase": event.get("phase"), "roomId": (event.get("room") or {}).get("id"), "loginOK": event.get("loginOK"), "endpoint": event.get("endpoint")}
                if event_name == "message" and event.get("kind") in ("chat", "gift"):
                    timestamp = event.get("at", "")
                    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00")) if timestamp else None
                    if parsed and parsed >= restart_time:
                        result["fresh_event"] = {"event_name": event_name, "kind": event.get("kind"), "roomId": event.get("roomId"), "at": timestamp}
                        break
                event_name, data_lines = "message", []
        # 要 phase 真的有值：信封解错时 current_status 是一整个 null 的字典，照样为真。
        require((result.get("current_status") or {}).get("phase") or result.get("fresh_event"), "No current SSE status or chat/gift event was received")
        return result
    finally:
        response.close()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    before = json.loads((WORK / "remote-before.json").read_text(encoding="utf-8"))
    build = json.loads((WORK / "build-result.json").read_text(encoding="utf-8"))
    require(build["result"] == "PASS" and before["architecture"] == "x86_64", "Matching Linux build is not ready")
    require(before["executable"] == DESTINATION and before["actual_executable"] == DESTINATION, "Inspected application executable path does not match")
    require(before["service"]["FragmentPath"] == UNIT_PATH and not before["service"].get("DropInPaths"), "Unexpected service configuration paths")
    require(ARTIFACT.stat().st_size == build["bytes"], "Candidate artifact changed after build")
    source_html = (PROJECT / "web" / "index.html").read_bytes()
    # 口令不在源码里：DANMAKU_PASSWORD 须与候选产物构建时注入的一致；换口令时 PREVIOUS 供替换前的预检登录。
    password = os.environ.get("DANMAKU_PASSWORD", "")
    require(password, "DANMAKU_PASSWORD is not set; it must equal the value the candidate artifact was built with")
    previous = os.environ.get("DANMAKU_PASSWORD_PREVIOUS", "") or password
    evidence = {"started_at": datetime.now(timezone.utc).isoformat(), "address": BASE + "/", "build": {"target": build["target"], "bytes": build["bytes"], "cgo_enabled": build["cgo_enabled"]}, "destination": DESTINATION, "rollback_copy": BACKUP, "unit": UNIT, "http_client_proxy": "disabled; direct connection"}
    replacement_attempted = False
    backup_created = False
    with connect() as client:
        with client.open_sftp() as sftp:
            try:
                state = service_state(client)
                require(state["MainPID"] == before["service"]["MainPID"] and state["ActiveState"] == "active", "Application process changed after inspection")
                old_stat = sftp.lstat(DESTINATION)
                require(stat.S_ISREG(old_stat.st_mode), "Installed application is not a regular executable")
                require(sftp.normalize(DESTINATION) == DESTINATION, "Unexpected executable path resolution")
                preserved_files = {path: read_remote(sftp, path) for path in (UNIT_PATH, SETTINGS)}
                preserved_permissions = {path: (sftp.stat(path).st_uid, sftp.stat(path).st_gid, stat.S_IMODE(sftp.stat(path).st_mode)) for path in preserved_files}
                remote_env = dict(item.split(b"=", 1) for item in read_remote(sftp, "/proc/" + state["MainPID"] + "/environ").split(b"\0") if b"=" in item)
                if remote_env.get(b"DANMAKU_PASSWORD"):
                    password = previous = remote_env[b"DANMAKU_PASSWORD"].decode()
                auth_client, jar = opener()
                evidence["preflight_existing_password"] = login(auth_client, jar, previous)
                status, _, body = request(auth_client, "/api/settings")
                require(status == 200 and json.loads(body).get("defaultRoom") == ROOM, "Existing default room changed before deployment")
                for path in (CANDIDATE, BACKUP):
                    try:
                        sftp.lstat(path)
                    except FileNotFoundError:
                        pass
                    else:
                        raise RuntimeError("Deployment staging or rollback path already exists")
                uploaded = sftp.put(str(ARTIFACT), CANDIDATE, confirm=True)
                require(uploaded.st_size == build["bytes"], "Uploaded artifact size does not match")
                sftp.chown(CANDIDATE, old_stat.st_uid, old_stat.st_gid)
                sftp.chmod(CANDIDATE, stat.S_IMODE(old_stat.st_mode))
                checked(client, "cp --preserve=all --no-clobber -- " + shlex.quote(DESTINATION) + " " + shlex.quote(BACKUP))
                require(sftp.stat(BACKUP).st_size == old_stat.st_size, "Rollback copy was not created correctly")
                backup_created = True
                evidence["upload"] = {"bytes": uploaded.st_size, "confirmation": "SFTP size confirmed", "permissions_preserved": True}
                replacement_attempted = True
                remote_python(client, "import os, json\nos.replace(" + repr(CANDIDATE) + ", " + repr(DESTINATION) + ")\nprint(json.dumps({'replaced': True}))\n")
                restarted_at = datetime.now(timezone.utc)
                stable_started = time.monotonic()
                checked(client, "systemctl restart douyu-danmaku.service", timeout=35)
                first_state = service_state(client)
                require(first_state["ActiveState"] == "active" and first_state["SubState"] == "running" and first_state["MainPID"] not in ("0", state["MainPID"]), "Application did not start after replacement")
                evidence["restart"] = {"at": restarted_at.isoformat(), "previous_pid": state["MainPID"], "new_pid": first_state["MainPID"]}
                print(json.dumps({"stage": "application_replaced_and_restarted", "pid": first_state["MainPID"], "rollback_copy": BACKUP}), flush=True)

                anon_client, _ = opener()
                anon = {}
                for path, expected in (("/", 303), ("/login", 200), ("/api/status", 401), ("/api/settings", 401), ("/events", 401)):
                    status, headers, _ = request(anon_client, path)
                    anon[path] = {"status": status, "location": headers.get("Location")}
                    require(status == expected, "Anonymous protection failed for " + path)
                require(anon["/"]["location"].startswith("/login"), "Anonymous root did not redirect to login")
                evidence["anonymous_auth"] = anon
                auth_client, jar = opener()
                evidence["login"] = login(auth_client, jar, password)
                password = previous = None
                status, _, served_html = request(auth_client, "/")
                require(status == 200 and served_html == source_html, "Served embedded UI does not equal current local source")
                html = served_html.decode("utf-8")
                markers = {
                    # 上一版就有，防止误部署到旧产物
                    "priority_padding_for_32px_row_at_14px_font": ".priority-row{padding-block:calc(var(--content-size)/4 + 1px)}",
                    "gift_follow_group_gap_9px": ".priority-section+.priority-section{margin-top:9px}",
                    # 原规则已重构成自定义属性，标记跟着改过，否则会误判成「产物不对」。
                    "special_chat_spacing_variables": ".message-item.is-admin,.message-item.is-followed{padding-top:var(--row-pad-strong);padding-bottom:var(--row-pad-strong);margin:0 0 var(--row-gap-strong);border-radius:5px}",
                    "score_direct_command_anchor": "const SCORE_COMMAND=/^#(?:给|炸)",
                    "score_nfkc_normalization": "SCORE_COMMAND.test(cleanText(text).normalize('NFKC'))",
                    "v5_reference_source": "douyu-room-catalog-v5",
                    "v5_reference_label": "斗鱼房间目录 v5",
                    # 弹幕教练：时段布防 + 聚合震动提醒
                    "coach_page": "弹幕教练",
                    "coach_duty_window": "function coachOnDuty(",
                    "coach_vibrate_pattern": "const COACH_PATTERN=",
                    # 火狐安卓版 vibrate 返回 true 却不震，只能按 UA 排除
                    "coach_gecko_phantom_buzz_fix": "const geckoBrowser=()=>/Firefox\\/|FxiOS\\//.test(navigator.userAgent)",
                    "coach_vibrate_excludes_gecko": "const coachCanVibrate=()=>typeof navigator.vibrate==='function'&&!geckoBrowser()",
                    "coach_sound_only_when_mute": "const coachSoundOffered=()=>!coachCanVibrate()",
                    "coach_hidden_when_unavailable": "function applyCoachAvailability()",
                    "coach_sound_row_id": 'id="coach-sound-row"',
                    "coach_wake_live_id": 'id="coach-wake-live"',
                    "coach_wake_hint_id": 'id="coach-wake-hint"',
                    "coach_skip_buzz_when_hidden": "document.visibilityState==='visible'",
                    # 去重认文案不认分组，否则同一句话被拆成新组后会重复震
                    "coach_dedupe_by_text": "mark=coachSeen.get(group.text)||0",
                    # pad 左栏是留白隔开的圆角卡片，当前房间用圆头药丸标出
                    "pad_rail_card_list": ":root[data-layout=pad] .saved-room-list{padding:8px 10px 10px;display:flex;flex-direction:column;gap:3px}",
                    "pad_rail_active_pill": ":root[data-layout=pad] .saved-room-row[data-active]::before{content:'';position:absolute;left:6px;",
                }
                marker_results = {key: token in html for key, token in markers.items()}
                require(all(marker_results.values()), "Current UI deployment markers are missing")
                evidence["authenticated_ui"] = {"status": status, "bytes": len(served_html), "equals_current_local_index_html": True, "markers": marker_results}
                status, _, body = request(auth_client, "/api/settings")
                settings = json.loads(body)
                require(status == 200 and settings.get("defaultRoom") == ROOM, "Default room was not preserved")
                evidence["settings"] = {"status": status, "defaultRoom": settings["defaultRoom"]}
                route, stream = open_stream(auth_client)
                try:
                    deadline = time.monotonic() + 25
                    while True:
                        status, _, body = request(auth_client, "/api/status?rid=" + ROOM)
                        envelope = json.loads(body)
                        runtime_status = unwrap(envelope)
                        connected = status == 200 and envelope.get("roomId") == ROOM and runtime_status.get("phase") == "connected" and runtime_status.get("loginOK") and runtime_status.get("groupSent") and (runtime_status.get("room") or {}).get("id") == ROOM and runtime_status.get("endpoint", "").startswith("wss://")
                        if connected or time.monotonic() >= deadline:
                            break
                        time.sleep(2)
                    evidence["room_connection"] = {"status": status, "data": snapshot_status(runtime_status)}
                    require(connected, "Updated application did not establish its room WSS connection within the bounded check")
                    evidence["sse"] = sse_probe(stream, route, restarted_at)
                finally:
                    stream.close()
                time.sleep(max(0, 15 - (time.monotonic() - stable_started)))
                after = remote_python(client, REMOTE)
                final_state = after["service"]
                require(final_state["ActiveState"] == "active" and final_state["SubState"] == "running" and final_state["MainPID"] == first_state["MainPID"] and final_state["NRestarts"] == first_state["NRestarts"], "Application was not stable during deployment validation")
                require(final_state["User"] == before["service"]["User"] and after["start_args"] == before["start_args"] and after["actual_executable"] == DESTINATION, "Existing process user or start parameters were not preserved")
                require("pid=" + final_state["MainPID"] + "," in after["listener"], "Expected application process does not own the HTTP port 80 listener")
                preserved = all(read_remote(sftp, path) == data for path, data in preserved_files.items())
                permissions_preserved = all((sftp.stat(path).st_uid, sftp.stat(path).st_gid, stat.S_IMODE(sftp.stat(path).st_mode)) == saved for path, saved in preserved_permissions.items())
                require(preserved and permissions_preserved, "Existing settings or unit file changed during deployment")
                new_stat = sftp.stat(DESTINATION)
                require((new_stat.st_uid, new_stat.st_gid, stat.S_IMODE(new_stat.st_mode)) == (old_stat.st_uid, old_stat.st_gid, stat.S_IMODE(old_stat.st_mode)), "Executable ownership or permissions changed")
                evidence["preservation"] = {"unit_and_settings_bytes_unchanged": preserved, "unit_and_settings_permissions_unchanged": permissions_preserved, "executable_owner_and_mode_unchanged": True, "service_user_and_start_args_unchanged": True}
                evidence["remote_after"] = after
                evidence["stability_seconds"] = round(time.monotonic() - stable_started, 2)
                evidence["result"] = "PASS"
            except Exception as error:
                evidence["result"] = "FAIL"
                evidence["error"] = str(error)
                if replacement_attempted and backup_created:
                    try:
                        restore = CANDIDATE + ".restore"
                        checked(client, "cp --preserve=all --no-clobber -- " + shlex.quote(BACKUP) + " " + shlex.quote(restore))
                        remote_python(client, "import os, json\nos.replace(" + repr(restore) + ", " + repr(DESTINATION) + ")\nprint(json.dumps({'restored': True}))\n")
                        checked(client, "systemctl restart douyu-danmaku.service", timeout=35)
                        evidence["rollback"] = service_state(client)
                        evidence["rollback"]["restored_previous_executable"] = True
                    except Exception as rollback_error:
                        evidence["rollback"] = {"error": str(rollback_error), "restored_previous_executable": "unconfirmed"}
                try:
                    sftp.remove(CANDIDATE)
                except FileNotFoundError:
                    pass
            finally:
                password = previous = None
                evidence["completed_at"] = datetime.now(timezone.utc).isoformat()
                (WORK / "deployment-result.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(evidence, ensure_ascii=False, indent=2), flush=True)
    return 0 if evidence["result"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
