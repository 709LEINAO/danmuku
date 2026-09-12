# VPS 部署

| 项目 | 配置 |
| --- | --- |
| systemd 服务 | `douyu-danmaku.service`，开机启动、失败自动重启 |
| 运行账号 | `douyu-danmaku`，不可交互登录 |
| 程序 | `/opt/douyu-danmaku/douyu-danmaku-linux-amd64` |
| 默认房间配置 | `/opt/douyu-danmaku/douyu-danmaku.settings.json` |
| 服务定义 | `/etc/systemd/system/douyu-danmaku.service` |
| 监听 | `0.0.0.0:80` |
| 启动参数 | 只有 `-addr 0.0.0.0:80` |

程序和服务定义由 root 持有；服务账号可保存相邻的房间配置。systemd 仅授予绑定低端口所需的 `CAP_NET_BIND_SERVICE`。主机原有的 SSH 与防火墙配置保持原状。

服务端不再自动连接任何房间，房间清单保存在各浏览器本地；`douyu-danmaku.settings.json` 里的 `defaultRoom`（仍为 `231059`）只是遗留字段，页面已不再调用，程序也不再据此连接。它保留在验收项里，纯粹用来确认部署没有意外改动服务端文件。

登录会话保存在内存中，固定有效期 24 小时；服务重启后需要重新登录。没有新增认证持久化文件。启动时可用非空 `DANMAKU_PASSWORD` 覆盖构建时注入的密码。

## 维护命令

在 VPS 上执行：

```sh
systemctl status douyu-danmaku --no-pager
systemctl restart douyu-danmaku
journalctl -u douyu-danmaku -n 50 --no-pager
```

`douyu-danmaku.settings.json` 由服务在启动时读取，但当前版本不再使用其中的 `defaultRoom`，页面也不会写它。要换看的房间，直接在网页的「我的房间」里加，不必碰服务器。

## 部署流程

开发机 Python 是 3.11.9，依赖在 `deployment/requirements.txt`：

```sh
py -m pip install -r deployment/requirements.txt
```

主机地址、SSH 用户与口令都从环境变量读取（`VPS_HOST`、`VPS_USER` 默认 `root`、`VPS_PASSWORD`），不落盘、也不写进任何 json 产物。主机公钥在首次连接时固定到 `deployment/vps-deployment/known_hosts`，此后变更即拒连；该文件属于本地信任状态，不入仓库。应用口令 `DANMAKU_PASSWORD` 同样只走环境变量：构建脚本用它注入产物，部署脚本用它做验收登录；换口令时把线上旧口令放在 `DANMAKU_PASSWORD_PREVIOUS`，供替换前的预检登录。三步：

```sh
DANMAKU_PASSWORD=... sh build-linux.sh
cd deployment/deploy-current-http && VPS_HOST=... VPS_PASSWORD=... py inspect_remote.py
cd deployment/deploy-current-http && VPS_HOST=... VPS_PASSWORD=... DANMAKU_PASSWORD=... py deploy_current.py
```

`build-linux.sh` 交叉编译并写出 `build-result.json`（含产物字节数）；`inspect_remote.py` 只读，刷新 `remote-before.json`。`deploy_current.py` 的前置校验拿这两个文件比对，过期就拒绝部署——尤其是 MainPID 和产物字节数。两个 json 与二进制都是构建／运行产物，不入库。

`deploy_current.py` 做的事：上传到暂存文件、按旧程序的属主权限设置、`cp --preserve=all --no-clobber` 备份、`os.replace` 原子替换（正在运行的可执行文件不能覆盖写，但换目录项可以）、重启，然后跑完上面那套验收项。任一步失败自动回滚并重启。证据写入 `deployment-result.json`。

改动 UI 后记得同步 `deploy_current.py` 里的 `markers`。

接口形状上：`/api/status` 和 `/events` 都必须带房间参数（分别是 `?rid=` 和 `?room=`，缺了是 400），而且只有订阅 `/events` 才会创建房间——`/api/status` 是只读的，没人订阅时返回 404 `room_inactive`。两者的响应都包在 `{roomId, generation, data}` 信封里。
