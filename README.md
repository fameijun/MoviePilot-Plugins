# MoviePilot-Plugins（个人插件仓库）

MoviePilot v3 插件仓库，通过「插件市场 → 添加仓库地址」安装。

## litepandisk — LitePan 云盘聚合存储

把 [LitePan](https://gitea 上的 litepan-pro) 注册为 MoviePilot 的一个存储「LitePan」：

- **一个入口访问全部网盘**：根目录下每个子目录是一个 LitePan 网盘账号（123/115/光鸭/百度…），账号只在 LitePan 登录一次，MoviePilot 仅保存 LitePan 管理员账密
- **上传强制零拷贝**：整理/上传时只把「文件路径」告诉 LitePan，由 LitePan 服务器直读源盘上传——数据不经 MoviePilot 中转，也不在 LitePan 磁盘落盘；未命中路径翻译会明确报错（不做数据回退）
- **整理操作直通网盘 API**：同账号改名/移动是网盘侧秒级操作；跨账号移动自动走数据中转

### 前置条件

1. LitePan 已添加各网盘账号，管理后台已启用 WebDAV
2. LitePan ≥ 含「按路径上传」接口的版本（`POST /api/admin/tools/local-upload/upload-path`）

### 安装

MoviePilot → 设定 → 插件 → 插件市场 → 添加仓库地址（本仓库 git 地址）→ 搜索「LitePan」安装。

### 配置

| 配置项 | 说明 |
|---|---|
| 服务地址 | `http://<litepan主机>:5211`（MP 在 Docker 时填宿主机地址） |
| 管理员用户名/密码 | LitePan 管理员账密（WebDAV 与管理 API 共用） |
| 上传等待超时 | 单个上传任务等待完成的分钟数，默认 120 |
| 路径翻译 | 可选，一行一条 `MP路径前缀 => LitePan容器路径前缀` |

**路径翻译说明**：MoviePilot 与 LitePan 容器挂载路径一致时（推荐都挂 `/vol2/Downloads:/data/downloads` 这种相同容器内路径），无需配置；不一致时补一行翻译，如：

```
/media/downloads => /vol2/Downloads
```

上传 `/media/downloads/Movie/x.mkv` 时插件会把 `/vol2/Downloads/Movie/x.mkv` 告诉 LitePan 直读。

### 数据流向（本地文件 → 网盘）

```
命中翻译（零拷贝）：MP 只发路径 JSON → LitePan 直读源盘 → 秒传/分片上传网盘
同账号整理：       网盘侧 API 改名/移动（不搬数据）
跨账号移动：       经 LitePan 数据中转（下载 + 上传一遍）
```

### 边界

- LitePan 若开启两步验证（2FA），账密登录会被拒，零拷贝上传不可用
- 「其它网盘存储 → LitePan」的整理：MoviePilot 会先下载到其 TEMP 目录，TEMP 路径未在挂载/翻译范围内时会明确报错——把 MP 的 TEMP 目录也映射给 LitePan 或加入翻译即可
