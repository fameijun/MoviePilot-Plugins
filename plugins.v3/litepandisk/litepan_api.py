"""LitePanApi：实现 MoviePilot 的存储操作契约（做法对照 p123disk 插件）。

路径模型：``/<账号名>/<子路径>``。

整理（列目录、查目标、移动、改名、复制、删除）走 LitePan 的 JSON 接口，按网盘文件 ID
操作；插件自己缓存「目录路径 → ID」，认识了的目录不再逐级去网盘列目录。早期版本把
路径交给 WebDAV，LitePan 每个请求都要从账号根目录逐级解析，网盘侧一次整理要等几十秒。

上传走 LitePan「按路径上传」零拷贝通道（litepan 服务器直读本地文件，不在 litepan
落盘、不经本进程搬运数据）；跨账号整理和下载仍经 WebDAV。
"""
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from .litepan_client import (UPLOAD_TERMINAL_STATUS, LitePanAdmin, LitePanClient,
                            UploadTasksError, human_size)

from app import schemas

try:
    from app.schemas import StorageQueryError
except ImportError:  # 兼容宿主 schema 布局差异
    from app.schemas.exception import StorageQueryError

try:
    from app.sdk.config import global_vars, settings
except ImportError:
    global_vars = None
    settings = None

try:
    from app.sdk.logging import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)


class _QueryError(Exception):
    """LitePan 查询失败（不等于「确认不存在」）。account_id 指明要清哪个账号的缓存。"""

    def __init__(self, account_id: int, message: str):
        super().__init__(message)
        self.account_id = account_id


def _fmt_elapsed(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f} 毫秒"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} 秒"
    if seconds < 3600:
        return f"{seconds // 60} 分 {seconds % 60} 秒"
    return f"{seconds // 3600} 小时 {(seconds % 3600) // 60} 分"


def _tree_size(path: Path, limit: int = 20000) -> tuple:
    """目录里所有文件的总大小。文件数超过 limit 就不再数下去，第二个返回值为 False。"""
    total = 0
    complete = True
    count = 0
    for child in path.rglob("*"):
        try:
            if child.is_file():
                total += child.stat().st_size
        except OSError:
            continue
        count += 1
        if count >= limit:
            complete = False
            break
    return total, complete


class _UploadProgressLog:
    """把一批上传任务的轮询快照写进日志。

    单个或少数几个文件：阶段变化、每 10%、每 30 秒各记一条。
    文件多（上传整个目录）：只记汇总，免得刷屏。
    任务进入终态不在这里记，由调用方写汇总。
    """

    PER_TASK_LIMIT = 4       # 不超过这个数就按文件记
    STEP = 10                # 每上传 10% 记一条
    SECONDS = 30             # 不足 10% 但过了 30 秒也记一条（大文件、慢网盘）
    # LitePan 在把任务标成终态之前，会先报一条「running + 这些文案」。
    FINAL_MESSAGES = ("上传成功", "秒传成功", "已跳过", "已存在")

    def __init__(self, expected: int):
        self.expected = max(int(expected or 1), 1)
        self.per_task = self.expected <= self.PER_TASK_LIMIT
        self.marks: Dict[str, tuple] = {}   # 单文件：task_id → (百分比, 阶段文案, 时间)
        self.queued: set = set()            # 已经记过「已排队」的任务
        self.total_logged = False
        self.aggregate = (0, 0.0)           # 汇总：(上次记录时的已完成数, 时间)

    def update(self, tasks: List[dict]):
        if self.per_task:
            for task in tasks:
                self._one(task)
        else:
            self._many(tasks)

    def _one(self, task: dict):
        status = str(task.get("status") or "")
        key = str(task.get("task_id"))
        name = str(task.get("file_name") or task.get("display_name") or "")
        total = int(task.get("total_bytes") or 0)
        message = str(task.get("message") or "")
        if status == "pending":
            if key not in self.queued:
                self.queued.add(key)
                logger.info(f"【LitePan】已排队等待 LitePan 上传: {name}（{human_size(total)}）")
            return
        if status in UPLOAD_TERMINAL_STATUS or any(m in message for m in self.FINAL_MESSAGES):
            return
        done = int(task.get("uploaded_bytes") or 0)
        speed = float(task.get("speed_bytes_per_second") or 0)
        percent = int(task.get("progress") or 0)
        now = time.monotonic()
        last = self.marks.get(key)
        due = (last is None or last[1] != message
               or percent - last[0] >= self.STEP or now - last[2] >= self.SECONDS)
        if not due:
            return
        self.marks[key] = (percent, message, now)
        detail = f"{percent}%"
        if total:
            detail += f"（{human_size(done)} / {human_size(total)}"
            if speed > 0:
                detail += f"，{human_size(speed)}/s"
                if total > done:
                    detail += f"，预计还需 {_fmt_elapsed((total - done) / speed)}"
            detail += "）"
        logger.info(f"【LitePan】上传中: {name} {detail}{'· ' + message if message else ''}")

    def _many(self, tasks: List[dict]):
        total_bytes = sum(int(t.get("total_bytes") or 0) for t in tasks)
        sent = sum(int(t.get("uploaded_bytes") or 0) for t in tasks)
        speed = sum(float(t.get("speed_bytes_per_second") or 0) for t in tasks)
        done = sum(1 for t in tasks if str(t.get("status") or "") in UPLOAD_TERMINAL_STATUS)
        now = time.monotonic()
        if not self.total_logged:
            self.total_logged = True
            self.aggregate = (done, now)
            logger.info(f"【LitePan】共 {self.expected} 个文件、{human_size(total_bytes)}，"
                        "由 LitePan 逐个上传")
            return
        last_done, last_time = self.aggregate
        if done - last_done < max(1, self.expected // 10) and now - last_time < self.SECONDS:
            return
        self.aggregate = (done, now)
        detail = f"已完成 {done}/{self.expected} 个文件，{human_size(sent)} / {human_size(total_bytes)}"
        if speed > 0:
            detail += f"，{human_size(speed)}/s"
            if total_bytes > sent:
                detail += f"，预计还需 {_fmt_elapsed((total_bytes - sent) / speed)}"
        logger.info(f"【LitePan】上传中: {detail}")


class LitePanApi:
    # MoviePilot 存储契约（StorageBase.transtype）：声明的整理方式。
    # 网盘侧支持移动/复制；链接类（link/reflink）远端不适用，交由数据中转或拒绝。
    transtype = {"move": "移动", "copy": "复制"}
    # 同账号复制后，等新副本在目标目录里出现的时间上限（秒）。
    copy_visible_seconds = 60.0

    def __init__(
        self,
        client: LitePanClient,
        admin: LitePanAdmin,
        disk_name: str = "LitePan",
        upload_timeout: int = 7200,
        path_mappings: Optional[List[Tuple[str, str]]] = None,
    ):
        self._client = client
        self._admin = admin
        self._disk_name = disk_name
        self._upload_timeout = max(int(upload_timeout or 7200), 60)
        # 路径翻译表：[(MP 本地前缀, litepan 容器内前缀)]；为空表示两边路径一致（恒等直传）。
        self._path_mappings = path_mappings or []
        self._accounts_cache: Optional[List[dict]] = None
        # 目录缓存：(account_id, "/a/b") → 目录项（fileid 即网盘目录 ID）。
        # 不过期，只在本插件删改、或查询失败时清理：MoviePilot 的整理目录基本固定，
        # 认识一次之后就不用再逐级去网盘列目录。
        self._dirs: Dict[Tuple[int, str], schemas.FileItem] = {}
        self._lock = threading.RLock()
        self._account_warned_at = 0.0

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def _parts(path: str) -> List[str]:
        return [p for p in str(path).replace("\\", "/").split("/") if p]

    @staticmethod
    def _dir_key(parts: List[str]) -> str:
        return "/" + "/".join(parts)

    @staticmethod
    def _build_path(*segments: str, is_dir: bool = False) -> str:
        joined = "/" + "/".join(s for s in segments if s)
        while "//" in joined:
            joined = joined.replace("//", "/")
        if is_dir and not joined.endswith("/"):
            joined += "/"
        return joined

    def _translate(self, local_path: Path) -> Optional[str]:
        """MP 本地路径 → litepan 容器内路径。未配置翻译表=恒等；配了但未命中=配置缺口。"""
        p = str(local_path)
        if not self._path_mappings:
            return p
        for mp_prefix, server_prefix in sorted(
            self._path_mappings, key=lambda m: -len(m[0])
        ):
            mp_prefix = mp_prefix.rstrip("/")
            if p == mp_prefix or p.startswith(mp_prefix + "/"):
                rest = p[len(mp_prefix):].lstrip("/")
                target = server_prefix.rstrip("/") + "/" + rest
                return target.rstrip("/")
        return None

    def _invalidate_accounts(self):
        self._accounts_cache = None

    # ------------------------------------------------------------------ 账号
    def _accounts(self) -> List[dict]:
        if self._accounts_cache is None:
            self._accounts_cache = self._admin.accounts() or []
        return self._accounts_cache

    def _account_by_name(self, name: str) -> Optional[dict]:
        found = next((a for a in self._accounts() if a.get("name") == name), None)
        if found is None:
            self._accounts_cache = self._admin.accounts() or []  # 可能新建了账号
            found = next((a for a in self._accounts_cache if a.get("name") == name), None)
        return found

    def _account_by_id(self, account_id: int) -> Optional[dict]:
        return next((a for a in self._accounts() if a.get("id") == account_id), None)

    def _locate(self, path) -> Optional[Tuple[int, List[str]]]:
        """``/<账号名>/...`` → (account_id, 账号内的路径段)。

        存储根返回 (0, [])；账号不存在返回 None。
        """
        parts = self._parts(str(path))
        if not parts:
            return 0, []
        account = self._account_by_name(parts[0])
        if account is None:
            self._warn_unknown_account(parts[0])
            return None
        return int(account.get("id") or 0), parts[1:]

    def _warn_unknown_account(self, name: str):
        now = time.monotonic()
        if now - self._account_warned_at < 60:  # 同一分钟内只提示一次
            return
        self._account_warned_at = now
        if self._accounts():
            logger.warning(f"【LitePan】LitePan 里没有名为「{name}」的账号")
        else:
            logger.error("【LitePan】拿不到 LitePan 的账号列表，请检查插件配置的服务地址和管理员账号密码")

    # -------------------------------------------------------------- FileItem
    def _root_item(self) -> schemas.FileItem:
        return schemas.FileItem(
            storage=self._disk_name, fileid="", parent_fileid="", name="",
            basename="", extension=None, type="dir", path="/", size=None,
            modify_time=None, thumbnail=None, pickcode=None, drive_id=None,
        )

    def _account_item(self, account: dict) -> schemas.FileItem:
        name = str(account.get("name") or "")
        # 根目录的 ID 用 "0"（LitePan 把 ""、"0" 都当根），避免给 MoviePilot 一个空 fileid。
        item = schemas.FileItem(
            storage=self._disk_name, fileid="0", parent_fileid="", name=name,
            basename=name, extension=None, type="dir", path=f"/{name}/", size=None,
            modify_time=None, thumbnail=None, pickcode=None, drive_id=None,
        )
        self._dirs[(int(account.get("id") or 0), "/")] = item
        return item

    @staticmethod
    def _epoch(value) -> Optional[float]:
        """LitePan 接口里的 RFC3339 时间 → 秒级时间戳；解析不了返回 None。"""
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).strip().replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    def _item_from(self, parent: schemas.FileItem, raw: dict) -> Optional[schemas.FileItem]:
        name = str(raw.get("name") or "")
        if not name:
            return None
        is_dir = bool(raw.get("is_dir"))
        suffix = Path(name).suffix
        return schemas.FileItem(
            storage=self._disk_name,
            fileid=str(raw.get("id") or ""),
            parent_fileid=parent.fileid,
            name=name,
            basename=Path(name).stem,
            extension=suffix[1:] if suffix and not is_dir else None,
            type="dir" if is_dir else "file",
            path=parent.path + name + ("/" if is_dir else ""),
            size=None if is_dir else int(raw.get("size") or 0),
            modify_time=self._epoch(raw.get("mod_time")),
            thumbnail=None,
            pickcode=None,
            drive_id=None,
        )

    # ------------------------------------------------------------------ 目录缓存
    def _dir_cached(self, account_id: int, parts: List[str]) -> Optional[schemas.FileItem]:
        return self._dirs.get((account_id, self._dir_key(parts)))

    def _forget_under(self, path):
        """忘掉某路径及其子路径的目录缓存：它被本插件删改过，或在插件外被改动过。"""
        norm = self._dir_key(self._parts(str(path)))
        with self._lock:
            for key in [k for k in self._dirs if k[1] == norm or k[1].startswith(norm + "/")]:
                del self._dirs[key]

    def _forget_account(self, account_id: int):
        if not account_id:
            return
        with self._lock:
            for key in [k for k in self._dirs if k[0] == account_id]:
                del self._dirs[key]

    # ------------------------------------------------------------------ 查询
    def _children(self, account_id: int, parent: schemas.FileItem) -> List[schemas.FileItem]:
        """列一次目录。失败抛 _QueryError：那是「查不了」，不是「确认不存在」。"""
        payload = self._admin.list_dir(account_id, parent.fileid)
        if not payload.get("success"):
            raise _QueryError(account_id, f"列目录失败 {parent.path}: {payload.get('message')}")
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        return [item for item in (self._item_from(parent, raw) for raw in (data.get("items") or [])
                                  if isinstance(raw, dict))
                if item is not None]

    def _attempt(self, fn: Callable[[], object], strict: bool = False):
        """执行一次查询；失败就清掉该账号的目录缓存重试一次。

        目录可能在插件不知情的时候被删改，缓存里的 ID 已经失效，重试时按路径重新解析。
        重试仍失败：strict 抛存储查询异常（MoviePilot 据此判定「无法确认」），否则返回 None。
        """
        try:
            return fn()
        except _QueryError as err:
            logger.debug(f"【LitePan】{err}，清掉目录缓存后重试")
            self._forget_account(err.account_id)
        try:
            return fn()
        except _QueryError as err:
            if strict:
                raise StorageQueryError(f"【LitePan】无法确认状态: {err}") from err
            logger.warning(f"【LitePan】{err}")
            return None

    def _resolve(self, account_id: int, parts: List[str], create: bool):
        return self._attempt(lambda: self._resolve_dir(account_id, parts, create))

    def _resolve_dir(self, account_id: int, parts: List[str],
                     create: bool) -> Optional[schemas.FileItem]:
        """逐级解析目录并返回目录项，只对缓存里没有的层级查询（create 时顺带创建）。

        create=False 且目录不存在、路径中间是文件、或创建失败时返回 None。
        """
        with self._lock:
            if account_id == 0:  # 存储根：下面是各个账号，没有可解析的目录
                return self._root_item() if not parts else None
            account = self._account_by_id(account_id)
            if account is None:
                raise _QueryError(account_id, f"LitePan 里没有 ID 为 {account_id} 的账号")
            current = self._dir_cached(account_id, []) or self._account_item(account)
            for index, name in enumerate(parts):
                prefix = parts[:index + 1]
                cached = self._dir_cached(account_id, prefix)
                if cached is not None:
                    current = cached
                    continue
                children = self._children(account_id, current)
                found = next((c for c in children if c.type == "dir" and c.name == name), None)
                if found is None and create:
                    if any(c.type == "file" and c.name == name for c in children):
                        return None
                    found = self._create_dir(account_id, current, name)
                if found is None:
                    return None
                self._dirs[(account_id, self._dir_key(prefix))] = found
                current = found
            return current

    def _create_dir(self, account_id: int, parent: schemas.FileItem,
                    name: str) -> Optional[schemas.FileItem]:
        payload = self._admin.create_folder(account_id, parent.fileid, name)
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        folder_id = str(data.get("folder_id") or "")
        if payload.get("success") and folder_id:
            logger.info(f"【LitePan】已创建目录: {parent.path}{name}/")
            return schemas.FileItem(
                storage=self._disk_name, fileid=folder_id, parent_fileid=parent.fileid,
                name=name, basename=name, extension=None, type="dir",
                path=parent.path + name + "/", size=None, modify_time=None,
                thumbnail=None, pickcode=None, drive_id=None,
            )
        # 没建成：可能刚被别的线程建好，或网盘那边已有同名目录而列表还没刷新。再列一次父目录确认。
        existing = next((c for c in self._children(account_id, parent)
                         if c.type == "dir" and c.name == name), None)
        if existing is None:
            logger.error(f"【LitePan】创建目录失败: {parent.path}{name}/（{payload.get('message') or '网盘未返回目录 ID'}）")
        else:
            logger.debug(f"【LitePan】目录已存在，直接复用: {parent.path}{name}/")
        return existing

    def _mutate(self, fileitem: schemas.FileItem, action: Callable[[str], bool]) -> bool:
        """按文件 ID 执行一次写操作；失败时按路径重新解析 ID 再试一次。

        MoviePilot 回放的旧任务可能带着早期版本插件生成的 fileid；目录也可能在插件外被
        删改过。文件 ID 就是路径的存储（如本机存储）移动后 ID 会变，也靠这里补救。
        """
        if action(fileitem.fileid or ""):
            return True
        self._forget_under(fileitem.path)
        fresh = self.get_item(fileitem.path)
        if fresh is None or fresh.fileid == fileitem.fileid:
            return False
        return bool(action(fresh.fileid))

    def _query_item(self, path) -> Optional[schemas.FileItem]:
        """按路径查文件/目录项：列一次父目录取最新状态（父目录链走缓存，不再逐级查询）。

        不直接返回缓存里的目录项：目录可能在插件外被删改，查到的必须是现在的样子。
        """
        located = self._locate(path)
        if located is None:
            return None
        account_id, parts = located
        if account_id == 0:
            return self._root_item()
        account = self._account_by_id(account_id)
        if account is None:
            raise _QueryError(account_id, f"LitePan 里没有 ID 为 {account_id} 的账号")
        if not parts:
            return self._account_item(account)
        parent = self._resolve_dir(account_id, parts[:-1], create=False)
        if parent is None:
            return None
        return next((c for c in self._children(account_id, parent) if c.name == parts[-1]), None)

    def _list_dir(self, account_id: int, parts: List[str]) -> Optional[List[schemas.FileItem]]:
        dir_item = self._resolve_dir(account_id, parts, create=False)
        if dir_item is None:
            return None
        return self._children(account_id, dir_item)

    def list(self, fileitem: schemas.FileItem, page: int = 0,
             page_size: int = 100) -> List[schemas.FileItem]:
        if fileitem.type == "file":
            item = self.get_item(fileitem.path)
            return [item] if item is not None else []
        located = self._locate(fileitem.path)
        if located is None:
            return []
        account_id, parts = located
        if account_id == 0:
            return [self._account_item(account) for account in self._accounts()]
        # 解析目录和列目录放在同一次尝试里：目录缓存过期时，重试会按路径重新解析。
        return self._attempt(lambda: self._list_dir(account_id, parts)) or []

    def get_item(self, path) -> Optional[schemas.FileItem]:
        return self._attempt(lambda: self._query_item(path))

    def get_item_strict(self, path) -> Optional[schemas.FileItem]:
        return self._attempt(lambda: self._query_item(path), strict=True)

    def get_folder(self, path) -> Optional[schemas.FileItem]:
        """确保目录存在并返回目录项。各级目录认识过一次之后就不再查询，缺失的层级才创建。"""
        located = self._locate(path)
        if located is None:
            return None
        dir_item = self._resolve(located[0], located[1], create=True)
        return dir_item if dir_item is not None and dir_item.type == "dir" else None

    def create_folder(self, fileitem: schemas.FileItem, name: str) -> Optional[schemas.FileItem]:
        name = str(name or "").strip()
        located = self._locate(fileitem.path)
        if located is None or located[0] == 0 or not name:
            return None  # 存储根下是账号，不能建目录
        account_id, parts = located
        with self._lock:
            return self._attempt(lambda: self._create_under(account_id, parts, name))

    def _create_under(self, account_id: int, parts: List[str], name: str) -> Optional[schemas.FileItem]:
        parent = self._resolve_dir(account_id, parts, create=False)
        if parent is None:
            return None
        existing = next((c for c in self._children(account_id, parent) if c.name == name), None)
        if existing is not None:
            # 同名目录已存在：直接复用，不让网盘再建一个；同名的是文件则没办法建。
            return existing if existing.type == "dir" else None
        created = self._create_dir(account_id, parent, name)
        if created is not None:
            self._dirs[(account_id, self._dir_key(parts + [name]))] = created
        return created

    def get_parent(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        path = fileitem.path.rstrip("/")
        idx = path.rfind("/")
        parent = path[:idx] or "/"
        if parent != "/":
            parent += "/"
        return self.get_item(parent)

    # ------------------------------------------------------------------ 下载
    def download(self, fileitem: schemas.FileItem, path: Path = None) -> Optional[Path]:
        try:
            base_dir = Path(settings.TEMP_PATH)
        except Exception:
            base_dir = Path("/tmp")
        local_path = Path(path) if path else base_dir / (fileitem.name or "litepan_download")
        stop_check = None
        if global_vars is not None:
            stop_check = lambda: global_vars.is_transfer_stopped(fileitem.path)  # noqa: E731
        started = time.monotonic()
        result = self._client.download(fileitem.path, local_path, stop_check=stop_check)
        if result is None:
            logger.error(f"【LitePan】下载失败: {fileitem.path} → {local_path}")
        else:
            size = result.stat().st_size
            logger.info(
                f"【LitePan】已下载: {fileitem.path} → {local_path}（{human_size(size)}，"
                f"用时 {_fmt_elapsed(time.monotonic() - started)}，经 WebDAV）"
            )
        return result

    # -------------------------------------------------------------- 零拷贝上传
    def upload(self, target_dir: schemas.FileItem, local_path: Path,
               new_name: Optional[str] = None) -> Optional[schemas.FileItem]:
        """零拷贝上传：把路径告诉 LitePan，由它从自己磁盘上读文件传到网盘。

        数据不经过 MoviePilot；MoviePilot 和 LitePan 必须能看到同一个文件（必要时配「路径翻译」）。
        """
        started = time.monotonic()
        local_path = Path(local_path)
        server_path = self._translate(local_path)
        if server_path is None:
            logger.error(
                f"【LitePan】上传取消：本地路径 {local_path} 未命中「路径翻译」配置，无法零拷贝上传。"
                "本插件不做数据中转回退，请在插件配置中补充该路径的前缀翻译"
                "（或让 MoviePilot 与 LitePan 容器使用相同挂载路径）。"
            )
            return None
        located = self._locate(target_dir.path)
        if located is None or located[0] == 0:
            logger.error(f"【LitePan】上传取消：找不到目标账号 {self._parts(target_dir.path)[:1]}")
            return None
        account_id, dir_parts = located
        dir_item = self._resolve(account_id, dir_parts, create=False)
        if dir_item is None:
            logger.error(f"【LitePan】上传取消：目标目录不存在 {target_dir.path}")
            return None

        is_dir = local_path.is_dir()
        rel_path = (new_name or "").strip()
        target_name = rel_path or local_path.name
        target = f"【{self._disk_name}】{str(target_dir.path).rstrip('/')}/{target_name}"
        where = f"，在 LitePan 那边是 {server_path}" if server_path != str(local_path) else ""
        logger.info(
            f"【LitePan】开始上传（零拷贝）: {local_path} → {target}"
            f"（{self._describe_size(local_path, is_dir)}{where}；"
            "数据不经过 MoviePilot，由 LitePan 从自己的磁盘读取后直传网盘）"
        )
        task_id = LitePanAdmin.new_task_id()
        payload = self._admin.upload_by_path(
            account_id=account_id,
            target_path=dir_item.fileid,
            items=[{"path": server_path, "is_dir": is_dir, "rel_path": rel_path}],
            client_task_id=task_id,
            target_display_path=str(target_dir.path),
        )
        if not payload.get("success"):
            logger.error(f"【LitePan】上传失败：LitePan 没有接受这次上传（{payload.get('message')}）")
            return None
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        progress = _UploadProgressLog(int(data.get("count") or 1))
        try:
            tasks = self._admin.wait_upload_tasks(
                task_id, self._upload_timeout, on_update=progress.update,
                expected=progress.expected, account_id=account_id)
        except UploadTasksError as err:
            logger.error(f"【LitePan】上传失败：{err}")
            return None
        except TimeoutError:
            logger.error(
                f"【LitePan】上传超时：已经等了 {_fmt_elapsed(time.monotonic() - started)}，"
                f"超过插件设置的上限（{self._upload_timeout} 秒）。任务仍在 LitePan 后台继续，"
                "可到 LitePan 的任务面板查看；文件大或上行带宽小时，请调大插件配置里的上传超时"
            )
            return None

        elapsed = time.monotonic() - started
        # 上传目录时一个上传请求会展开成多个任务；按最差的结果判定整批。
        rank = {"success": 0, "skipped": 0, "failed": 1, "canceled": 1, "paused": 1}
        worst = max(tasks, key=lambda t: rank.get(str(t.get("status") or ""), 1))
        status = str(worst.get("status") or "")
        sent = sum(int(t.get("uploaded_bytes") or 0) for t in tasks)
        if status not in ("success", "skipped"):
            failed = [t for t in tasks if str(t.get("status") or "") not in ("success", "skipped")]
            reason = worst.get("error") or worst.get("message") or "LitePan 没有给出原因"
            which = (f"；{len(failed)}/{len(tasks)} 个文件未成功，第一个是 {failed[0].get('file_name')}"
                     if len(tasks) > 1 else "")
            logger.error(
                f"【LitePan】上传失败（{status}）: {reason}{which}；"
                f"已传 {human_size(sent)}，用时 {_fmt_elapsed(elapsed)} → {target}"
            )
            return None
        skipped = [t for t in tasks if t.get("status") == "skipped"]
        instant = any("秒传" in str(t.get("message") or "") for t in tasks)
        notes = []
        if skipped:
            notes.append(f"{len(skipped)} 个网盘已有同名文件，已跳过")
        if instant:
            notes.append("秒传：网盘已有相同文件，没有真正上传数据")
        count = f"{len(tasks)} 个文件，" if len(tasks) > 1 else ""
        speed = f"，平均 {human_size(sent / elapsed)}/s" if elapsed > 0 and sent and not instant else ""
        logger.info(
            f"【LitePan】上传完成: {count}共 {human_size(sent)}，用时 {_fmt_elapsed(elapsed)}{speed}"
            f"{'（' + '；'.join(notes) + '）' if notes else ''} → {target}"
        )

        # 上传单个文件时，任务结果里直接带网盘文件 ID，不用再去查一遍目录。
        # 上传目录时结果里记的是目录内部的文件，目录本身按名字到目标目录里找。
        result = worst.get("result") if isinstance(worst.get("result"), dict) else {}
        remote_name = str((not is_dir and result.get("file_name")) or target_name)
        remote = self._build_path(*self._parts(target_dir.path), remote_name, is_dir=is_dir)
        file_id = "" if is_dir else str(result.get("file_id") or "")
        if file_id:
            suffix = Path(remote_name).suffix
            size = result.get("size")
            item = schemas.FileItem(
                storage=self._disk_name, fileid=file_id, parent_fileid=dir_item.fileid,
                name=remote_name, basename=Path(remote_name).stem,
                extension=suffix[1:] if suffix and not is_dir else None,
                type="dir" if is_dir else "file", path=remote,
                size=None if is_dir or size is None else int(size),
                modify_time=None, thumbnail=None, pickcode=None, drive_id=None,
            )
        else:
            item = None
            for _ in range(10):  # 目录树较大时可见性有延迟
                item = self.get_item(remote)
                if item is not None:
                    break
                time.sleep(0.5)
        if is_dir and item is not None and item.type == "dir":
            self._dirs[(account_id, self._dir_key(dir_parts + [remote_name]))] = item
        return item

    @staticmethod
    def _describe_size(local_path: Path, is_dir: bool) -> str:
        """上传开始那一行里的「多大」：文件给大小，目录给总大小；拿不到就不显示。"""
        try:
            if not is_dir:
                return human_size(local_path.stat().st_size)
            total, complete = _tree_size(local_path)
            return f"目录，{'至少 ' if not complete else ''}{human_size(total)}"
        except OSError as err:
            return f"大小未知（{err.__class__.__name__}）"

    # ------------------------------------------------------------------ 删除/改名
    def delete(self, fileitem: schemas.FileItem) -> bool:
        located = self._locate(fileitem.path)
        if located is None or not located[1]:
            return False
        account_id, parts = located
        parent = self._resolve(account_id, parts[:-1], create=False)
        if parent is None:
            return False
        ok = self._mutate(
            fileitem,
            lambda file_id: self._admin.delete_files(account_id, [file_id], parent.fileid),
        )
        if ok:
            self._forget_under(fileitem.path)
            logger.info(f"【LitePan】已删除: {fileitem.path}（进网盘回收站）")
        else:
            logger.error(f"【LitePan】删除失败: {fileitem.path}")
        return ok

    def rename(self, fileitem: schemas.FileItem, name: str, *, _inside_transfer: bool = False) -> bool:
        """改名。_inside_transfer=True 表示它是「移动+改名」里的一步，只记 DEBUG。"""
        name = str(name or "").strip()
        log = logger.debug if _inside_transfer else logger.info
        located = self._locate(fileitem.path)
        if located is None or not located[1] or not name:
            return False
        if name == fileitem.name:
            return True
        account_id, parts = located
        parent = self._resolve(account_id, parts[:-1], create=False)
        if parent is None:
            logger.error(f"【LitePan】改名失败: 找不到所在目录 {fileitem.path}")
            return False
        ok = self._mutate(fileitem, lambda file_id: self._admin.rename_file(
            account_id, file_id, name, parent.fileid))
        if ok:
            self._forget_under(fileitem.path)
            log(f"【LitePan】已改名: {fileitem.path} → {name}")
        else:
            logger.error(f"【LitePan】改名失败: {fileitem.path} → {name}")
        return ok

    # ------------------------------------------------------------------ 移动/复制
    def support_transtype(self) -> dict:
        """MoviePilot 存储契约：当前存储支持的整理方式。"""
        return self.transtype

    def is_support_transtype(self, transtype: str) -> bool:
        """MoviePilot 存储契约：同存储远端整理前会先调用此方法做能力判定。"""
        if transtype in self.transtype:
            return True
        names = {"link": "硬链接", "softlink": "软链接", "reflink": " reflink（写时复制）".strip()}
        shown = names.get(transtype, transtype)
        if logger is not None:
            logger.warning(
                f"【LitePan】不支持「{shown}」整理方式：网盘上的文件没法做链接，"
                f"请在 MoviePilot 的存储设置里把【{self._disk_name}】的整理方式改成"
                + "或".join(f"「{self.transtype[k]}」" for k in self.transtype)
                + f"。整理到【{self._disk_name}】的文件如果选了「{shown}」，MoviePilot 只会报「未知错误」，原因在这里"
            )
        return False

    def move(self, fileitem: schemas.FileItem, path, new_name: str = None) -> bool:
        return self._transfer(fileitem, path, new_name, delete_source=True)

    def copy(self, fileitem: schemas.FileItem, path, new_name: str = None) -> bool:
        return self._transfer(fileitem, path, new_name, delete_source=False)

    def _transfer(self, fileitem: schemas.FileItem, path, new_name: Optional[str],
                  delete_source: bool) -> bool:
        started = time.monotonic()
        action = "移动" if delete_source else "复制"
        source = self._locate(fileitem.path)
        target = self._locate(path)
        target_name = str(new_name or fileitem.name).strip()
        if (source is None or target is None or not source[1] or not target_name
                or fileitem.type not in ("file", "dir")):
            logger.warning(
                f"【LitePan】{action}取消: 路径不在 LitePan 存储里（{fileitem.path} → {path}），"
                "该操作交由 MoviePilot 处理"
            )
            return False
        kind = "目录" if fileitem.type == "dir" else f"{human_size(fileitem.size)} 的文件"
        same_account = source[0] == target[0]
        logger.info(
            f"【LitePan】开始{action}: {fileitem.path} → {str(path).rstrip('/')}/{target_name}"
            f"（{kind}；{'同一账号，网盘侧接口直接' + action if same_account else '跨账号，经 MoviePilot 数据中转'}）"
        )
        if same_account:
            outcome = self._transfer_within_account(
                fileitem, source, target[1], target_name, delete_source)
            if outcome is None:
                # 网盘已受理复制，但目标目录里还没看到新副本（可能还在后台复制）。
                # 这时再中转一份就会重复，所以直接报失败。
                logger.error(
                    f"【LitePan】{action}失败: 网盘已受理，但 {self.copy_visible_seconds:.0f} 秒内没在目标目录"
                    f"看到新副本 {str(path).rstrip('/')}/{target_name}"
                    "（有的网盘复制在后台进行；为避免出现两份，没有回退数据中转）"
                    f"，用时 {_fmt_elapsed(time.monotonic() - started)}"
                )
                return False
            if outcome or delete_source:
                level = logger.info if outcome else logger.error
                level(f"【LitePan】{action}{'完成' if outcome else '失败'}: {fileitem.path}"
                      f"（网盘侧接口，不搬数据，用时 {_fmt_elapsed(time.monotonic() - started)}）")
                return outcome  # 移动失败不做数据中转，免得中转出错还去删源；复制失败再回退
            logger.warning(f"【LitePan】网盘侧复制失败，回退数据中转: {fileitem.path}")
        elif delete_source:
            logger.info(
                f"【LitePan】跨账号{action}：先经 MoviePilot 把数据传到目标账号"
                "（读一遍、写一遍），成功后再删除源文件"
            )
        ok = self._transfer_stream(fileitem, self._parts(str(path)), target_name)
        if ok and delete_source:
            self.delete(fileitem)
        level = logger.info if ok else logger.error
        level(f"【LitePan】{action}{'完成' if ok else '失败'}（数据中转）: {fileitem.path}"
              f"（用时 {_fmt_elapsed(time.monotonic() - started)}）")
        return ok

    def _transfer_within_account(self, fileitem: schemas.FileItem, located: Tuple[int, List[str]],
                                 dst_parts: List[str], target_name: str,
                                 delete_source: bool) -> Optional[bool]:
        """同账号：网盘侧接口直接移动/复制，不搬数据。

        返回 True 表示完成；False 表示网盘没执行这次操作（复制可以回退数据中转）；
        None 只在复制时出现：网盘受理了，但新副本还没在目标目录里看到，不能回退。
        """
        account_id, src_parts = located
        dst_dir = self._resolve(account_id, dst_parts, create=False)
        src_parent = self._resolve(account_id, src_parts[:-1], create=False)
        if dst_dir is None or src_parent is None:
            return False
        if dst_dir.fileid != src_parent.fileid:
            if delete_source:
                if not self._mutate(fileitem, lambda file_id: self._admin.move_files(
                        account_id, [file_id], dst_dir.fileid, src_parent.fileid)):
                    return False
                self._forget_under(fileitem.path)
                # 网盘侧移动保留原名：接下来在目标目录里按原名找到它，再改成目标名。
                is_dir = fileitem.type == "dir"
                fileitem = schemas.FileItem(
                    storage=self._disk_name, fileid=fileitem.fileid, parent_fileid=dst_dir.fileid,
                    name=fileitem.name, basename=Path(fileitem.name).stem,
                    extension=fileitem.extension, type=fileitem.type,
                    path=dst_dir.path + fileitem.name + ("/" if is_dir else ""),
                    size=fileitem.size, modify_time=fileitem.modify_time,
                    thumbnail=None, pickcode=None, drive_id=None,
                )
            else:
                known = {c.fileid for c in (self._attempt(
                    lambda: self._children(account_id, dst_dir)) or [])}
                if not self._mutate(fileitem, lambda file_id: self._admin.copy_files(
                        account_id, [file_id], dst_dir.fileid, src_parent.fileid)):
                    return False
                fileitem = self._find_new_copy(account_id, dst_dir, fileitem, known)
                if fileitem is None:
                    return None
        return self.rename(fileitem, target_name, _inside_transfer=True)

    def _find_new_copy(self, account_id: int, dst_dir: schemas.FileItem,
                       source: schemas.FileItem, known_ids: set) -> Optional[schemas.FileItem]:
        """复制后找出新副本：目标目录里与源同名、且不是复制前就在的那些项之一。

        123、光鸭、夸克这些网盘，LitePan 会等复制做完才返回，新副本立刻就在。
        115、OneDrive、OpenList 的复制则是提交后在后台进行，LitePan 返回时新副本
        还没出现，这里等它露头。
        """
        attempts = max(int(self.copy_visible_seconds / 0.5), 1)
        for attempt in range(attempts):
            children = self._attempt(lambda: self._children(account_id, dst_dir)) or []
            candidates = [c for c in children if c.name == source.name
                          and c.type == source.type and c.fileid not in known_ids]
            if candidates:
                return candidates[-1]
            if attempt < attempts - 1:
                time.sleep(0.5)
        return None   # 原因由调用方（_transfer）写进失败日志

    def _transfer_stream(self, fileitem: schemas.FileItem, dst_parts: List[str],
                         target_name: str) -> bool:
        """数据中转（跨账号，或网盘侧复制失败时）：经本插件 GET→PUT。"""
        if fileitem.type == "dir":
            if not self._client.mkcol(self._build_path(*dst_parts, target_name, is_dir=True)):
                return False
            for child in self.list(fileitem):
                if not self._transfer_stream(child, [*dst_parts, target_name], child.name):
                    return False
            return True
        src = self._client.stream(fileitem.path)
        if src is None:
            return False
        ok = self._client.put_stream(self._build_path(*dst_parts, target_name), src)
        src.close()
        return ok

    # ------------------------------------------------------------------ 快照/用量
    def snapshot(self, fileitem: schemas.FileItem) -> List[schemas.FileItem]:
        out: List[schemas.FileItem] = []

        def walk(item: schemas.FileItem):
            for child in self.list(item):
                if child.type == "dir":
                    walk(child)
                else:
                    out.append(child)

        walk(fileitem)
        return out

    def usage(self) -> schemas.StorageUsage:
        total = used = 0.0
        for acc in self._admin.accounts():
            total += float(acc.get("profile_total_bytes") or 0)
            used += float(acc.get("profile_used_bytes") or 0)
        return schemas.StorageUsage(total=total, available=max(total - used, 0.0))
