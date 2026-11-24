"""LitePanApi：实现 MoviePilot 的存储操作契约（对照 guangyadisk 参考插件）。

路径模型：``/<账号名>/<子路径>``；上传走 LitePan「按路径上传」零拷贝通道
（litepan 服务器直读本地文件，不在 litepan 落盘、不经本进程搬运数据）。
"""
import time
from hashlib import md5
from pathlib import Path
from typing import List, Optional, Tuple

from .litepan_client import DavItem, LitePanAdmin, LitePanClient

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


class LitePanApi:
    # MoviePilot 存储契约（StorageBase.transtype）：声明的整理方式。
    # 网盘侧支持移动/复制；链接类（link/reflink）远端不适用，交由数据中转或拒绝。
    transtype = {"move": "移动", "copy": "复制"}

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

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def _parts(path: str) -> List[str]:
        return [p for p in str(path).replace("\\", "/").split("/") if p]

    def _account_id_of(self, path: str) -> Optional[int]:
        parts = self._parts(path)
        if not parts:
            return None
        if self._accounts_cache is None:
            self._accounts_cache = self._admin.accounts()
        for acc in self._accounts_cache:
            if acc.get("name") == parts[0]:
                return int(acc["id"])
        self._accounts_cache = self._admin.accounts()  # 可能新建了账号，刷新一次
        for acc in self._accounts_cache:
            if acc.get("name") == parts[0]:
                return int(acc["id"])
        return None

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

    # -------------------------------------------------------------- FileItem
    def _root_item(self) -> schemas.FileItem:
        return schemas.FileItem(
            storage=self._disk_name, fileid="", parent_fileid="", name="",
            basename="", extension=None, type="dir", path="/", size=None,
            modify_time=None, thumbnail=None, pickcode=None, drive_id=None,
        )

    def _to_file_item(self, item: DavItem) -> schemas.FileItem:
        path = item.path
        name = item.name
        is_dir = item.is_dir
        suffix = Path(name).suffix
        # etag 缺省时用路径哈希充当 fileid（快照比对用；真实网盘 ID 不外露）。
        fileid = item.etag or md5(path.encode("utf-8")).hexdigest()
        return schemas.FileItem(
            storage=self._disk_name,
            fileid=fileid,
            parent_fileid="",
            name=name,
            basename=Path(name).stem,
            extension=suffix[1:] if suffix and not is_dir else None,
            type="dir" if is_dir else "file",
            path=path,
            size=None if is_dir else item.size,
            modify_time=item.modify_time,
            thumbnail=None,
            pickcode=None,
            drive_id=None,
        )

    def _build_path(self, *segments: str, is_dir: bool = False) -> str:
        joined = "/" + "/".join(s for s in segments if s)
        while "//" in joined:
            joined = joined.replace("//", "/")
        if is_dir and not joined.endswith("/"):
            joined += "/"
        return joined

    # ------------------------------------------------------------------ 查询
    def list(self, fileitem: schemas.FileItem, page: int = 0,
             page_size: int = 100) -> List[schemas.FileItem]:
        try:
            items, _status = self._client.propfind(fileitem.path, depth=1)
        except RuntimeError:
            return []
        if not items:
            return []
        base = fileitem.path.rstrip("/")
        out = []
        for it in items:
            if it.path.rstrip("/") == base:
                continue
            out.append(self._to_file_item(it))
        return out

    def _stat(self, path) -> Tuple[Optional[List[DavItem]], Optional[int]]:
        return self._client.propfind(str(path), depth=0)

    def get_item(self, path) -> Optional[schemas.FileItem]:
        if self._parts(str(path)) == [] or str(path) == "/":
            return self._root_item()
        try:
            items, _status = self._stat(path)
        except RuntimeError:
            return None
        if not items:
            return None
        return self._to_file_item(items[0])

    def get_item_strict(self, path) -> Optional[schemas.FileItem]:
        if self._parts(str(path)) == [] or str(path) == "/":
            return self._root_item()
        try:
            items, _status = self._stat(path)
        except RuntimeError as err:
            raise StorageQueryError(f"无法确认 {path} 的状态: {err}")
        if not items:
            return None
        return self._to_file_item(items[0])

    def get_folder(self, path) -> Optional[schemas.FileItem]:
        parts = self._parts(str(path))
        current = ""
        for seg in parts:
            current = current + "/" + seg
            if not self._client.mkcol(current):  # 405=已存在也视为成功
                return None
        target = self._build_path(*parts, is_dir=True)
        return self.get_item(target)

    def create_folder(self, fileitem: schemas.FileItem, name: str) -> Optional[schemas.FileItem]:
        parts = self._parts(fileitem.path)
        if not parts or not str(name).strip():
            return None
        target = self._build_path(*parts, str(name).strip(), is_dir=True)
        if not self._client.mkcol(target):
            return None
        return self.get_item(target)

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
        return self._client.download(fileitem.path, local_path, stop_check=stop_check)

    # -------------------------------------------------------------- 零拷贝上传
    def upload(self, target_dir: schemas.FileItem, local_path: Path,
               new_name: Optional[str] = None) -> Optional[schemas.FileItem]:
        local_path = Path(local_path)
        server_path = self._translate(local_path)
        if server_path is None:
            logger.error(
                f"【LitePan】本地路径 {local_path} 未命中「路径翻译」配置，无法零拷贝上传。"
                "本插件不做数据中转回退，请在插件配置中补充该路径的前缀翻译"
                "（或让 MoviePilot 与 LitePan 容器使用相同挂载路径）。"
            )
            return None
        account_id = self._account_id_of(target_dir.path)
        if account_id is None:
            logger.error(f"【LitePan】找不到目标账号: {self._parts(target_dir.path)[:1]}")
            return None
        dir_parts = self._parts(target_dir.path)[1:]
        dir_id = self._admin.resolve_dir_id(account_id, dir_parts)
        if dir_id is None and dir_parts:
            logger.error(f"【LitePan】目标目录不存在: {target_dir.path}")
            return None

        is_dir = local_path.is_dir()
        rel_path = (new_name or "").strip()
        task_id = LitePanAdmin.new_task_id()
        payload = self._admin.upload_by_path(
            account_id=account_id,
            target_path=dir_id or "",
            items=[{"path": server_path, "is_dir": is_dir, "rel_path": rel_path}],
            client_task_id=task_id,
            target_display_path=str(target_dir.path),
        )
        if not payload.get("success"):
            logger.error(f"【LitePan】创建零拷贝上传任务失败: {payload.get('message')}")
            return None

        task = self._admin.wait_upload_task(task_id, self._upload_timeout)
        if task is None:
            logger.error(f"【LitePan】等待上传任务完成超时（{self._upload_timeout}s）: {server_path}")
            return None
        status = str(task.get("status") or "")
        if status not in ("success", "skipped"):
            logger.error(
                f"【LitePan】上传任务未成功: status={status} "
                f"message={task.get('message')} file={server_path}"
            )
            return None

        remote_name = rel_path or local_path.name
        remote = self._build_path(*self._parts(target_dir.path), remote_name, is_dir=is_dir)
        item = self.get_item(remote)
        for _ in range(10):  # 目录树较大时可见性有延迟
            if item is not None:
                break
            time.sleep(0.5)
            item = self.get_item(remote)
        return item

    # ------------------------------------------------------------------ 删除/改名
    def delete(self, fileitem: schemas.FileItem) -> bool:
        return self._client.delete(fileitem.path)

    def rename(self, fileitem: schemas.FileItem, name: str) -> bool:
        parts = self._parts(fileitem.path)
        if len(parts) < 2:
            return False
        dst = self._build_path(*parts[:-1], name, is_dir=fileitem.type == "dir")
        return self._client.move(fileitem.path, dst)

    # ------------------------------------------------------------------ 移动/复制
    def support_transtype(self) -> dict:
        """MoviePilot 存储契约：当前存储支持的整理方式。"""
        return self.transtype

    def is_support_transtype(self, transtype: str) -> bool:
        """MoviePilot 存储契约：同存储远端整理前会先调用此方法做能力判定。"""
        return transtype in self.transtype

    def move(self, fileitem: schemas.FileItem, path, new_name: str = None) -> bool:
        return self._transfer(fileitem, path, new_name, delete_source=True)

    def copy(self, fileitem: schemas.FileItem, path, new_name: str = None) -> bool:
        return self._transfer(fileitem, path, new_name, delete_source=False)

    def _transfer(self, fileitem: schemas.FileItem, path, new_name: Optional[str],
                  delete_source: bool) -> bool:
        src_parts = self._parts(fileitem.path)
        dst_parts = self._parts(str(path))
        if len(src_parts) < 2 or len(dst_parts) < 1:
            return False
        target_name = new_name or fileitem.name
        if src_parts[0] == dst_parts[0]:
            # 同账号：网盘侧 API（rename/move 秒级；copy 走服务端复制）。
            dst = self._build_path(*dst_parts, target_name, is_dir=fileitem.type == "dir")
            if delete_source:
                return self._client.move(fileitem.path, dst)
            if self._copy_within_account(fileitem, dst_parts, target_name):
                return True
            logger.warning(f"【LitePan】网盘侧复制失败，回退数据中转: {fileitem.path}")
        ok = self._transfer_stream(fileitem, dst_parts, target_name)
        if ok and delete_source:
            self._client.delete(fileitem.path)
        return ok

    def _copy_within_account(self, fileitem: schemas.FileItem, dst_parts: List[str],
                             target_name: str) -> bool:
        """同账号复制：解析真实网盘 ID 后调 LitePan 的 copy 接口（服务端网盘侧复制）。"""
        account_id = self._account_id_of(fileitem.path)
        if account_id is None:
            return False
        src_parts = self._parts(fileitem.path)
        item_id, parent_id, _is_dir = self._admin.resolve_item(account_id, src_parts[1:])
        if item_id is None:
            return False
        target_parent = self._admin.resolve_dir_id(account_id, dst_parts[1:]) if len(dst_parts) > 1 else ""
        if target_parent is None:
            return False
        if not self._admin.copy_files(account_id, item_id, target_parent, parent_id):
            return False
        if target_name != fileitem.name:
            # 网盘侧 copy 保留原名：先取原名副本再改名成目标名。
            copied = self.get_item(self._build_path(*dst_parts, fileitem.name,
                                                    is_dir=fileitem.type == "dir"))
            if copied is not None:
                return self.rename(copied, target_name)
        return True

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
        dst = self._build_path(*dst_parts, target_name)
        if not self._client.put_stream(dst, src):
            src.close()
            return False
        return True

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
