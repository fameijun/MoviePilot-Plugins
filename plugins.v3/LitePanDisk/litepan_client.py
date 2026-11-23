"""LitePan HTTP 客户端：WebDAV（文件操作）+ 管理 JSON API（登录/零拷贝上传）。

路径模型：MP 侧统一使用虚拟路径 ``/<账号名>/<子路径>...``，与 LitePan WebDAV
``/dav/<账号名>/...`` 一一对应（客户端内部自动加 /dav 前缀与 URL 编码）。
"""
import time
import uuid
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote, urlsplit

import requests

DAV_NS = "{DAV:}"
# 终态：与 litepan upload 包的状态常量对齐。
UPLOAD_TERMINAL_STATUS = {"success", "failed", "canceled", "skipped"}
CHUNK_SIZE = 10 * 1024 * 1024


class DavItem:
    """PROPFIND 解析结果。path 为虚拟路径（目录带尾斜杠）。"""

    __slots__ = ("name", "path", "is_dir", "size", "modify_time", "etag")

    def __init__(self, name: str, path: str, is_dir: bool,
                 size: Optional[int] = None, modify_time: Optional[float] = None,
                 etag: Optional[str] = None):
        self.name = name
        self.path = path
        self.is_dir = is_dir
        self.size = size
        self.modify_time = modify_time
        self.etag = etag


class LitePanClient:
    """LitePan WebDAV 客户端（Basic 认证 = LitePan 管理员账密）。"""

    def __init__(self, base_url: str, username: str, password: str, timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.dav_base = self.base_url + "/dav"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (username, password)

    def close(self):
        self.session.close()

    # ------------------------------------------------------------------ 路径
    @staticmethod
    def normalize_path(path: str) -> str:
        """虚拟路径规范化：始终以 / 开头、目录保留尾斜杠信息由调用方维护。"""
        path = (path or "/").replace("\\", "/")
        if not path.startswith("/"):
            path = "/" + path
        while "//" in path:
            path = path.replace("//", "/")
        if len(path) > 1 and path.endswith("/"):
            path = path.rstrip("/") or "/"
        return path

    def _url(self, path: str) -> str:
        return self.dav_base + quote(self.normalize_path(path))

    @staticmethod
    def _href_to_path(href: str) -> str:
        """响应 href（URL 编码、含 /dav 前缀）→ 虚拟路径。"""
        path = unquote(urlsplit(href).path)
        prefix = "/dav"
        if path.startswith(prefix):
            path = path[len(prefix):]
        return path or "/"

    # -------------------------------------------------------------- PROPFIND
    def propfind(self, path: str, depth: int = 0) -> Tuple[Optional[List[DavItem]], Optional[int]]:
        """返回 (items, status)：200 → items（含自身）；404 → (None, 404)；其它 → (None, code)。"""
        url = self._url(path)
        try:
            resp = self.session.request(
                "PROPFIND", url,
                headers={"Depth": str(depth), "Content-Type": "application/xml"},
                data='<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop>'
                     '<D:displayname/><D:resourcetype/><D:getcontentlength/>'
                     '<D:getlastmodified/><D:getetag/></D:prop></D:propfind>',
                timeout=self.timeout,
            )
        except requests.RequestException as err:
            raise RuntimeError(f"LitePan WebDAV 请求失败: {err}") from err
        if resp.status_code == 404:
            return None, 404
        if resp.status_code != 207:
            return None, resp.status_code
        return self._parse_multistatus(resp.content), 207

    def _parse_multistatus(self, content: bytes) -> List[DavItem]:
        items: List[DavItem] = []
        root = ET.fromstring(content)
        for response in root.iter(f"{DAV_NS}response"):
            href_el = response.find(f"{DAV_NS}href")
            if href_el is None or not href_el.text:
                continue
            path = self._href_to_path(href_el.text.strip())
            prop = response.find(f"{DAV_NS}propstat/{DAV_NS}prop")
            if prop is None:
                continue
            is_dir = prop.find(f"{DAV_NS}resourcetype/{DAV_NS}collection") is not None
            name = path.rstrip("/").rpartition("/")[2] or "/"
            size: Optional[int] = None
            length_el = prop.find(f"{DAV_NS}getcontentlength")
            if length_el is not None and length_el.text and not is_dir:
                try:
                    size = int(length_el.text)
                except ValueError:
                    size = None
            modify_time: Optional[float] = None
            mtime_el = prop.find(f"{DAV_NS}getlastmodified")
            if mtime_el is not None and mtime_el.text:
                try:
                    modify_time = parsedate_to_datetime(mtime_el.text).timestamp()
                except (TypeError, ValueError):
                    modify_time = None
            etag_el = prop.find(f"{DAV_NS}getetag")
            etag = etag_el.text.strip() if etag_el is not None and etag_el.text else None
            if is_dir and not path.endswith("/"):
                path += "/"
            items.append(DavItem(name, path, is_dir, size, modify_time, etag))
        return items

    # ----------------------------------------------------------------- 写操作
    def mkcol(self, path: str) -> bool:
        try:
            resp = self.session.request("MKCOL", self._url(path), timeout=self.timeout)
        except requests.RequestException:
            return False
        return resp.status_code in (200, 201, 405)  # 405=已存在

    def delete(self, path: str) -> bool:
        try:
            resp = self.session.request("DELETE", self._url(path), timeout=self.timeout)
        except requests.RequestException:
            return False
        return resp.status_code in (200, 204, 404)

    def move(self, src: str, dst: str) -> bool:
        """同账号 MOVE（网盘侧 rename/move API，不搬数据）。dst 已存在时不覆盖（412）。"""
        headers = {"Destination": self._url(dst), "Overwrite": "F"}
        try:
            resp = self.session.request(
                "MOVE", self._url(src), headers=headers, timeout=self.timeout)
        except requests.RequestException:
            return False
        return resp.status_code in (200, 201, 204)

    # ----------------------------------------------------------------- 数据流
    def download(self, path: str, local_path, stop_check=None) -> Optional[Path]:
        """流式下载到 local_path（文件）。stop_check() 为真时中止并清理。"""
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.session.get(self._url(path), stream=True, timeout=self.timeout) as resp:
                if resp.status_code != 200:
                    return None
                with open(local_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if stop_check and stop_check():
                            fh.close()
                            local_path.unlink(missing_ok=True)
                            return None
                        if chunk:
                            fh.write(chunk)
            return local_path
        except requests.RequestException:
            local_path.unlink(missing_ok=True)
            return None

    def stream(self, path: str) -> Optional[requests.Response]:
        """跨账号中转用的源流。"""
        try:
            resp = self.session.get(self._url(path), stream=True, timeout=self.timeout)
        except requests.RequestException:
            return None
        return resp if resp.status_code == 200 else None

    def put_stream(self, path: str, source: requests.Response) -> bool:
        """把源流写到目标（跨账号中转用）。"""
        try:
            resp = self.session.put(
                self._url(path), data=source.iter_content(chunk_size=CHUNK_SIZE),
                timeout=self.timeout)
            return resp.status_code in (200, 201, 204)
        except requests.RequestException:
            return False


class LitePanAdmin:
    """LitePan 管理 JSON API：cookie session 登录 + 零拷贝上传。"""

    def __init__(self, base_url: str, username: str, password: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/") + "/api"
        self.username = username
        self.password = password
        self.timeout = timeout
        self.session = requests.Session()
        self._logged_in = False

    def close(self):
        self.session.close()

    def _login(self) -> bool:
        try:
            resp = self.session.post(
                f"{self.base_url}/auth/login",
                data={"username": self.username, "password": self.password, "remember": "1"},
                timeout=self.timeout,
            )
        except requests.RequestException:
            return False
        if resp.status_code != 200:
            return False
        try:
            payload = resp.json()
        except ValueError:
            return False
        self._logged_in = bool(payload.get("success"))
        return self._logged_in

    def _request(self, method: str, path: str, **kwargs) -> Dict[str, Any]:
        """带 401 重登的请求，返回响应 JSON dict；失败返回 {"success": False, ...}。"""
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)
        for attempt in range(2):
            try:
                resp = self.session.request(method, url, **kwargs)
            except requests.RequestException as err:
                return {"success": False, "message": f"LitePan 请求失败: {err}"}
            if resp.status_code == 401 and attempt == 0 and self._login():
                continue
            if resp.status_code != 200:
                return {"success": False, "message": f"LitePan HTTP {resp.status_code}"}
            try:
                return resp.json()
            except ValueError:
                return {"success": False, "message": "LitePan 响应不是 JSON"}
        return {"success": False, "message": "LitePan 登录失败"}

    # -------------------------------------------------------------- 账号信息
    def accounts(self) -> List[Dict[str, Any]]:
        """获取活跃账号列表（id/name/空间用量）。优先管理员 /accounts（带 profile
        空间数据），失败再退回免登录的 /public/accounts（无空间字段）。"""
        payload = self._request("GET", "/admin/accounts")
        data = payload.get("data") if isinstance(payload, dict) else None
        if not payload.get("success") or not isinstance(data, list):
            payload = self._request("GET", "/public/accounts")
            data = payload.get("data") if isinstance(payload, dict) else None
            if not payload.get("success") or not isinstance(data, list):
                return []
        return [a for a in data
                if a.get("id") and a.get("is_active", True)]

    def list_dir(self, account_id: int, parent_id: str, force: bool = False) -> Dict[str, Any]:
        params = {"account_id": account_id, "parent_id": parent_id}
        if force:
            params["force_refresh"] = "true"
        return self._request("GET", "/files/list", params=params)

    def resolve_dir_id(self, account_id: int, dir_parts: List[str]) -> Optional[str]:
        """按路径逐级解析网盘目录 ID；不存在返回 None。空 parts 返回根 parent（空串）。"""
        parent_id = ""
        for name in dir_parts:
            payload = self.list_dir(account_id, parent_id, force=True)
            if not payload.get("success"):
                return None
            items = (payload.get("data") or {}).get("items") or []
            matched = next((it for it in items
                            if it.get("is_dir") and it.get("name") == name), None)
            if matched is None:
                return None
            parent_id = str(matched.get("id") or "")
        return parent_id

    def resolve_item(self, account_id: int, parts: List[str]):
        """解析路径末段（可为文件）→ (item_id, parent_id, is_dir)；不存在返回 (None, None, False)。"""
        if not parts:
            return "", "", True
        *dir_parts, name = parts
        parent_id = self.resolve_dir_id(account_id, dir_parts)
        if parent_id is None:
            return None, None, False
        payload = self.list_dir(account_id, parent_id, force=True)
        if not payload.get("success"):
            return None, None, False
        items = (payload.get("data") or {}).get("items") or []
        matched = next((it for it in items if it.get("name") == name), None)
        if matched is None:
            return None, None, False
        return str(matched.get("id") or ""), parent_id, bool(matched.get("is_dir"))

    def copy_files(self, account_id: int, file_id: str,
                   target_parent_id: str, source_parent_id: str) -> bool:
        """同账号复制：网盘侧 copy_file（服务端执行，不经客户端搬数据）。"""
        payload = self._request("POST", "/files/copy", json={
            "account_id": account_id,
            "file_ids": [file_id],
            "target_parent_id": target_parent_id,
            "source_parent_id": source_parent_id,
        }, timeout=max(self.timeout, 600))
        return bool(payload.get("success"))

    # -------------------------------------------------------------- 零拷贝上传
    def upload_by_path(self, account_id: int, target_path: str, items: List[Dict[str, Any]],
                       client_task_id: str, conflict_policy: str = "overwrite",
                       target_display_path: str = "") -> Dict[str, Any]:
        payload = {
            "account_id": account_id,
            "target_path": target_path,
            "target_display_path": target_display_path,
            "conflict_policy": conflict_policy,
            "client_task_id": client_task_id,
            "items": items,
        }
        return self._request("POST", "/admin/tools/local-upload/upload-path", json=payload)

    def list_upload_tasks(self) -> List[Dict[str, Any]]:
        payload = self._request("GET", "/files/upload/tasks", params={"limit": 200})
        if not payload.get("success"):
            return []
        data = payload.get("data")
        return data if isinstance(data, list) else []

    def wait_upload_task(self, client_task_id: str, timeout_seconds: int,
                         interval: float = 3.0) -> Optional[Dict[str, Any]]:
        """轮询匹配的任务至终态；超时返回 None。

        LitePan 会给任务 client_task_id 追加序号（xxx-0），batch_id 保留原值，
        因此按 batch_id 精确匹配 + client_task_id 前缀匹配双重兜底。
        """
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            for task in self.list_upload_tasks():
                task_cid = str(task.get("client_task_id") or "")
                if task.get("batch_id") == client_task_id or task_cid == client_task_id \
                        or task_cid.startswith(client_task_id + "-"):
                    status = str(task.get("status") or "")
                    if status in UPLOAD_TERMINAL_STATUS:
                        return task
                    break
            time.sleep(interval)
        return None

    @staticmethod
    def new_task_id() -> str:
        return f"mp-{uuid.uuid4().hex[:16]}"
