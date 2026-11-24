"""LitePan HTTP 客户端：管理 JSON API（登录、按 ID 的文件操作、零拷贝上传）+ WebDAV。

路径模型：MP 侧统一使用虚拟路径 ``/<账号名>/<子路径>...``，与 LitePan WebDAV
``/dav/<账号名>/...`` 一一对应（客户端内部自动加 /dav 前缀与 URL 编码）。
整理走 JSON 接口、按网盘文件 ID 操作；WebDAV 只用于下载和跨账号数据中转。
"""
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

try:
    from app.sdk.logging import logger
except ImportError:
    import logging

    logger = logging.getLogger(__name__)

# 终态：与 litepan upload 包的状态常量对齐。
UPLOAD_TERMINAL_STATUS = {"success", "failed", "canceled", "skipped"}
CHUNK_SIZE = 10 * 1024 * 1024
# 请求耗时全部记 DEBUG，超过该秒数的另记一条 INFO，
# 这样从 MoviePilot 日志就能直接看出一次整理慢在哪个请求上。
SLOW_REQUEST_SECONDS = 3.0


def log_timing(label: str, elapsed: float):
    if logger is None:
        return
    line = f"【LitePan】{label} 耗时 {elapsed:.2f}s"
    if elapsed >= SLOW_REQUEST_SECONDS:
        logger.info(line)
    else:
        logger.debug(line)


def _new_session() -> requests.Session:
    """发往 LitePan（局域网）的会话：不读环境里的代理和 netrc 设置。

    MoviePilot 容器里常为了访问外部站点配了 HTTP_PROXY，默认情况下 requests 会连发往
    LitePan 的请求也绕道代理，多一层转发，还可能出错。
    """
    session = requests.Session()
    session.trust_env = False
    return session


class SizedBody:
    """把流式响应包装成带长度的文件式请求体。

    直接拿 iter_content() 当请求体会走分块传输；有的代理不转发分块体，而 LitePan 对空
    请求体照样回 201（什么都不写），看上去传成功了，文件却不存在。带上 Content-Length
    就和普通文件上传走同一条路。
    """

    def __init__(self, response: requests.Response, length: int):
        self.length = length
        self._chunks = response.iter_content(chunk_size=CHUNK_SIZE)
        self._buffer = b""

    def __len__(self) -> int:
        return self.length

    def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self._buffer) < size:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break
        if size < 0:
            data, self._buffer = self._buffer, b""
        else:
            data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data


class LitePanClient:
    """LitePan WebDAV 客户端（Basic 认证 = LitePan 管理员账密）。"""

    def __init__(self, base_url: str, username: str, password: str, timeout: int = 120):
        self.base_url = base_url.rstrip("/")
        self.dav_base = self.base_url + "/dav"
        self.timeout = timeout
        self.session = _new_session()
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

    # ------------------------------------------------------------- 请求与耗时
    def _dav_request(self, method: str, path: str, **kwargs) -> requests.Response:
        started = time.monotonic()
        try:
            resp = self.session.request(method, self._url(path), **kwargs)
        except requests.RequestException:
            log_timing(f"{method} {self.normalize_path(path)} 网络错误", time.monotonic() - started)
            raise
        log_timing(f"{method} {self.normalize_path(path)} {resp.status_code}", time.monotonic() - started)
        return resp

    # ----------------------------------------------------------------- 写操作
    def mkcol(self, path: str) -> bool:
        try:
            resp = self._dav_request("MKCOL", path, timeout=self.timeout)
        except requests.RequestException:
            return False
        return resp.status_code in (200, 201, 405)  # 405=已存在（LitePan 对建目录出错也回 405）

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
        """把源流写到目标（跨账号中转用）。

        源响应给了 Content-Length 就按长度流式上传；没给（长度未知）才退回分块传输。
        """
        try:
            length = int(source.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length > 0:
            body = SizedBody(source, length)
        elif source.headers.get("Content-Length") is not None:
            return False  # 源文件是空的：LitePan 对空请求体也回 201，但什么都不会写
        else:
            body = source.iter_content(chunk_size=CHUNK_SIZE)
        try:
            resp = self.session.put(self._url(path), data=body, timeout=self.timeout)
        except requests.RequestException:
            return False
        # 真正写入时 LitePan 会带 ETag 返回；只回了状态码说明请求体没到。
        return resp.status_code in (200, 201, 204) and bool(resp.headers.get("ETag"))


class LitePanAdmin:
    """LitePan 管理 JSON API：cookie session 登录、按 ID 的文件操作、零拷贝上传。"""

    def __init__(self, base_url: str, username: str, password: str, timeout: int = 60):
        self.base_url = base_url.rstrip("/") + "/api"
        self.username = username
        self.password = password
        self.timeout = timeout
        self.session = _new_session()
        self._logged_in = False
        self._login_lock = threading.Lock()

    def close(self):
        self.session.close()

    def _login(self) -> bool:
        # 多个线程同时撞上会话过期时只登录一次，其余线程等它登完直接复用。
        with self._login_lock:
            if self._logged_in:
                return True
            return self._do_login()

    def _do_login(self) -> bool:
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
        started = time.monotonic()
        payload = self._request_with_relogin(method, url, **kwargs)
        log_timing(f"{method} /api{path} {'成功' if payload.get('success') else '失败'}",
                   time.monotonic() - started)
        return payload

    def _request_with_relogin(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
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

    # -------------------------------------------------------------- 文件操作
    # 都按网盘文件 ID 操作。parent_id 一并传给 LitePan，它据此失效对应目录的列表缓存。
    def create_folder(self, account_id: int, parent_id: str, name: str) -> Dict[str, Any]:
        """建目录；成功时 data 里带 folder_id。"""
        return self._request("POST", "/files/create-folder", json={
            "account_id": account_id, "parent_id": parent_id or "", "name": name})

    def delete_files(self, account_id: int, file_ids: List[str], parent_id: str = "") -> bool:
        payload = self._request("DELETE", "/files/delete", json={
            "account_id": account_id, "file_ids": file_ids, "parent_id": parent_id or ""})
        return bool(payload.get("success"))

    def move_files(self, account_id: int, file_ids: List[str], target_parent_id: str,
                   source_parent_id: str = "") -> bool:
        """网盘侧移动（不搬数据）。"""
        payload = self._request("POST", "/files/move", json={
            "account_id": account_id, "file_ids": file_ids,
            "target_parent_id": target_parent_id or "", "source_parent_id": source_parent_id or ""})
        return bool(payload.get("success"))

    def copy_files(self, account_id: int, file_ids: List[str], target_parent_id: str,
                   source_parent_id: str = "") -> bool:
        """网盘侧复制（服务端执行，不经客户端搬数据）。"""
        payload = self._request("POST", "/files/copy", json={
            "account_id": account_id, "file_ids": file_ids,
            "target_parent_id": target_parent_id or "", "source_parent_id": source_parent_id or ""})
        return bool(payload.get("success"))

    def rename_file(self, account_id: int, file_id: str, new_name: str, parent_id: str = "") -> bool:
        payload = self._request("PUT", "/files/rename", json={
            "account_id": account_id, "file_id": file_id, "new_name": new_name,
            "parent_id": parent_id or ""})
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
