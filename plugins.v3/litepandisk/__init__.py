"""MoviePilot 存储插件：LitePan 聚合网盘（零拷贝上传）。

向 MoviePilot 注册一个存储「LitePan」：根目录下每个子目录是一个 LitePan 网盘账号。
浏览/整理/删除/改名经 LitePan 的接口按网盘文件 ID 执行，插件自己缓存目录 ID，
整理同盘文件和直接用网盘插件一样快；上传走 LitePan「按路径上传」零拷贝通道
（只传路径，LitePan 服务器直读源文件，不在其磁盘落盘）。
账号只在 LitePan 登录一次，MoviePilot 仅保存 LitePan 管理员账密。
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app import schemas
from app.schemas import FileItem, StorageOperSelectionEventData

try:
    from app.schemas import StorageQueryError
except ImportError:  # 兼容宿主 schema 布局差异
    from app.schemas.exception import StorageQueryError

from app.schemas.types import ChainEventType
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.services import StorageHelper

try:
    from app.sdk.plugin import _PluginBase
except ImportError:  # 兼容旧版基类布局
    from app.plugins import _PluginBase

from .litepan_api import LitePanApi
from .litepan_client import LitePanAdmin, LitePanClient


def _parse_mappings(text: str) -> List[Tuple[str, str]]:
    """解析路径翻译表：一条 ``MP前缀 => LitePan前缀``（也接受单个 =），
    多条之间用换行、逗号或分号分隔。"""
    mappings: List[Tuple[str, str]] = []
    text = (text or "").replace(",", "\n").replace("；", "\n").replace(";", "\n")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        sep = "=>" if "=>" in line else ("=" if "=" in line else None)
        if sep is None:
            logger.warning(f"【LitePan】忽略无法解析的路径翻译行: {line}")
            continue
        left, _, right = line.partition(sep)
        mp_prefix = left.strip().rstrip("/")
        server_prefix = right.strip().rstrip("/")
        if not mp_prefix.startswith("/") or not server_prefix.startswith("/"):
            logger.warning(f"【LitePan】路径翻译两侧必须都是绝对路径，忽略: {line}")
            continue
        mappings.append((mp_prefix, server_prefix))
    return mappings


class LitePanDisk(_PluginBase):
    # 插件名称
    plugin_name = "LitePan云盘"
    # 插件描述
    plugin_desc = "使存储支持 LitePan 聚合网盘（零拷贝上传，账号只在 LitePan 登录一次）。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Plugins/main/icons/cloud.png"
    # 插件版本
    plugin_version = "1.0.7"
    # 插件作者
    plugin_author = "famei"
    # 作者主页
    author_url = ""
    # 插件配置项 ID 前缀
    plugin_config_prefix = "litepandisk_"
    # 加载顺序
    plugin_order = 99
    # 可使用的用户级别
    auth_level = 1

    _enabled = False
    _disk_name = "LitePan"
    _client: Optional[LitePanClient] = None
    _admin: Optional[LitePanAdmin] = None
    _api: Optional[LitePanApi] = None

    _base_url = ""
    _username = ""
    _password = ""
    _upload_timeout = 120  # 分钟
    _path_mappings_text = ""

    def __init__(self):
        super().__init__()

    def init_plugin(self, config: dict = None):
        if not config:
            return

        storage_helper = StorageHelper()
        storages = storage_helper.get_storagies()
        if not any(
            s.type == self._disk_name and s.name == self._disk_name for s in storages
        ):
            storage_helper.add_storage(
                storage=self._disk_name,
                name=self._disk_name,
                conf={},
            )

        self._enabled = bool(config.get("enabled"))
        self._base_url = (config.get("base_url") or "").strip().rstrip("/")
        self._username = (config.get("username") or "").strip()
        self._password = (config.get("password") or "").strip()
        try:
            self._upload_timeout = max(int(config.get("upload_timeout") or 120), 1)
        except (TypeError, ValueError):
            self._upload_timeout = 120
        self._path_mappings_text = (config.get("path_mappings") or "").strip()

        self.stop_service()
        if not self._enabled or not (self._base_url and self._username and self._password):
            self._api = None
            return

        mappings = _parse_mappings(self._path_mappings_text)
        self._client = LitePanClient(self._base_url, self._username, self._password)
        self._admin = LitePanAdmin(self._base_url, self._username, self._password)
        self._api = LitePanApi(
            client=self._client,
            admin=self._admin,
            disk_name=self._disk_name,
            upload_timeout=self._upload_timeout * 60,
            path_mappings=mappings,
        )
        logger.info(
            "【LitePan】初始化完成: %s（路径翻译 %d 条）",
            self._base_url,
            len(mappings),
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    @staticmethod
    def get_api() -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upload_timeout",
                                            "label": "上传等待超时（分钟）",
                                            "placeholder": "120",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "base_url",
                                            "label": "LitePan 服务地址",
                                            "placeholder": "http://192.168.1.10:5211",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {"model": "username", "label": "管理员用户名"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "password",
                                            "label": "管理员密码",
                                            "type": "password",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "path_mappings",
                                            "label": "路径翻译（可选，多条用逗号分隔）",
                                            "placeholder": "/media/downloads => /vol2/Downloads, /data/movies => /vol2/Movies",
                                            "hint": "MoviePilot 本地路径前缀 => LitePan 容器内路径前缀；"
                                                    "不填表示两边挂载路径一致（零拷贝直传）",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "base_url": "",
            "username": "",
            "password": "",
            "upload_timeout": 120,
            "path_mappings": "",
        }

    def get_page(self) -> List[dict]:
        return []

    def get_module(self) -> Dict[str, Any]:
        return {
            "list_files": self.list_files,
            "any_files": self.any_files,
            "download_file": self.download_file,
            "upload_file": self.upload_file,
            "delete_file": self.delete_file,
            "rename_file": self.rename_file,
            "create_folder": self.create_folder,
            "get_folder": self.get_folder,
            "get_file_item": self.get_file_item,
            "get_parent_item": self.get_parent_item,
            "snapshot_storage": self.snapshot_storage,
            "storage_manage": self.storage_manage,
        }

    @eventmanager.register(ChainEventType.StorageOperSelection)
    def storage_oper_selection(self, event: Event):
        if not self._enabled:
            return
        event_data: StorageOperSelectionEventData = event.event_data
        if event_data.storage == self._disk_name:
            event_data.storage_oper = self._api  # noqa: SLF001

    def list_files(
        self, fileitem: schemas.FileItem, recursion: bool = False
    ) -> Optional[List[schemas.FileItem]]:
        if fileitem.storage != self._disk_name:
            return None
        # 未初始化时返回 None（未接管）：[] 会被宿主当成「目录确实为空」的权威结论。
        if not self._api:
            return None

        result: List[schemas.FileItem] = []

        def _walk(_item: FileItem, _recursion: bool = False):
            items = self._api.list(_item)
            if not items:
                return
            if _recursion:
                for sub_item in items:
                    if sub_item.type == "dir":
                        _walk(sub_item, _recursion)
                    else:
                        result.append(sub_item)
            else:
                result.extend(items)

        _walk(fileitem, recursion)
        return result

    def any_files(
        self, fileitem: schemas.FileItem, extensions: list = None
    ) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        # 同上：False 对宿主是「确认没有媒体文件」，未初始化时必须返回 None。
        if not self._api:
            return None

        def _any(_item: FileItem) -> bool:
            items = self._api.list(_item)
            if not items:
                return False
            if not extensions:
                return True
            for sub_item in items:
                if (
                    sub_item.type == "file"
                    and sub_item.extension
                    and f".{sub_item.extension.lower()}" in extensions
                ):
                    return True
                if sub_item.type == "dir" and _any(sub_item):
                    return True
            return False

        return _any(fileitem)

    def create_folder(
        self, fileitem: schemas.FileItem, name: str
    ) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.create_folder(fileitem=fileitem, name=name)

    def get_folder(self, storage: str, path: Path) -> Optional[schemas.FileItem]:
        if storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.get_folder(path)

    def download_file(
        self, fileitem: schemas.FileItem, path: Path = None
    ) -> Optional[Path]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.download(fileitem, path)

    def upload_file(
        self, fileitem: schemas.FileItem, path: Path, new_name: Optional[str] = None
    ) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.upload(fileitem, path, new_name)

    def delete_file(self, fileitem: schemas.FileItem) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.delete(fileitem)

    def rename_file(self, fileitem: schemas.FileItem, name: str) -> Optional[bool]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.rename(fileitem, name)

    def get_file_item(self, storage: str, path: Path) -> Optional[schemas.FileItem]:
        # V3 的 get_file_item 同时服务宽松与严格入口：StorageChain.get_file_item_strict
        # 依赖 provider 在「无法确认」时抛 StorageQueryError，静默 None 会被误判为
        # 「文件不存在」而绕过覆盖保护。
        if storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.get_item_strict(path)

    def get_parent_item(self, fileitem: schemas.FileItem) -> Optional[schemas.FileItem]:
        if fileitem.storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.get_parent(fileitem)

    def snapshot_storage(
        self,
        storage: str,
        path: Path,
        last_snapshot_time: float = None,
        max_depth: int = 5,
        previous_snapshot: Optional[Dict[str, Dict]] = None,
    ) -> Optional[Dict[str, Dict]]:
        if storage != self._disk_name:
            return None
        if not self._api:
            return {}

        files_info: Dict[str, Dict] = {}

        def _snapshot(_fileitem: schemas.FileItem, _current_depth: int = 0):
            if _fileitem.type == "dir":
                if _current_depth >= max_depth:
                    return
                for _sub_file in self._api.list(_fileitem) or []:
                    _snapshot(_sub_file, _current_depth + 1)
            else:
                # 键与宿主 StorageBase.snapshot 保持一致（含 fileid），缺了会让
                # 「同大小同时间戳的替换文件」识别不出来。
                files_info[_fileitem.path] = {
                    "size": _fileitem.size or 0,
                    "modify_time": getattr(_fileitem, "modify_time", 0) or 0,
                    "fileid": getattr(_fileitem, "fileid", None),
                    "type": _fileitem.type,
                }

        try:
            root_item = self._api.get_item_strict(path)
        except StorageQueryError as err:
            logger.warning(f"【LitePan】快照读取失败，保留上次基线: {path} - {err}")
            return None
        if not root_item:
            return {}
        try:
            _snapshot(root_item)
        except StorageQueryError as err:
            logger.warning(f"【LitePan】快照读取失败，保留上次基线: {path} - {err}")
            return None
        return files_info

    def storage_usage(self, storage: str) -> Optional[schemas.StorageUsage]:
        if storage != self._disk_name:
            return None
        if not self._api:
            return None
        return self._api.usage()

    def storage_manage(self, storage: str, action: str, **params) -> Optional[Dict[str, Any]]:
        # 宿主以 run_module("storage_manage") 分派存储管理动作：本存储的动作在此应答，
        # 其余一律返回 None 交回宿主，避免抢占其它存储。
        if storage != self._disk_name:
            return None
        if action == "usage":
            usage = self.storage_usage(storage) or schemas.StorageUsage()
            return {"success": True, "data": usage.model_dump()}
        if action == "support_transtype":
            return {"success": True, "data": {"transtype": {"move": "移动", "copy": "复制"}}}
        return None

    def stop_service(self):
        if self._client:
            self._client.close()
        if self._admin:
            self._admin.close()
        self._client = None
        self._admin = None
        self._api = None
