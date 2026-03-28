import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    # 数据库文件路径（MVP: SQLite）
    db_path: str = field(
        default_factory=lambda: os.getenv("DB_PATH", "notification.db")
    )
    # Dispatcher 轮询间隔（秒）
    poll_interval_seconds: float = field(
        default_factory=lambda: float(os.getenv("POLL_INTERVAL", "5"))
    )
    # 将 processing 状态任务视为滞留的超时阈值（分钟）
    stale_timeout_minutes: int = field(
        default_factory=lambda: int(os.getenv("STALE_TIMEOUT_MINUTES", "30"))
    )
    # 每次轮询获取的最大任务数
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "10"))
    )
    # HTTP 投递默认超时（秒）
    default_timeout_seconds: int = field(
        default_factory=lambda: int(os.getenv("DEFAULT_TIMEOUT", "30"))
    )
    # 默认最大重试次数
    default_max_retries: int = field(
        default_factory=lambda: int(os.getenv("DEFAULT_MAX_RETRIES", "10"))
    )


settings = Settings()
