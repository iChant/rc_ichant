import os
import tempfile

import pytest


@pytest.fixture
def temp_db():
    """
    为每个测试提供独立的临时 SQLite 数据库文件。

    测试结束后自动清理 .db、.db-wal、.db-shm 文件，
    确保测试之间完全隔离。
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    for ext in ("", "-wal", "-shm"):
        p = path + ext
        if os.path.exists(p):
            try:
                os.unlink(p)
            except OSError:
                pass
