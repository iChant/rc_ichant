import logging

import uvicorn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)


if __name__ == "__main__":
    from notification.api import app

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_config=None,  # 使用上方 basicConfig，不覆盖 uvicorn 默认日志配置
    )
