"""
seed.py — 初始化演示数据

运行方式：
    python seed.py

功能：
    1. 初始化数据库（创建所有必要的表）
    2. 插入两个带有不同 webhook 配置的样本客户
    3. 可重复运行（INSERT OR REPLACE，幂等）

注意：
    customers 表属于业务系统，本脚本仅用于演示目的，模拟业务系统
    在客户注册流程中写入 webhook 配置的行为。
    生产环境中，此类数据由业务系统的注册逻辑维护，通知系统只读。
"""

import logging

from notification.database import get_connection, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# 样本客户数据
#
# webhook_url 使用 https://httpbin.org/post：
#   - 接受任意 POST 请求并返回请求详情（status 200）
#   - 无需注册，适合本地测试
#
# webhook_body_tpl 使用 Jinja2 模板语法：
#   - {{ title }}         : 通知标题（字符串）
#   - {{ content }}       : 通知正文（dict）
#   - {{ content.key }}   : 访问正文中的具体字段
#   - {{ content | tojson }}: 将 content dict 序列化为 JSON 字符串
#   - {{ customer_id }}   : 客户 ID
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_CUSTOMERS = [
    {
        "id": "cust_ads_001",
        "name": "Acme 广告平台",
        "email": "webhook@acme-ads.example.com",
        #
        # 场景：用户通过广告引流注册后，通知广告平台上报转化事件
        #
        "webhook_url": "https://httpbin.org/post",
        "webhook_method": "POST",
        "webhook_headers_tpl": (
            '{"Content-Type": "application/json", '
            '"X-Source": "notification-service", '
            '"X-Customer-Id": "{{ customer_id }}"}'
        ),
        "webhook_body_tpl": (
            "{"
            '"event_type": "{{ title }}", '
            '"user_id": "{{ content.user_id }}", '
            '"registered_at": "{{ content.registered_at }}", '
            '"channel": "{{ content.channel | default(\'organic\') }}"'
            "}"
        ),
        "webhook_timeout_s": 30,
        "webhook_max_retries": 10,
    },
    {
        "id": "cust_crm_002",
        "name": "Acme CRM 系统",
        "email": "webhook@acme-crm.example.com",
        #
        # 场景：用户订阅付款成功后，通知 CRM 系统更新 Contact 状态
        #
        "webhook_url": "https://httpbin.org/post",
        "webhook_method": "POST",
        "webhook_headers_tpl": (
            '{"Content-Type": "application/json", "X-Event-Source": "billing"}'
        ),
        "webhook_body_tpl": (
            "{"
            '"action": "{{ title }}", '
            '"contact": {'
            '"email": "{{ content.email }}", '
            '"plan": "{{ content.plan }}"'
            "}, "
            '"amount": {{ content.amount | default(0) }}, '
            '"currency": "{{ content.currency | default(\'USD\') }}", '
            '"metadata": {{ content | tojson }}'
            "}"
        ),
        "webhook_timeout_s": 30,
        "webhook_max_retries": 5,
    },
    {
        "id": "cust_inventory_003",
        "name": "库存管理系统",
        "email": "webhook@inventory.example.com",
        #
        # 场景：用户购买商品后，通知库存系统扣减库存
        # webhook_url 为空，表示该客户暂未配置 webhook（通知系统会返回 400）
        #
        "webhook_url": None,
        "webhook_method": "POST",
        "webhook_headers_tpl": '{"Content-Type": "application/json"}',
        "webhook_body_tpl": None,
        "webhook_timeout_s": 30,
        "webhook_max_retries": 10,
    },
]

INSERT_CUSTOMER_SQL = """
INSERT OR REPLACE INTO customers (
    id, name, email,
    webhook_url, webhook_method,
    webhook_headers_tpl, webhook_body_tpl,
    webhook_timeout_s, webhook_max_retries
) VALUES (
    :id, :name, :email,
    :webhook_url, :webhook_method,
    :webhook_headers_tpl, :webhook_body_tpl,
    :webhook_timeout_s, :webhook_max_retries
)
"""


def seed() -> None:
    logger.info("=== Seeding database ===")

    # 初始化数据库（幂等）
    init_db()

    with get_connection() as conn:
        for customer in SAMPLE_CUSTOMERS:
            conn.execute(INSERT_CUSTOMER_SQL, customer)
            webhook_status = (
                f"webhook → {customer['webhook_url']}"
                if customer["webhook_url"]
                else "no webhook configured"
            )
            logger.info(
                "  ✓ Customer %-25s  %s  (%s)",
                f"[{customer['id']}]",
                customer["name"],
                webhook_status,
            )

    logger.info("=== Seeding complete ===")
    logger.info("")
    logger.info("You can now test the API with:")
    logger.info("")
    logger.info("  # 提交广告转化通知（应返回 202）")
    logger.info(
        "  curl -s -X POST http://localhost:8000/api/v1/notifications \\"
    )
    logger.info("    -H 'Content-Type: application/json' \\")
    logger.info(
        "    -d '{"
        '"customer_id": "cust_ads_001", '
        '"title": "user_registered", '
        '"event_type": "ad_conversion", '
        '"content": {"user_id": "u_999", "registered_at": "2024-01-01T10:00:00Z", "channel": "google_ads"}'
        "}' | python -m json.tool"
    )
    logger.info("")
    logger.info("  # 提交 CRM 订阅付款通知（应返回 202）")
    logger.info(
        "  curl -s -X POST http://localhost:8000/api/v1/notifications \\"
    )
    logger.info("    -H 'Content-Type: application/json' \\")
    logger.info(
        "    -d '{"
        '"customer_id": "cust_crm_002", '
        '"title": "subscription_paid", '
        '"event_type": "billing", '
        '"content": {"email": "user@example.com", "plan": "pro", "amount": 99.00, "currency": "USD"}'
        "}' | python -m json.tool"
    )
    logger.info("")
    logger.info("  # 提交库存通知（应返回 400，因为该客户未配置 webhook）")
    logger.info(
        "  curl -s -X POST http://localhost:8000/api/v1/notifications \\"
    )
    logger.info("    -H 'Content-Type: application/json' \\")
    logger.info(
        "    -d '{"
        '"customer_id": "cust_inventory_003", '
        '"title": "order_placed", '
        '"content": {"sku": "PROD-001", "quantity": 2}'
        "}' | python -m json.tool"
    )


if __name__ == "__main__":
    seed()
