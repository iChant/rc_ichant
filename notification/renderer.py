import json
import logging

from jinja2 import Environment, TemplateError, Undefined

logger = logging.getLogger(__name__)

# 使用严格模式：访问未定义变量时抛出异常而非静默返回空字符串
_env = Environment(undefined=Undefined)


def build_template_context(
    customer_id: str,
    title: str | None,
    content: dict | None,
) -> dict:
    """
    构建模板渲染上下文。

    可用变量：
      - customer_id : 客户 ID（字符串）
      - title       : 通知标题（字符串，未传时为空字符串）
      - content     : 通知正文（dict，未传时为空 dict）
                      支持 {{ content.key }} 或 {{ content['key'] }} 两种访问方式
    """
    return {
        "customer_id": customer_id,
        "title": title or "",
        "content": content or {},
    }


def render_body(body_template: str, context: dict) -> str:
    """
    渲染 Body 模板，返回最终请求体字符串。

    示例模板：
        {"event": "{{ title }}", "data": {{ content | tojson }}}
    """
    if not body_template or not body_template.strip():
        return ""
    try:
        return _env.from_string(body_template).render(**context)
    except TemplateError as exc:
        raise ValueError(f"Body template rendering failed: {exc}") from exc


def render_headers(headers_template: str, context: dict) -> str:
    """
    渲染 Headers 模板，返回 JSON 字符串。
    渲染结果必须是合法的 JSON 对象（dict），否则抛出 ValueError。

    示例模板：
        {"Content-Type": "application/json", "X-Source": "{{ customer_id }}"}
    """
    if not headers_template or not headers_template.strip():
        return "{}"
    try:
        rendered = _env.from_string(headers_template).render(**context)
    except TemplateError as exc:
        raise ValueError(f"Headers template rendering failed: {exc}") from exc

    try:
        parsed = json.loads(rendered)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Headers template did not render to valid JSON: {exc}\n"
            f"Rendered output: {rendered!r}"
        ) from exc

    if not isinstance(parsed, dict):
        raise ValueError(
            f"Headers template must render to a JSON object, got {type(parsed).__name__}"
        )

    return rendered
