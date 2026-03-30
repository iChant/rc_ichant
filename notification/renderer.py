import json
import logging

from jinja2 import Environment, StrictUndefined, TemplateError

logger = logging.getLogger(__name__)


def _tojson_filter(value) -> str:
    """
    Jinja2 过滤器：将 Python 对象序列化为 JSON 字面量字符串。

    用于在 body 模板中内嵌 JSON 值，例如：
        {{ content | tojson }}          →  {"key": "value"}
        {{ content.amount | tojson }}   →  99.0
        {{ True | tojson }}             →  true
    """
    return json.dumps(value, ensure_ascii=False)


# 使用 StrictUndefined：访问模板中未定义的变量时抛出异常，
# 而非静默返回空字符串，便于尽早发现模板配置错误。
_env = Environment(undefined=StrictUndefined)
_env.filters["tojson"] = _tojson_filter


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
                      支持 {{ content | tojson }} 将整个 dict 序列化为 JSON 字符串
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
            f"Headers template must render to a JSON object, "
            f"got {type(parsed).__name__}"
        )

    return rendered
