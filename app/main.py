from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from collections import deque
from pathlib import Path
from threading import RLock
from typing import Any, AsyncIterator

import httpx
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, Field, field_validator


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "providers.yaml"
USAGE_LOG_PATH = ROOT / "logs" / "usage.jsonl"

AUTH_ERROR = "Missing or invalid local Authorization bearer token."
RETRYABLE_STATUS_CODES = {400, 401, 402, 403, 404, 408, 409, 429, 500, 502, 503, 504}
usage_events: deque[dict[str, Any]] = deque(maxlen=500)
config_lock = RLock()


class UpstreamStreamError(Exception):
    pass


class ProviderConfig(BaseModel):
    name: str
    base_url: str
    api_key: str
    enabled: bool = True
    priority: int = 0

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        return value.rstrip("/")


class AppConfig(BaseModel):
    local_api_key: str = "sk-local-router-dev"
    timeout_seconds: float = 60
    connect_timeout_seconds: float = 10
    stream_first_token_timeout_seconds: float = 18
    max_route_attempts: int = 6
    max_failures: int = 3
    circuit_breaker_seconds: int = 600
    providers: list[ProviderConfig] = Field(default_factory=list)


class ProviderInput(BaseModel):
    name: str | None = None
    base_url: str
    api_key: str
    enabled: bool = True
    priority: int = 0

    @field_validator("base_url")
    @classmethod
    def normalize_base_url(cls, value: str) -> str:
        return value.rstrip("/")

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-")
        return normalized or None


class ProviderState(BaseModel):
    name: str
    base_url: str
    enabled: bool
    consecutive_failures: int = 0
    success_count: int = 0
    failure_count: int = 0
    circuit_open_until: float = 0
    last_error: str | None = None
    last_status_code: int | None = None
    last_success_at: float | None = None
    last_failure_at: float | None = None

    def is_circuit_open(self) -> bool:
        return self.circuit_open_until > time.time()


def load_config() -> AppConfig:
    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file) or {}
    config = AppConfig.model_validate(raw)
    if not config.providers:
        raise RuntimeError("No providers configured.")
    return config


def config_to_yaml_data(config_value: AppConfig) -> dict[str, Any]:
    return {
        "local_api_key": config_value.local_api_key,
        "timeout_seconds": config_value.timeout_seconds,
        "connect_timeout_seconds": config_value.connect_timeout_seconds,
        "stream_first_token_timeout_seconds": config_value.stream_first_token_timeout_seconds,
        "max_route_attempts": config_value.max_route_attempts,
        "max_failures": config_value.max_failures,
        "circuit_breaker_seconds": config_value.circuit_breaker_seconds,
        "providers": [provider.model_dump() for provider in config_value.providers],
    }


def save_config(config_value: AppConfig) -> None:
    CONFIG_PATH.write_text(
        yaml.safe_dump(config_to_yaml_data(config_value), allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def slug_from_base_url(base_url: str) -> str:
    value = re.sub(r"^https?://", "", base_url.rstrip("/"))
    value = re.sub(r"/v1$", "", value)
    value = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-")
    return value or "provider"


def unique_provider_name(base_name: str, skip_name: str | None = None) -> str:
    existing = {provider.name for provider in config.providers if provider.name != skip_name}
    if base_name not in existing:
        return base_name
    index = 2
    while f"{base_name}-{index}" in existing:
        index += 1
    return f"{base_name}-{index}"


def provider_public(provider: ProviderConfig) -> dict[str, Any]:
    return {
        "name": provider.name,
        "base_url": provider.base_url,
        "api_key": provider.api_key,
        "enabled": provider.enabled,
        "priority": provider.priority,
    }


def rebuild_provider_states() -> None:
    current = set()
    for provider in config.providers:
        current.add(provider.name)
        state = provider_states.get(provider.name)
        if state is None:
            provider_states[provider.name] = ProviderState(
                name=provider.name,
                base_url=provider.base_url,
                enabled=provider.enabled,
            )
        else:
            state.base_url = provider.base_url
            state.enabled = provider.enabled
    for name in list(provider_states):
        if name not in current:
            del provider_states[name]


def find_provider_index(name: str) -> int:
    for index, provider in enumerate(config.providers):
        if provider.name == name:
            return index
    raise HTTPException(status_code=404, detail="Provider not found.")


config = load_config()
provider_states: dict[str, ProviderState] = {
    provider.name: ProviderState(
        name=provider.name,
        base_url=provider.base_url,
        enabled=provider.enabled,
    )
    for provider in config.providers
}

app = FastAPI(title="Local Key Router", version="0.1.0")


def require_local_auth(authorization: str | None = Header(default=None)) -> None:
    expected = f"Bearer {config.local_api_key}"
    if authorization != expected:
        raise HTTPException(status_code=401, detail=AUTH_ERROR)


def now_iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(timestamp))


def new_request_id() -> str:
    return "req_" + uuid.uuid4().hex[:16]


def append_usage_event(event: dict[str, Any]) -> None:
    event = {"timestamp": now_iso(time.time()), **event}
    usage_events.appendleft(event)
    USAGE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with USAGE_LOG_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")


def read_recent_usage_events(limit: int) -> list[dict[str, Any]]:
    if not USAGE_LOG_PATH.exists():
        return list(usage_events)[:limit]
    lines = USAGE_LOG_PATH.read_text(encoding="utf-8").splitlines()
    events: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
        if len(events) >= limit:
            break
    return events


def usage_from_response(data: dict[str, Any]) -> dict[str, Any] | None:
    usage = data.get("usage")
    return usage if isinstance(usage, dict) else None


def response_usage_from_chat_usage(usage: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(usage, dict):
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
        }
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0
    total_tokens = usage.get("total_tokens", input_tokens + output_tokens) or 0
    normalized = {
        **usage,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
    }
    input_details = usage.get("input_tokens_details")
    if not isinstance(input_details, dict):
        prompt_details = usage.get("prompt_tokens_details")
        input_details = prompt_details if isinstance(prompt_details, dict) else {}
    output_details = usage.get("output_tokens_details")
    if not isinstance(output_details, dict):
        completion_details = usage.get("completion_tokens_details")
        output_details = completion_details if isinstance(completion_details, dict) else {}
    normalized["input_tokens_details"] = {
        "cached_tokens": input_details.get("cached_tokens", 0) or 0,
    }
    normalized["output_tokens_details"] = {
        "reasoning_tokens": output_details.get("reasoning_tokens", usage.get("reasoning_tokens", 0)) or 0,
    }
    return normalized


def model_from_payload(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    model = payload.get("model")
    return str(model) if model is not None else None


def provider_status(provider: ProviderConfig) -> dict[str, Any]:
    state = provider_states[provider.name]
    circuit_remaining = max(0, int(state.circuit_open_until - time.time()))
    return {
        "name": state.name,
        "base_url": state.base_url,
        "enabled": state.enabled,
        "healthy": state.enabled and circuit_remaining == 0,
        "circuit_open": circuit_remaining > 0,
        "circuit_remaining_seconds": circuit_remaining,
        "consecutive_failures": state.consecutive_failures,
        "success_count": state.success_count,
        "failure_count": state.failure_count,
        "last_status_code": state.last_status_code,
        "last_error": state.last_error,
        "last_success_at": now_iso(state.last_success_at),
        "last_failure_at": now_iso(state.last_failure_at),
    }


def available_providers() -> list[ProviderConfig]:
    candidates: list[ProviderConfig] = []
    for index, provider in enumerate(config.providers):
        state = provider_states[provider.name]
        if not provider.enabled or not state.enabled or state.is_circuit_open():
            continue
        candidates.append((index, provider))
    candidates.sort(key=lambda item: (-item[1].priority, item[0]))
    return [provider for _, provider in candidates]


def mark_success(provider: ProviderConfig) -> None:
    state = provider_states[provider.name]
    state.consecutive_failures = 0
    state.success_count += 1
    state.last_error = None
    state.last_status_code = None
    state.last_success_at = time.time()
    state.circuit_open_until = 0


def mark_failure(provider: ProviderConfig, message: str, status_code: int | None = None) -> None:
    state = provider_states[provider.name]
    state.consecutive_failures += 1
    state.failure_count += 1
    state.last_error = message[:600]
    state.last_status_code = status_code
    state.last_failure_at = time.time()
    if state.consecutive_failures >= config.max_failures:
        state.circuit_open_until = time.time() + config.circuit_breaker_seconds


def timeout() -> httpx.Timeout:
    return httpx.Timeout(
        timeout=config.timeout_seconds,
        connect=config.connect_timeout_seconds,
    )


def auth_headers(provider: ProviderConfig) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {provider.api_key}",
        "Content-Type": "application/json",
    }


def key_hint(api_key: str) -> str:
    if len(api_key) <= 14:
        return api_key
    return f"{api_key[:7]}...{api_key[-6:]}"


def provider_route_label(provider: ProviderConfig) -> str:
    return f"{provider.base_url} · {key_hint(provider.api_key)}"


def upstream_error_message(response: httpx.Response) -> str:
    body = response.text.strip()
    if len(body) > 400:
        body = body[:400] + "..."
    return f"HTTP {response.status_code}: {body or response.reason_phrase}"


def validate_chat_response(data: dict[str, Any], provider: ProviderConfig) -> None:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError(f"{provider.name} returned HTTP 200 without choices[]")
    first = choices[0]
    if not isinstance(first, dict):
        raise ValueError(f"{provider.name} returned invalid choice shape")
    message = first.get("message")
    delta = first.get("delta")
    if not isinstance(message, dict) and not isinstance(delta, dict):
        raise ValueError(f"{provider.name} returned choice without message/delta")
    payload = message if isinstance(message, dict) else delta
    has_content = payload.get("content") not in (None, "")
    has_tool_calls = bool(payload.get("tool_calls"))
    has_function_call = bool(payload.get("function_call"))
    if not has_content and not has_tool_calls and not has_function_call:
        raise ValueError(f"{provider.name} returned empty message content")


async def request_json(
    provider: ProviderConfig,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{provider.base_url}{path}"
    async with httpx.AsyncClient(timeout=timeout()) as client:
        response = await client.request(method, url, headers=auth_headers(provider), json=payload)
    if response.status_code >= 400:
        raise httpx.HTTPStatusError(upstream_error_message(response), request=response.request, response=response)
    if not response.content:
        return {}
    return response.json()


async def try_non_stream_chat(payload: dict[str, Any], endpoint: str, request_id: str) -> JSONResponse:
    started_at = time.time()
    errors: list[dict[str, Any]] = []
    for provider in available_providers()[: config.max_route_attempts]:
        try:
            data = await request_json(provider, "POST", "/chat/completions", payload)
            validate_chat_response(data, provider)
            mark_success(provider)
            append_usage_event(
                {
                    "request_id": request_id,
                    "endpoint": endpoint,
                    "provider": provider.name,
                    "base_url": provider.base_url,
                    "key_hint": key_hint(provider.api_key),
                    "route": provider_route_label(provider),
                    "model": model_from_payload(payload),
                    "status": "success",
                    "stream": False,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "usage": usage_from_response(data),
                    "error": None,
                    "attempt_errors": errors,
                }
            )
            return JSONResponse(data)
        except httpx.HTTPStatusError as exc:
            mark_failure(provider, str(exc), exc.response.status_code)
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": str(exc)})
        except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
            mark_failure(provider, type(exc).__name__ + ": " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": type(exc).__name__ + ": " + str(exc)})
    append_usage_event(
        {
            "request_id": request_id,
            "endpoint": endpoint,
            "provider": None,
            "base_url": None,
            "key_hint": None,
            "route": None,
            "model": model_from_payload(payload),
            "status": "failed",
            "stream": False,
            "duration_ms": int((time.time() - started_at) * 1000),
            "usage": None,
            "error": "All upstream providers failed.",
            "attempt_errors": errors,
        }
    )
    return all_failed(errors)


def all_failed(errors: list[dict[str, Any]]) -> JSONResponse:
    if not errors:
        errors = [{"provider": None, "error": "No enabled provider is currently available."}]
    return JSONResponse(
        status_code=502,
        content={
            "error": {
                "message": "All upstream providers failed.",
                "type": "upstream_unavailable",
                "details": errors,
            }
        },
    )


async def stream_from_provider(provider: ProviderConfig, payload: dict[str, Any]) -> AsyncIterator[bytes]:
    url = f"{provider.base_url}/chat/completions"
    async with httpx.AsyncClient(timeout=timeout()) as client:
        async with client.stream("POST", url, headers=auth_headers(provider), json=payload) as response:
            if response.status_code >= 400:
                body = await response.aread()
                text = body.decode("utf-8", errors="replace")
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}: {text[:400]}",
                    request=response.request,
                    response=response,
                )
            first_buffer = b""
            byte_stream = response.aiter_bytes()
            async for chunk in byte_stream:
                first_buffer += chunk
                text = first_buffer.decode("utf-8", errors="replace")
                if "\n\n" not in text and len(first_buffer) < 4096:
                    continue
                first_event = first_sse_json_event(text)
                if first_event and first_event.get("error"):
                    raise UpstreamStreamError(json.dumps(first_event["error"], ensure_ascii=False))
                if first_event and first_event.get("choices") is not None:
                    choices = first_event.get("choices")
                    if not isinstance(choices, list) or not choices:
                        raise UpstreamStreamError(f"{provider.name} returned stream chunk without choices[]")
                mark_success(provider)
                yield first_buffer
                break
            async for chunk in byte_stream:
                yield chunk


def first_sse_json_event(text: str) -> dict[str, Any] | None:
    for event in text.split("\n\n"):
        data_lines = []
        for line in event.splitlines():
            if line.startswith("data:"):
                data = line.removeprefix("data:").strip()
                if data == "[DONE]":
                    return None
                data_lines.append(data)
        if not data_lines:
            continue
        try:
            value = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            return None
        if isinstance(value, dict):
            return value
    return None


async def stream_error_event(message: str, details: list[dict[str, Any]]) -> AsyncIterator[bytes]:
    payload = {
        "error": {
            "message": message,
            "type": "upstream_unavailable",
            "details": details,
        }
    }
    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")
    yield b"data: [DONE]\n\n"


async def routed_stream(payload: dict[str, Any], request_id: str) -> AsyncIterator[bytes]:
    started_at = time.time()
    errors: list[dict[str, Any]] = []
    providers = available_providers()[: config.max_route_attempts]
    for provider in providers:
        try:
            async for chunk in stream_from_provider(provider, payload):
                yield chunk
            append_usage_event(
                {
                    "request_id": request_id,
                    "endpoint": "/v1/chat/completions",
                    "provider": provider.name,
                    "base_url": provider.base_url,
                    "key_hint": key_hint(provider.api_key),
                    "route": provider_route_label(provider),
                    "model": model_from_payload(payload),
                    "status": "success",
                    "stream": True,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "usage": None,
                    "error": None,
                    "attempt_errors": errors,
                }
            )
            return
        except httpx.HTTPStatusError as exc:
            mark_failure(provider, str(exc), exc.response.status_code)
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": str(exc)})
        except UpstreamStreamError as exc:
            mark_failure(provider, "stream_error: " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": "stream_error: " + str(exc)})
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            mark_failure(provider, type(exc).__name__ + ": " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": type(exc).__name__ + ": " + str(exc)})
    if not providers:
        errors.append({"provider": None, "error": "No enabled provider is currently available."})
    append_usage_event(
        {
            "request_id": request_id,
            "endpoint": "/v1/chat/completions",
            "provider": None,
            "base_url": None,
            "key_hint": None,
            "route": None,
            "model": model_from_payload(payload),
            "status": "failed",
            "stream": True,
            "duration_ms": int((time.time() - started_at) * 1000),
            "usage": None,
            "error": "All upstream providers failed.",
            "attempt_errors": errors,
        }
    )
    async for chunk in stream_error_event("All upstream providers failed.", errors):
        yield chunk


def content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text") or part.get("content") or part.get("output_text")
                if text is not None:
                    parts.append(str(text))
            else:
                parts.append(str(part))
        return " ".join(parts)
    if content is None:
        return ""
    return str(content)


def normalize_response_input(input_value: Any) -> list[dict[str, Any]]:
    if isinstance(input_value, str):
        return [{"role": "user", "content": input_value}]
    if isinstance(input_value, list):
        messages: list[dict[str, Any]] = []
        for item in input_value:
            if isinstance(item, dict):
                item_type = item.get("type")
                if item_type == "function_call_output":
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": str(item.get("call_id") or item.get("id") or ""),
                            "content": content_to_text(item.get("output")),
                        }
                    )
                    continue
                if item_type == "function_call":
                    call_id = str(item.get("call_id") or item.get("id") or "call_" + uuid.uuid4().hex)
                    messages.append(
                        {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": call_id,
                                    "type": "function",
                                    "function": {
                                        "name": str(item.get("name") or ""),
                                        "arguments": item.get("arguments") or "{}",
                                    },
                                }
                            ],
                        }
                    )
                    continue
                role = str(item.get("role") or "user")
                content = item.get("content", item.get("text", ""))
                messages.append({"role": role, "content": content_to_text(content)})
            else:
                messages.append({"role": "user", "content": str(item)})
        return messages or [{"role": "user", "content": ""}]
    return [{"role": "user", "content": str(input_value)}]


def response_tools_to_chat_tools(tools: Any) -> list[dict[str, Any]]:
    if not isinstance(tools, list):
        return []
    chat_tools: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        if tool.get("type") == "function" or tool.get("name"):
            function = tool.get("function") if isinstance(tool.get("function"), dict) else tool
            name = function.get("name")
            if not name:
                continue
            chat_tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": function.get("description", ""),
                        "parameters": function.get("parameters", {"type": "object", "properties": {}}),
                    },
                }
            )
    return chat_tools


def response_tool_choice_to_chat(tool_choice: Any) -> Any:
    if tool_choice in (None, "auto", "none", "required"):
        return tool_choice
    if isinstance(tool_choice, dict):
        if tool_choice.get("type") == "function" and tool_choice.get("name"):
            return {"type": "function", "function": {"name": tool_choice["name"]}}
        function = tool_choice.get("function")
        if isinstance(function, dict) and function.get("name"):
            return {"type": "function", "function": {"name": function["name"]}}
    return tool_choice


def response_payload_to_chat(payload: dict[str, Any]) -> dict[str, Any]:
    chat_payload = {
        "model": payload.get("model", "gpt-4o-mini"),
        "messages": normalize_response_input(payload.get("input", "")),
        "stream": False,
    }
    for source, target in [
        ("temperature", "temperature"),
        ("top_p", "top_p"),
        ("max_output_tokens", "max_tokens"),
        ("max_tokens", "max_tokens"),
    ]:
        if source in payload:
            chat_payload[target] = payload[source]
    chat_tools = response_tools_to_chat_tools(payload.get("tools"))
    if chat_tools:
        chat_payload["tools"] = chat_tools
    if "tool_choice" in payload:
        chat_payload["tool_choice"] = response_tool_choice_to_chat(payload.get("tool_choice"))
    return chat_payload


def chat_to_response(chat_data: dict[str, Any], model: str) -> dict[str, Any]:
    choice = (chat_data.get("choices") or [{}])[0]
    message = choice.get("message") or {}
    output_text = message.get("content") or ""
    response_id = "resp_" + uuid.uuid4().hex
    created_at = int(time.time())
    output_items: list[dict[str, Any]] = []
    if output_text:
        output_items.append(
            {
                "id": "msg_" + uuid.uuid4().hex,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": output_text,
                        "annotations": [],
                    }
                ],
            }
        )
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            function = tool_call.get("function") if isinstance(tool_call.get("function"), dict) else {}
            output_items.append(
                {
                    "id": tool_call.get("id") or "fc_" + uuid.uuid4().hex,
                    "type": "function_call",
                    "status": "completed",
                    "call_id": tool_call.get("id") or "call_" + uuid.uuid4().hex,
                    "name": function.get("name") or "",
                    "arguments": function.get("arguments") or "{}",
                }
            )
    if not output_items:
        output_items.append(
            {
                "id": "msg_" + uuid.uuid4().hex,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "",
                        "annotations": [],
                    }
                ],
            }
        )
    return {
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "model": chat_data.get("model") or model,
        "output": output_items,
        "output_text": output_text,
        "usage": response_usage_from_chat_usage(chat_data.get("usage")),
    }


def sse_event(event: str, data: dict[str, Any]) -> bytes:
    return (
        f"event: {event}\n"
        f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    ).encode("utf-8")


def parse_sse_event_block(block: str) -> dict[str, Any] | str | None:
    data_lines: list[str] = []
    for line in block.splitlines():
        if line.startswith("data:"):
            data_lines.append(line.removeprefix("data:").strip())
    if not data_lines:
        return None
    data = "\n".join(data_lines)
    if data == "[DONE]":
        return "[DONE]"
    try:
        value = json.loads(data)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


async def upstream_chat_stream_events(
    provider: ProviderConfig,
    payload: dict[str, Any],
) -> AsyncIterator[dict[str, Any] | str]:
    url = f"{provider.base_url}/chat/completions"
    async with httpx.AsyncClient(timeout=timeout()) as client:
        async with client.stream("POST", url, headers=auth_headers(provider), json=payload) as response:
            if response.status_code >= 400:
                body = await response.aread()
                text = body.decode("utf-8", errors="replace")
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}: {text[:400]}",
                    request=response.request,
                    response=response,
                )
            buffer = ""
            saw_event = False
            async for chunk in response.aiter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    block, buffer = buffer.split("\n\n", 1)
                    event = parse_sse_event_block(block)
                    if event is None:
                        continue
                    if isinstance(event, dict) and event.get("error"):
                        raise UpstreamStreamError(json.dumps(event["error"], ensure_ascii=False))
                    saw_event = True
                    yield event
            if buffer.strip():
                event = parse_sse_event_block(buffer)
                if isinstance(event, dict) and event.get("error"):
                    raise UpstreamStreamError(json.dumps(event["error"], ensure_ascii=False))
                if event is not None:
                    saw_event = True
                    yield event
            if not saw_event:
                raise UpstreamStreamError("stream ended without SSE events")


async def stream_response_from_provider(
    provider: ProviderConfig,
    chat_payload: dict[str, Any],
    response_id: str,
    created_at: int,
    model: str,
) -> AsyncIterator[tuple[bytes, dict[str, Any] | None]]:
    item_id = "msg_" + uuid.uuid4().hex
    output_text_parts: list[str] = []
    tool_call_buffers: dict[int, dict[str, Any]] = {}
    usage: dict[str, Any] | None = None
    message_added = False
    tool_added: set[int] = set()

    item = {
        "id": item_id,
        "type": "message",
        "status": "in_progress",
        "role": "assistant",
        "content": [],
    }

    async for event in upstream_chat_stream_events(provider, chat_payload):
        if event == "[DONE]":
            break
        if not isinstance(event, dict):
            continue
        if isinstance(event.get("usage"), dict):
            usage = event["usage"]
        choices = event.get("choices")
        if not isinstance(choices, list) or not choices:
            continue
        first = choices[0]
        if not isinstance(first, dict):
            continue
        delta = first.get("delta")
        message = first.get("message")
        payload = delta if isinstance(delta, dict) else message if isinstance(message, dict) else {}
        content = payload.get("content")
        tool_calls = payload.get("tool_calls")
        if isinstance(content, str) and content:
            if not message_added:
                message_added = True
                yield sse_event("response.in_progress", {
                    "type": "response.in_progress",
                    "response": {
                        "id": response_id,
                        "object": "response",
                        "created_at": created_at,
                        "status": "in_progress",
                        "model": model,
                        "output": [],
                    },
                }), None
                yield sse_event("response.output_item.added", {
                    "type": "response.output_item.added",
                    "output_index": 0,
                    "item": item,
                }), None
            output_text_parts.append(content)
            yield sse_event("response.output_text.delta", {
                "type": "response.output_text.delta",
                "item_id": item_id,
                "output_index": 0,
                "content_index": 0,
                "delta": content,
            }), None
        if isinstance(tool_calls, list):
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                index = int(tool_call.get("index", len(tool_call_buffers)))
                buffer = tool_call_buffers.setdefault(
                    index,
                    {
                        "id": tool_call.get("id") or "fc_" + uuid.uuid4().hex,
                        "call_id": tool_call.get("id") or "call_" + uuid.uuid4().hex,
                        "name": "",
                        "arguments": "",
                    },
                )
                if tool_call.get("id"):
                    buffer["id"] = tool_call["id"]
                    buffer["call_id"] = tool_call["id"]
                function = tool_call.get("function") if isinstance(tool_call.get("function"), dict) else {}
                if function.get("name"):
                    buffer["name"] = function["name"]
                argument_delta = function.get("arguments")
                if isinstance(argument_delta, str):
                    buffer["arguments"] += argument_delta
                if index not in tool_added and buffer["name"]:
                    tool_added.add(index)
                    yield sse_event("response.in_progress", {
                        "type": "response.in_progress",
                        "response": {
                            "id": response_id,
                            "object": "response",
                            "created_at": created_at,
                            "status": "in_progress",
                            "model": model,
                            "output": [],
                        },
                    }), None
                    yield sse_event("response.output_item.added", {
                        "type": "response.output_item.added",
                        "output_index": index,
                        "item": {
                            "id": buffer["id"],
                            "type": "function_call",
                            "status": "in_progress",
                            "call_id": buffer["call_id"],
                            "name": buffer["name"],
                            "arguments": "",
                        },
                    }), None
                if index in tool_added and argument_delta:
                    yield sse_event("response.function_call_arguments.delta", {
                        "type": "response.function_call_arguments.delta",
                        "item_id": buffer["id"],
                        "output_index": index,
                        "delta": argument_delta,
                    }), None

    output_text = "".join(output_text_parts)
    if not message_added and not tool_call_buffers:
        raise UpstreamStreamError("stream ended without assistant content")
    output_items: list[dict[str, Any]] = []
    item_done = {
        **item,
        "status": "completed",
        "content": [
            {
                "type": "output_text",
                "text": output_text,
                "annotations": [],
            }
        ],
    }
    if message_added:
        output_items.append(item_done)
    for index in sorted(tool_call_buffers):
        buffer = tool_call_buffers[index]
        if index not in tool_added:
            yield sse_event("response.output_item.added", {
                "type": "response.output_item.added",
                "output_index": index,
                "item": {
                    "id": buffer["id"],
                    "type": "function_call",
                    "status": "in_progress",
                    "call_id": buffer["call_id"],
                    "name": buffer["name"],
                    "arguments": "",
                },
            }), None
        yield sse_event("response.function_call_arguments.done", {
            "type": "response.function_call_arguments.done",
            "item_id": buffer["id"],
            "output_index": index,
            "arguments": buffer["arguments"] or "{}",
        }), None
        tool_item = {
            "id": buffer["id"],
            "type": "function_call",
            "status": "completed",
            "call_id": buffer["call_id"],
            "name": buffer["name"],
            "arguments": buffer["arguments"] or "{}",
        }
        yield sse_event("response.output_item.done", {
            "type": "response.output_item.done",
            "output_index": index,
            "item": tool_item,
        }), None
        output_items.append(tool_item)
    completed = {
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "model": model,
        "output": output_items,
        "output_text": output_text,
        "usage": response_usage_from_chat_usage(usage),
    }
    if message_added:
        yield sse_event("response.output_text.done", {
            "type": "response.output_text.done",
            "item_id": item_id,
            "output_index": 0,
            "content_index": 0,
            "text": output_text,
        }), None
        yield sse_event("response.output_item.done", {
            "type": "response.output_item.done",
            "output_index": 0,
            "item": item_done,
        }), None
    yield sse_event("response.completed", {"type": "response.completed", "response": completed}), usage


async def responses_stream(payload: dict[str, Any], request_id: str) -> AsyncIterator[bytes]:
    response_id = "resp_" + uuid.uuid4().hex
    created_at = int(time.time())
    model = str(payload.get("model") or "gpt-4o-mini")
    started = {
        "id": response_id,
        "object": "response",
        "created_at": created_at,
        "status": "in_progress",
        "model": model,
        "output": [],
    }
    yield sse_event("response.created", {"type": "response.created", "response": started})

    chat_payload = response_payload_to_chat(payload)
    chat_payload["stream"] = True
    started_at = time.time()
    errors: list[dict[str, Any]] = []
    providers = available_providers()
    providers = providers[: config.max_route_attempts]
    for provider in providers:
        try:
            final_usage = None
            stream_iterator = stream_response_from_provider(provider, chat_payload, response_id, created_at, model)
            try:
                async with asyncio.timeout(config.stream_first_token_timeout_seconds):
                    first_chunk, first_usage = await anext(stream_iterator)
            except TimeoutError as exc:
                raise UpstreamStreamError(
                    f"first token timeout after {config.stream_first_token_timeout_seconds}s"
                ) from exc
            if first_usage is not None:
                final_usage = first_usage
            yield first_chunk
            async for chunk, usage in stream_iterator:
                if usage is not None:
                    final_usage = usage
                yield chunk
            mark_success(provider)
            append_usage_event(
                {
                    "request_id": request_id,
                    "endpoint": "/v1/responses",
                    "provider": provider.name,
                    "base_url": provider.base_url,
                    "key_hint": key_hint(provider.api_key),
                    "route": provider_route_label(provider),
                    "model": model_from_payload(chat_payload),
                    "status": "success",
                    "stream": True,
                    "duration_ms": int((time.time() - started_at) * 1000),
                    "usage": final_usage,
                    "error": None,
                    "attempt_errors": errors,
                }
            )
            yield b"data: [DONE]\n\n"
            return
        except httpx.HTTPStatusError as exc:
            mark_failure(provider, str(exc), exc.response.status_code)
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": str(exc)})
        except UpstreamStreamError as exc:
            mark_failure(provider, "stream_error: " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": "stream_error: " + str(exc)})
        except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
            mark_failure(provider, type(exc).__name__ + ": " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": type(exc).__name__ + ": " + str(exc)})

    append_usage_event(
        {
            "request_id": request_id,
            "endpoint": "/v1/responses",
            "provider": None,
            "base_url": None,
            "key_hint": None,
            "route": None,
            "model": model,
            "status": "failed",
            "stream": True,
            "duration_ms": int((time.time() - started_at) * 1000),
            "usage": None,
            "error": "All upstream providers failed.",
            "attempt_errors": errors,
        }
    )
    failed = {
        **started,
        "status": "failed",
        "error": {
            "message": "All upstream providers failed.",
            "type": "upstream_unavailable",
            "details": errors,
        },
    }
    yield sse_event("response.failed", {"type": "response.failed", "response": failed})
    yield b"data: [DONE]\n\n"


ADMIN_HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Local Key Router Admin</title>
  <style>
    :root {
      color-scheme: light;
      --bg: oklch(0.985 0 0);
      --surface: oklch(1 0 0);
      --surface-2: oklch(0.958 0.006 230);
      --ink: oklch(0.205 0.018 230);
      --muted: oklch(0.455 0.018 230);
      --line: oklch(0.895 0.01 230);
      --primary: oklch(0.450 0.086 230);
      --primary-strong: oklch(0.360 0.092 230);
      --accent: oklch(0.610 0.140 34);
      --success: oklch(0.540 0.130 150);
      --warning: oklch(0.690 0.135 70);
      --danger: oklch(0.560 0.165 25);
      --info: oklch(0.560 0.105 250);
      --radius: 10px;
      --shadow: 0 1px 2px oklch(0 0 0 / 0.06);
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--sans);
      font-size: 14px;
      line-height: 1.45;
    }

    button, input { font: inherit; }

    .shell {
      min-height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr;
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      min-height: 58px;
      padding: 0 24px;
      border-bottom: 1px solid var(--line);
      background: oklch(0.985 0 0 / 0.92);
      backdrop-filter: blur(10px);
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
      min-width: 0;
    }

    .mark {
      width: 30px;
      height: 30px;
      display: grid;
      place-items: center;
      border-radius: 8px;
      background: var(--primary);
      color: white;
      font-family: var(--mono);
      font-weight: 700;
    }

    h1 {
      margin: 0;
      font-size: 16px;
      line-height: 1.2;
      letter-spacing: 0;
    }

    .subtle {
      color: var(--muted);
      font-size: 12px;
    }

    .actions {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }

    .pill, .button {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 32px;
      border-radius: 999px;
      white-space: nowrap;
    }

    .pill {
      padding: 0 11px;
      background: var(--surface-2);
      border: 1px solid var(--line);
      color: var(--muted);
      font-family: var(--mono);
      font-size: 12px;
    }

    .button {
      border: 1px solid var(--primary);
      background: var(--primary);
      color: white;
      padding: 0 13px;
      cursor: pointer;
    }

    .button:hover { background: var(--primary-strong); }
    .button:focus-visible, .row:focus-visible {
      outline: 3px solid oklch(0.760 0.095 230 / 0.55);
      outline-offset: 2px;
    }

    .button.secondary {
      border-color: var(--line);
      background: var(--surface);
      color: var(--ink);
    }

    .button.danger {
      border-color: var(--danger);
      background: var(--danger);
    }

    .icon-button {
      min-height: 28px;
      padding: 0 9px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      cursor: pointer;
    }

    .icon-button:hover {
      border-color: var(--primary);
      color: var(--primary-strong);
    }

    .content {
      width: 100%;
      margin: 0;
      padding: 22px 24px 28px;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }

    .metric {
      min-width: 0;
      padding: 14px 15px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      box-shadow: var(--shadow);
    }

    .metric-label {
      margin-bottom: 6px;
      color: var(--muted);
      font-size: 12px;
    }

    .metric-value {
      overflow: hidden;
      text-overflow: ellipsis;
      font-family: var(--mono);
      font-size: 22px;
      font-weight: 700;
      line-height: 1.15;
    }

    .layout {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
      align-items: start;
    }

    .records-panel {
      grid-column: 1 / -1;
      order: -1;
    }

    .providers-panel {
      min-width: 0;
      grid-column: 1 / -1;
    }

    .records-table th,
    .records-table td {
      padding: 9px 10px;
    }

    .records-table td {
      font-family: var(--mono);
      font-size: 12px;
    }

    .metric-blue { color: oklch(0.560 0.180 255); font-weight: 700; }
    .metric-green { color: oklch(0.560 0.145 135); font-weight: 700; }
    .metric-amber { color: oklch(0.640 0.145 70); font-weight: 700; }
    .metric-violet { color: oklch(0.590 0.185 305); font-weight: 700; }
    .metric-total { color: oklch(0.570 0.110 42); font-weight: 800; }
    .metric-muted { color: var(--muted); font-weight: 650; }

    .editor {
      margin-bottom: 16px;
    }

    .form-grid {
      display: grid;
      grid-template-columns: minmax(150px, 0.7fr) minmax(230px, 1.15fr) minmax(230px, 1.25fr) minmax(96px, 0.35fr) auto;
      gap: 10px;
      align-items: end;
      padding: 14px 15px;
    }

    label {
      display: grid;
      gap: 5px;
      min-width: 0;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }

    input[type="text"], input[type="password"], input[type="number"] {
      width: 100%;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
      color: var(--ink);
      padding: 0 10px;
      font-family: var(--mono);
      font-size: 12px;
    }

    input[type="checkbox"] {
      width: 16px;
      height: 16px;
      accent-color: var(--primary);
    }

    input:focus-visible {
      outline: 3px solid oklch(0.760 0.095 230 / 0.55);
      outline-offset: 2px;
    }

    .check-row {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 36px;
      color: var(--ink);
      font-size: 13px;
    }

    .form-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      justify-content: flex-end;
      padding: 0 15px 14px;
    }

    .panel {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--surface);
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 13px 15px;
      border-bottom: 1px solid var(--line);
      background: var(--surface-2);
    }

    .panel-title {
      margin: 0;
      font-size: 14px;
      font-weight: 700;
    }

    .table-wrap {
      overflow: auto;
      max-height: calc(100vh - 250px);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
    }

    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: middle;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--surface);
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }

    td {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .mono { font-family: var(--mono); font-size: 12px; }
    .right { text-align: right; }

    .status {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-weight: 650;
    }

    .dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      background: var(--muted);
      flex: 0 0 auto;
    }

    .dot.ok { background: var(--success); }
    .dot.warn { background: var(--warning); }
    .dot.bad { background: var(--danger); }

    .tag {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 0 8px;
      border-radius: 999px;
      background: var(--surface-2);
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }

    .tag.success { background: oklch(0.930 0.045 150); color: oklch(0.315 0.090 150); }
    .tag.failed { background: oklch(0.940 0.050 25); color: oklch(0.390 0.125 25); }
    .tag.stream { background: oklch(0.930 0.040 250); color: oklch(0.335 0.095 250); }

    .row {
      cursor: pointer;
    }

    .row:hover td {
      background: oklch(0.970 0.010 230);
    }

    .row.selected td {
      background: oklch(0.940 0.020 230);
    }

    .detail {
      display: grid;
      gap: 10px;
      padding: 14px 15px;
      border-top: 1px solid var(--line);
      background: oklch(0.980 0.004 230);
    }

    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }

    .kv {
      min-width: 0;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--surface);
    }

    .kv-label {
      margin-bottom: 4px;
      color: var(--muted);
      font-size: 11px;
    }

    .kv-value {
      overflow-wrap: anywhere;
      font-family: var(--mono);
      font-size: 12px;
    }

    pre {
      max-height: 240px;
      margin: 0;
      overflow: auto;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: oklch(0.145 0.010 230);
      color: oklch(0.945 0.006 230);
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.55;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .empty {
      padding: 28px 16px;
      color: var(--muted);
      text-align: center;
    }

    .error-banner {
      display: none;
      margin-bottom: 16px;
      padding: 12px 14px;
      border: 1px solid oklch(0.760 0.090 25);
      border-radius: var(--radius);
      background: oklch(0.960 0.035 25);
      color: oklch(0.350 0.125 25);
    }

    @media (max-width: 1080px) {
      .metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .layout { grid-template-columns: 1fr; }
      .form-grid { grid-template-columns: 1fr 1fr; }
      .table-wrap { max-height: none; }
    }

    @media (max-width: 680px) {
      .topbar { align-items: flex-start; padding: 12px 14px; flex-direction: column; }
      .content { padding: 14px; }
      .metrics { grid-template-columns: 1fr; }
      .form-grid { grid-template-columns: 1fr; }
      .form-actions { justify-content: flex-start; flex-wrap: wrap; }
      .detail-grid { grid-template-columns: 1fr; }
      th, td { padding: 9px 10px; }
      .provider-url, .provider-key, .optional-col { display: none; }
    }

    @media (prefers-reduced-motion: no-preference) {
      .button, .row td { transition: background-color 160ms ease-out, border-color 160ms ease-out; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header class="topbar">
      <div class="brand">
        <div class="mark">LR</div>
        <div>
          <h1>Local Key Router</h1>
          <div class="subtle">127.0.0.1:8787 · 本地管理面板</div>
        </div>
      </div>
      <div class="actions">
        <span class="pill" id="lastRefresh">refreshing...</span>
        <button class="button" id="refreshButton" type="button">Refresh</button>
      </div>
    </header>

    <main class="content">
      <div class="error-banner" id="errorBanner"></div>

      <section class="metrics" aria-label="Router metrics">
        <div class="metric">
          <div class="metric-label">Providers</div>
          <div class="metric-value" id="metricProviders">-</div>
        </div>
        <div class="metric">
          <div class="metric-label">Healthy</div>
          <div class="metric-value" id="metricHealthy">-</div>
        </div>
        <div class="metric">
          <div class="metric-label">Last Provider</div>
          <div class="metric-value" id="metricLastProvider">-</div>
        </div>
        <div class="metric">
          <div class="metric-label">Last Status</div>
          <div class="metric-value" id="metricLastStatus">-</div>
        </div>
      </section>

      <section class="panel editor" aria-label="Provider editor">
        <div class="panel-head">
          <h2 class="panel-title" id="providerFormTitle">Add Provider</h2>
          <span class="subtle">每个 URL + Key 组合会单独统计</span>
        </div>
        <form id="providerForm">
          <div class="form-grid">
            <label>
              Name
              <input id="providerName" type="text" autocomplete="off" placeholder="optional-auto-name" />
            </label>
            <label>
              Base URL
              <input id="providerBaseUrl" type="text" autocomplete="off" placeholder="https://example.com/v1" required />
            </label>
            <label>
              API Key
              <input id="providerApiKey" type="text" autocomplete="off" placeholder="sk-..." required />
            </label>
            <label>
              Priority
              <input id="providerPriority" type="number" inputmode="numeric" step="1" value="0" />
            </label>
            <label>
              Enabled
              <span class="check-row"><input id="providerEnabled" type="checkbox" checked /> Active</span>
            </label>
          </div>
          <div class="form-actions">
            <button class="button" id="saveProviderButton" type="submit">Add Provider</button>
            <button class="button secondary" id="cancelEditButton" type="button">Clear</button>
          </div>
        </form>
      </section>

      <section class="layout">
        <article class="panel providers-panel">
          <div class="panel-head">
            <h2 class="panel-title">Providers</h2>
            <span class="subtle" id="providerSummary">-</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th style="width: 18%">Name</th>
                  <th class="provider-url" style="width: 26%">Base URL</th>
                  <th class="provider-key" style="width: 15%">Key</th>
                  <th class="right" style="width: 7%">Priority</th>
                  <th style="width: 12%">State</th>
                  <th class="right" style="width: 6%">OK</th>
                  <th class="right" style="width: 6%">Fail</th>
                  <th class="right optional-col" style="width: 6%">Circuit</th>
                  <th class="right" style="width: 14%">Actions</th>
                </tr>
              </thead>
              <tbody id="providersBody"></tbody>
            </table>
          </div>
        </article>

        <article class="panel records-panel">
          <div class="panel-head">
            <h2 class="panel-title">Recent 20 Call Records - Codex</h2>
            <span class="subtle" id="usageSummary">-</span>
          </div>
          <div class="table-wrap">
            <table class="records-table">
              <thead>
                <tr>
                  <th style="width: 12%">Time</th>
                  <th style="width: 18%">Route</th>
                  <th style="width: 10%">Model</th>
                  <th class="right" style="width: 8%">Input</th>
                  <th class="right" style="width: 8%">Output</th>
                  <th class="right" style="width: 9%">Cache Input</th>
                  <th class="right" style="width: 9%">Reasoning</th>
                  <th class="right" style="width: 8%">Total</th>
                  <th class="right" style="width: 8%">Duration</th>
                  <th style="width: 10%">Status</th>
                </tr>
              </thead>
              <tbody id="usageBody"></tbody>
            </table>
          </div>
          <div class="detail" id="usageDetail">
            <div class="empty">No request selected yet.</div>
          </div>
        </article>
      </section>
    </main>
  </div>

  <script>
    const LOCAL_KEY = "__LOCAL_API_KEY__";
    let selectedRequestId = null;
    let providerConfigs = [];
    let editingProviderName = null;

    const $ = (id) => document.getElementById(id);
    const authHeaders = { "Authorization": `Bearer ${LOCAL_KEY}` };
    const jsonHeaders = { ...authHeaders, "Content-Type": "application/json" };

    function text(value, fallback = "-") {
      return value === null || value === undefined || value === "" ? fallback : String(value);
    }

    function escapeHtml(value) {
      return text(value, "").replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;",
      }[char]));
    }

    function keyLabel(value) {
      const key = text(value, "");
      if (!key) return "-";
      if (key.length <= 14) return key;
      return `${key.slice(0, 7)}...${key.slice(-6)}`;
    }

    function shortUrl(value) {
      try {
        const url = new URL(value);
        return `${url.host}${url.pathname === "/" ? "" : url.pathname}`;
      } catch {
        return text(value);
      }
    }

    function routeLabel(event) {
      if (event.route) return event.route;
      const provider = providerConfigs.find((item) => item.name === event.provider);
      if (provider) return `${provider.base_url} · ${keyLabel(provider.api_key)}`;
      if (event.base_url && event.key_hint) return `${event.base_url} · ${event.key_hint}`;
      return event.provider || event.base_url || "-";
    }

    function compactRoute(value) {
      const raw = text(value, "");
      if (!raw) return "-";
      const parts = raw.split(" · ");
      if (parts.length >= 2) return `${shortUrl(parts[0])} · ${parts.slice(1).join(" · ")}`;
      return raw.length > 34 ? `${raw.slice(0, 31)}...` : raw;
    }

    function tokenValue(usage, keys) {
      if (!usage) return null;
      for (const key of keys) {
        const value = key.split(".").reduce((current, part) => current && current[part], usage);
        if (typeof value === "number") return value;
      }
      return null;
    }

    function usageMetrics(event) {
      const usage = event.usage || {};
      const input = tokenValue(usage, ["prompt_tokens", "input_tokens"]);
      const output = tokenValue(usage, ["completion_tokens", "output_tokens"]);
      const cache = tokenValue(usage, ["prompt_tokens_details.cached_tokens", "input_tokens_details.cached_tokens"]);
      const reasoning = tokenValue(usage, ["completion_tokens_details.reasoning_tokens", "output_tokens_details.reasoning_tokens", "reasoning_tokens"]);
      const total = tokenValue(usage, ["total_tokens"]) ?? [input, output].filter((value) => value !== null).reduce((sum, value) => sum + value, 0);
      return { input, output, cache, reasoning, total: total || null };
    }

    function formatNumber(value) {
      if (value === null || value === undefined) return "-";
      if (Math.abs(value) >= 1000000) return `${(value / 1000000).toFixed(2)}M`;
      if (Math.abs(value) >= 10000) return `${(value / 1000).toFixed(2)}K`;
      return new Intl.NumberFormat().format(value);
    }

    function formatDuration(ms) {
      if (ms === null || ms === undefined) return "-";
      if (ms >= 1000) return `${(ms / 1000).toFixed(2)}s`;
      return `${ms}ms`;
    }

    function fmtTime(value) {
      if (!value) return "-";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }

    function statusClass(provider) {
      if (!provider.enabled || provider.circuit_open) return "bad";
      if (provider.consecutive_failures > 0) return "warn";
      return "ok";
    }

    function statusLabel(provider) {
      if (!provider.enabled) return "disabled";
      if (provider.circuit_open) return "circuit";
      if (provider.consecutive_failures > 0) return "degraded";
      return "healthy";
    }

    function providerRows(statusProviders, configs) {
      const statusByName = new Map(statusProviders.map((provider) => [provider.name, provider]));
      return configs.map((config, index) => ({
        ...statusByName.get(config.name),
        ...config,
        _order: index,
        healthy: statusByName.get(config.name)?.healthy ?? false,
        circuit_open: statusByName.get(config.name)?.circuit_open ?? false,
        circuit_remaining_seconds: statusByName.get(config.name)?.circuit_remaining_seconds ?? 0,
        consecutive_failures: statusByName.get(config.name)?.consecutive_failures ?? 0,
        success_count: statusByName.get(config.name)?.success_count ?? 0,
        failure_count: statusByName.get(config.name)?.failure_count ?? 0,
      })).sort((left, right) => (right.priority || 0) - (left.priority || 0) || left._order - right._order);
    }

    function renderProviders(providers) {
      const body = $("providersBody");
      body.innerHTML = "";
      if (!providers.length) {
        body.innerHTML = `<tr><td colspan="8"><div class="empty">No providers configured.</div></td></tr>`;
        return;
      }
      for (const provider of providers) {
        const cls = statusClass(provider);
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td class="mono" title="${escapeHtml(provider.name)}">${escapeHtml(provider.name)}</td>
          <td class="mono provider-url" title="${escapeHtml(provider.base_url)}">${escapeHtml(provider.base_url)}</td>
          <td class="mono provider-key" title="${escapeHtml(provider.api_key)}">${escapeHtml(keyLabel(provider.api_key))}</td>
          <td class="right mono">${provider.priority || 0}</td>
          <td><span class="status"><span class="dot ${cls}"></span>${statusLabel(provider)}</span></td>
          <td class="right mono">${provider.success_count}</td>
          <td class="right mono">${provider.failure_count}</td>
          <td class="right mono optional-col">${provider.circuit_remaining_seconds || 0}s</td>
          <td class="right">
            <button class="icon-button" type="button" data-action="edit" data-name="${escapeHtml(provider.name)}">Edit</button>
            <button class="icon-button" type="button" data-action="delete" data-name="${escapeHtml(provider.name)}">Del</button>
          </td>
        `;
        tr.querySelector('[data-action="edit"]').addEventListener("click", () => startEditProvider(provider.name));
        tr.querySelector('[data-action="delete"]').addEventListener("click", () => deleteProvider(provider.name));
        body.appendChild(tr);
      }
    }

    function resetProviderForm() {
      editingProviderName = null;
      $("providerFormTitle").textContent = "Add Provider";
      $("saveProviderButton").textContent = "Add Provider";
      $("providerName").value = "";
      $("providerBaseUrl").value = "";
      $("providerApiKey").value = "";
      $("providerPriority").value = "0";
      $("providerEnabled").checked = true;
    }

    function startEditProvider(name) {
      const provider = providerConfigs.find((item) => item.name === name);
      if (!provider) return;
      editingProviderName = provider.name;
      $("providerFormTitle").textContent = `Edit ${provider.name}`;
      $("saveProviderButton").textContent = "Save Provider";
      $("providerName").value = provider.name;
      $("providerBaseUrl").value = provider.base_url;
      $("providerApiKey").value = provider.api_key;
      $("providerPriority").value = provider.priority || 0;
      $("providerEnabled").checked = provider.enabled;
      $("providerName").focus();
    }

    async function saveProvider(event) {
      event.preventDefault();
      const payload = {
        name: $("providerName").value.trim() || null,
        base_url: $("providerBaseUrl").value.trim(),
        api_key: $("providerApiKey").value.trim(),
        enabled: $("providerEnabled").checked,
        priority: Number.parseInt($("providerPriority").value || "0", 10) || 0,
      };
      if (!payload.base_url || !payload.api_key) return;
      const url = editingProviderName ? `/providers/${encodeURIComponent(editingProviderName)}` : "/providers";
      const method = editingProviderName ? "PUT" : "POST";
      await fetchJson(url, {
        method,
        headers: jsonHeaders,
        body: JSON.stringify(payload),
      });
      resetProviderForm();
      await refresh();
    }

    async function deleteProvider(name) {
      const ok = window.confirm(`Delete provider "${name}"?`);
      if (!ok) return;
      await fetchJson(`/providers/${encodeURIComponent(name)}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (editingProviderName === name) resetProviderForm();
      await refresh();
    }

    function renderUsage(events) {
      const body = $("usageBody");
      body.innerHTML = "";
      if (!events.length) {
        body.innerHTML = `<tr><td colspan="10"><div class="empty">No usage events yet. Send a chat request to populate this table.</div></td></tr>`;
        renderDetail(null);
        return;
      }
      if (!selectedRequestId || !events.some((event) => event.request_id === selectedRequestId)) {
        selectedRequestId = events[0].request_id;
      }
      for (const event of events) {
        const metrics = usageMetrics(event);
        const route = routeLabel(event);
        const tr = document.createElement("tr");
        tr.className = `row ${event.request_id === selectedRequestId ? "selected" : ""}`;
        tr.tabIndex = 0;
        tr.dataset.requestId = event.request_id;
        const streamTag = event.stream ? `<span class="tag stream">stream</span>` : "";
        tr.innerHTML = `
          <td class="mono" title="${event.timestamp}">${fmtTime(event.timestamp)}</td>
          <td class="mono" title="${escapeHtml(route)}">${escapeHtml(compactRoute(route))}</td>
          <td class="mono" title="${text(event.model)}">${text(event.model)}</td>
          <td class="right metric-blue">${formatNumber(metrics.input)}</td>
          <td class="right metric-green">${formatNumber(metrics.output)}</td>
          <td class="right metric-amber">${formatNumber(metrics.cache)}</td>
          <td class="right metric-violet">${formatNumber(metrics.reasoning)}</td>
          <td class="right metric-total">${formatNumber(metrics.total)}</td>
          <td class="right metric-muted">${formatDuration(event.duration_ms)}</td>
          <td><span class="tag ${event.status}">${event.status}</span> ${streamTag}</td>
        `;
        tr.addEventListener("click", () => {
          selectedRequestId = event.request_id;
          renderUsage(events);
        });
        tr.addEventListener("keydown", (keyboardEvent) => {
          if (keyboardEvent.key === "Enter" || keyboardEvent.key === " ") {
            keyboardEvent.preventDefault();
            selectedRequestId = event.request_id;
            renderUsage(events);
          }
        });
        body.appendChild(tr);
      }
      renderDetail(events.find((event) => event.request_id === selectedRequestId) || events[0]);
    }

    function renderDetail(event) {
      const detail = $("usageDetail");
      if (!event) {
        detail.innerHTML = `<div class="empty">No request selected yet.</div>`;
        return;
      }
      const usage = event.usage ? JSON.stringify(event.usage, null, 2) : "null";
      const attempts = event.attempt_errors && event.attempt_errors.length
        ? JSON.stringify(event.attempt_errors, null, 2)
        : "[]";
      const route = routeLabel(event);
      detail.innerHTML = `
        <div class="detail-grid">
          <div class="kv">
            <div class="kv-label">Request ID</div>
            <div class="kv-value">${event.request_id}</div>
          </div>
          <div class="kv">
            <div class="kv-label">Endpoint</div>
            <div class="kv-value">${event.endpoint}</div>
          </div>
          <div class="kv">
            <div class="kv-label">Base URL</div>
            <div class="kv-value">${text(event.base_url)}</div>
          </div>
          <div class="kv">
            <div class="kv-label">URL + Key</div>
            <div class="kv-value">${escapeHtml(route)}</div>
          </div>
          <div class="kv">
            <div class="kv-label">Error</div>
            <div class="kv-value">${text(event.error)}</div>
          </div>
        </div>
        <pre>${usage}</pre>
        <pre>${attempts}</pre>
      `;
    }

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, options);
      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
      return response.json();
    }

    async function refresh() {
      $("errorBanner").style.display = "none";
      try {
        const [status, usage, providerConfigResponse] = await Promise.all([
          fetchJson("/status"),
          fetchJson("/usage?limit=20", { headers: authHeaders }),
          fetchJson("/providers", { headers: authHeaders }),
        ]);
        const providers = status.providers || [];
        providerConfigs = providerConfigResponse.data || [];
        const providerTableRows = providerRows(providers, providerConfigs);
        const events = usage.data || [];
        const healthy = providers.filter((provider) => provider.healthy).length;
        const last = events[0] || null;
        $("metricProviders").textContent = providerConfigs.length;
        $("metricHealthy").textContent = `${healthy}/${providerConfigs.length}`;
        $("metricLastProvider").textContent = last ? text(last.provider) : "-";
        $("metricLastStatus").textContent = last ? text(last.status) : "-";
        $("providerSummary").textContent = `${providerConfigs.length} configured · counted per URL+Key`;
        $("usageSummary").textContent = `${events.length} records · URL+Key shown`;
        $("lastRefresh").textContent = `updated ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}`;
        renderProviders(providerTableRows);
        renderUsage(events);
      } catch (error) {
        $("errorBanner").textContent = `Admin refresh failed: ${error.message}`;
        $("errorBanner").style.display = "block";
      }
    }

    $("refreshButton").addEventListener("click", refresh);
    $("providerForm").addEventListener("submit", saveProvider);
    $("cancelEditButton").addEventListener("click", resetProviderForm);
    refresh();
    window.setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


@app.get("/status")
async def status() -> dict[str, Any]:
    return {
        "service": "local-key-router",
        "providers": [provider_status(provider) for provider in config.providers],
    }


@app.get("/")
async def root() -> dict[str, Any]:
    return service_info()


@app.get("/v1")
async def v1_root() -> dict[str, Any]:
    return service_info()


@app.get("/admin", response_class=HTMLResponse)
async def admin() -> str:
    return ADMIN_HTML.replace("__LOCAL_API_KEY__", config.local_api_key)


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


def service_info() -> dict[str, Any]:
    return {
        "service": "local-key-router",
        "base_url": "http://127.0.0.1:8787/v1",
        "local_api_key": config.local_api_key,
        "endpoints": {
            "admin": "GET /admin",
            "status": "GET /status",
            "usage": "GET /usage",
            "providers": "GET/POST/PUT/DELETE /providers",
            "models": "GET /v1/models",
            "chat_completions": "POST /v1/chat/completions",
            "responses": "POST /v1/responses",
        },
    }


@app.get("/usage", dependencies=[Depends(require_local_auth)])
async def usage(limit: int = 50) -> dict[str, Any]:
    limit = max(1, min(limit, 500))
    return {
        "object": "list",
        "data": read_recent_usage_events(limit),
        "log_file": str(USAGE_LOG_PATH),
    }


@app.get("/usage/{request_id}", dependencies=[Depends(require_local_auth)])
async def usage_detail(request_id: str) -> dict[str, Any]:
    for event in usage_events:
        if event.get("request_id") == request_id:
            return event
    if USAGE_LOG_PATH.exists():
        with USAGE_LOG_PATH.open("r", encoding="utf-8") as file:
            for line in reversed(file.readlines()):
                if not line.strip():
                    continue
                event = json.loads(line)
                if event.get("request_id") == request_id:
                    return event
    raise HTTPException(status_code=404, detail="Usage event not found.")


@app.get("/providers", dependencies=[Depends(require_local_auth)])
async def list_providers() -> dict[str, Any]:
    with config_lock:
        return {
            "object": "list",
            "data": [provider_public(provider) for provider in config.providers],
        }


@app.post("/providers", dependencies=[Depends(require_local_auth)])
async def create_provider(provider_input: ProviderInput) -> dict[str, Any]:
    global config
    with config_lock:
        name = unique_provider_name(provider_input.name or slug_from_base_url(provider_input.base_url))
        provider = ProviderConfig(
            name=name,
            base_url=provider_input.base_url,
            api_key=provider_input.api_key,
            enabled=provider_input.enabled,
            priority=provider_input.priority,
        )
        config.providers.append(provider)
        rebuild_provider_states()
        save_config(config)
        return provider_public(provider)


@app.put("/providers/{name}", dependencies=[Depends(require_local_auth)])
async def update_provider(name: str, provider_input: ProviderInput) -> dict[str, Any]:
    global config
    with config_lock:
        index = find_provider_index(name)
        new_name = unique_provider_name(
            provider_input.name or name or slug_from_base_url(provider_input.base_url),
            skip_name=name,
        )
        provider = ProviderConfig(
            name=new_name,
            base_url=provider_input.base_url,
            api_key=provider_input.api_key,
            enabled=provider_input.enabled,
            priority=provider_input.priority,
        )
        if new_name != name and name in provider_states:
            old_state = provider_states.pop(name)
            old_state.name = new_name
            old_state.base_url = provider.base_url
            old_state.enabled = provider.enabled
            provider_states[new_name] = old_state
        config.providers[index] = provider
        rebuild_provider_states()
        save_config(config)
        return provider_public(provider)


@app.delete("/providers/{name}", dependencies=[Depends(require_local_auth)])
async def delete_provider(name: str) -> dict[str, Any]:
    global config
    with config_lock:
        index = find_provider_index(name)
        provider = config.providers.pop(index)
        provider_states.pop(provider.name, None)
        save_config(config)
        return {"deleted": provider.name}


@app.get("/v1/models", dependencies=[Depends(require_local_auth)])
async def models() -> JSONResponse:
    request_id = new_request_id()
    started_at = time.time()
    merged: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    success_providers: list[str] = []
    success_routes: list[str] = []
    for provider in available_providers():
        try:
            data = await request_json(provider, "GET", "/models")
            mark_success(provider)
            success_providers.append(provider.name)
            success_routes.append(provider_route_label(provider))
            for item in data.get("data", []):
                model_id = item.get("id")
                if model_id and model_id not in merged:
                    merged[model_id] = item
        except httpx.HTTPStatusError as exc:
            mark_failure(provider, str(exc), exc.response.status_code)
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": str(exc)})
        except (httpx.TimeoutException, httpx.TransportError, ValueError) as exc:
            mark_failure(provider, type(exc).__name__ + ": " + str(exc))
            errors.append({"provider": provider.name, "route": provider_route_label(provider), "error": type(exc).__name__ + ": " + str(exc)})
    if merged:
        append_usage_event(
            {
                "request_id": request_id,
                "endpoint": "/v1/models",
                "provider": ",".join(success_providers) or None,
                "base_url": None,
                "key_hint": None,
                "route": " | ".join(success_routes) or None,
                "model": None,
                "status": "success",
                "stream": False,
                "duration_ms": int((time.time() - started_at) * 1000),
                "usage": {"model_count": len(merged)},
                "error": None,
                "attempt_errors": errors,
            }
        )
        return JSONResponse({"object": "list", "data": list(merged.values()), "router_errors": errors})
    append_usage_event(
        {
            "request_id": request_id,
            "endpoint": "/v1/models",
            "provider": None,
            "base_url": None,
            "key_hint": None,
            "route": None,
            "model": None,
            "status": "failed",
            "stream": False,
            "duration_ms": int((time.time() - started_at) * 1000),
            "usage": None,
            "error": "All upstream providers failed.",
            "attempt_errors": errors,
        }
    )
    return all_failed(errors)


@app.post("/v1/chat/completions", dependencies=[Depends(require_local_auth)], response_model=None)
async def chat_completions(request: Request) -> JSONResponse | StreamingResponse:
    payload = await request.json()
    request_id = new_request_id()
    if payload.get("stream") is True:
        return StreamingResponse(routed_stream(payload, request_id), media_type="text/event-stream")
    return await try_non_stream_chat(payload, "/v1/chat/completions", request_id)


@app.post("/v1/responses", dependencies=[Depends(require_local_auth)], response_model=None)
async def responses(request: Request) -> JSONResponse | StreamingResponse:
    payload = await request.json()
    request_id = new_request_id()
    if payload.get("stream") is True:
        return StreamingResponse(responses_stream(payload, request_id), media_type="text/event-stream")
    chat_payload = response_payload_to_chat(payload)
    chat_result = await try_non_stream_chat(chat_payload, "/v1/responses", request_id)
    if chat_result.status_code >= 400:
        return chat_result
    chat_data = json.loads(chat_result.body)
    return JSONResponse(chat_to_response(chat_data, chat_payload["model"]))
