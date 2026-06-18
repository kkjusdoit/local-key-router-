# Local Key Router

本项目是一个独立的本地 OpenAI-compatible 聚合网关，用来把多个中转站 API key 汇集成一个本地出口。

## 启动

```bash
cd /Users/linkunkun/Documents/Codex/2026-06-18/api-key-base-url-https-cc/work/local-key-router
uv run uvicorn app.main:app --host 127.0.0.1 --port 8787
```

本地出口：

```text
base_url = http://127.0.0.1:8787/v1
api_key = sk-local-router-dev
```

## 后台管理面板

浏览器打开：

```text
http://127.0.0.1:8787/admin
```

面板可以查看上游健康状态、最近请求命中的 provider、耗时、usage 和失败轮换记录。

Provider 按 `base_url + api_key` 组合单独计算；同一个 URL 下多个 key 需要建多条 provider，后台会分别统计成功、失败、熔断和 usage。

面板支持动态新增、编辑、删除 provider，改动会保存回：

```text
config/providers.yaml
```

`priority` 用于手动提权：数值越大越优先使用；同 priority 时按配置文件顺序尝试。

## 状态查看

```bash
curl http://127.0.0.1:8787/status
```

## Usage Log

查看最近请求实际打到哪个上游：

```bash
curl http://127.0.0.1:8787/usage \
  -H 'Authorization: Bearer sk-local-router-dev'
```

日志也会持久写入：

```text
/Users/linkunkun/Documents/Codex/2026-06-18/api-key-base-url-https-cc/work/local-key-router/logs/usage.jsonl
```

## 模型列表

```bash
curl http://127.0.0.1:8787/v1/models \
  -H 'Authorization: Bearer sk-local-router-dev'
```

## Chat Completions

```bash
curl http://127.0.0.1:8787/v1/chat/completions \
  -H 'Authorization: Bearer sk-local-router-dev' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "hello"}]
  }'
```

## Responses 兼容入口

```bash
curl http://127.0.0.1:8787/v1/responses \
  -H 'Authorization: Bearer sk-local-router-dev' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-4o-mini",
    "input": "hello"
  }'
```

## 配置

上游配置在 `config/providers.yaml`。当前按文件顺序尝试；某个上游连续失败 3 次后，会熔断 10 分钟。

这是本地自用项目，配置文件按要求明文保存上游地址和 key。不要把这个目录上传到公开仓库。
# local-key-router-
