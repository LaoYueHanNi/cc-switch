//! Claude Desktop token 预算估算请求（count 请求）拦截
//!
//! Claude Desktop 会在发起真实对话前，发送 `max_tokens:1` + 单条 `count` 哨兵消息、
//! 并携带完整 tools/system 的请求，让模型"数一下"当前上下文占用多少 input token，
//! 用于预算 / 上下文窗口管理。本模块在 CC Switch 本地代理层识别这类请求并直接
//! 用启发式估算返回 `usage.input_tokens`，不转发上游——避免不理解 count 语义的
//! 上游（如 DeepSeek）把计数请求当真推理，浪费真实 token 与延迟。
//!
//! 仅由 Claude Desktop 入口（AppType::ClaudeDesktop）调用，不影响 claude CLI / codex。

use axum::body::Body;
use axum::http::{header, HeaderValue, Response, StatusCode};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

const UNSUPPORTED_CACHE_TTL: Duration = Duration::from_secs(300);

static COUNT_TOKENS_UNSUPPORTED: OnceLock<RwLock<HashMap<String, Instant>>> = OnceLock::new();

fn unsupported_cache() -> &'static RwLock<HashMap<String, Instant>> {
    COUNT_TOKENS_UNSUPPORTED.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 检查供应商是否被标记为不支持 count_tokens（TTL 内有效）。
pub fn is_count_tokens_unsupported(provider_id: &str) -> bool {
    let cache = unsupported_cache().read().unwrap();
    cache
        .get(provider_id)
        .is_some_and(|marked_at| marked_at.elapsed() < UNSUPPORTED_CACHE_TTL)
}

/// 标记供应商不支持 count_tokens，后续请求直接走 fallback。
pub fn mark_count_tokens_unsupported(provider_id: &str) {
    let mut cache = unsupported_cache().write().unwrap();
    cache.insert(provider_id.to_string(), Instant::now());
    log::info!("[count_tokens] 供应商 {provider_id} 不支持 count_tokens，已缓存 {UNSUPPORTED_CACHE_TTL:?}");
}

/// 环境变量开关：设为 1/true/yes/on 时关闭拦截（默认开启）。
fn count_intercept_disabled() -> bool {
    std::env::var("CC_SWITCH_DISABLE_COUNT_INTERCEPT")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            value == "1" || value == "true" || value == "yes" || value == "on"
        })
        .unwrap_or(false)
}

/// 识别 token 预算计数请求。
///
/// Claude Desktop 的预算机制会把 prompt 的每一段（system 提示词、CLAUDE.md、
/// Memory 索引、模型介绍等）分别作为单条 user 消息、以 `max_tokens == 1` 发出，
/// 让模型"只数一下这段占多少 input token"。日志观察到的形态有：
/// ① `max_tokens:1` + 单条 `content == "count"` + 完整 tools；
/// ② `max_tokens:1` + 单条真实上下文片段（无 tools / system）。
/// 两类请求的共同信号是 `max_tokens == 1`（几乎零生成，仅计数）——
/// 真实对话请求的 max_tokens 不可能是 1。此处只据此判定，避免漏拦截；
/// 是否仅限 Claude Desktop 由调用处（handle_messages_for_app）保证。
pub fn is_count_request(body: &Value) -> bool {
    body.get("max_tokens").and_then(Value::as_u64) == Some(1)
        && body
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|msgs| msgs.len() == 1)
}

/// 是否 CJK 字符（中/日/韩/全角等），按 1 token/字 估算。
fn is_cjk_char(c: char) -> bool {
    matches!(
        c as u32,
        0x3400..=0x4DBF   // CJK 扩展 A
        | 0x4E00..=0x9FFF // CJK 统一表意
        | 0x3040..=0x30FF // 日文假名
        | 0xAC00..=0xD7AF // 韩文
        | 0xF900..=0xFAFF // CJK 兼容表意
        | 0xFF00..=0xFFEF // 全角
    )
}

/// 提取请求中模型实际会看到的文本内容。
/// 只提取 system / messages content / tools 定义，跳过 API 参数字段
/// （max_tokens, model 等）和 JSON 结构字符——它们不会进入模型输入。
fn extract_model_input(body: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();

    collect_text_from_value(body.get("system"), &mut parts);

    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        for msg in messages {
            collect_text_from_value(msg.get("content"), &mut parts);
        }
    }

    if let Some(tools) = body.get("tools").and_then(Value::as_array) {
        for tool in tools {
            if let Some(name) = tool.get("name").and_then(Value::as_str) {
                parts.push(name.to_string());
            }
            if let Some(desc) = tool.get("description").and_then(Value::as_str) {
                parts.push(desc.to_string());
            }
            if let Some(schema) = tool.get("input_schema") {
                parts.push(serde_json::to_string(schema).unwrap_or_default());
            }
        }
    }

    parts.join(" ")
}

fn collect_text_from_value(value: Option<&Value>, parts: &mut Vec<String>) {
    let Some(value) = value else { return };
    match value {
        Value::String(s) => parts.push(s.clone()),
        Value::Array(arr) => {
            for item in arr {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    parts.push(text.to_string());
                } else {
                    collect_text_from_value(Some(item), parts);
                }
            }
        }
        Value::Object(obj) => {
            if let Some(text) = obj.get("text").and_then(Value::as_str) {
                parts.push(text.to_string());
            }
        }
        _ => {}
    }
}

/// 轻量启发式 token 估算：CJK 字符约 1 token/字，其余约 3 字符/token。
/// 用于对模型实际输入文本估算 input_tokens。
pub fn estimate_tokens(text: &str) -> u64 {
    let mut cjk = 0u64;
    let mut other = 0u64;
    for c in text.chars() {
        if is_cjk_char(c) {
            cjk += 1;
        } else {
            other += 1;
        }
    }
    cjk + (other + 2) / 3
}

/// 构造最小 Claude Messages 非流式响应。
/// usage 字段名对齐 `TokenUsage::from_claude_response`（proxy/usage/parser.rs）。
fn build_count_response(body: &Value, input_tokens: u64) -> Value {
    json!({
        "id": format!("msg_{}", uuid::Uuid::new_v4().simple()),
        "type": "message",
        "role": "assistant",
        "model": body.get("model").and_then(Value::as_str).unwrap_or(""),
        "content": [{"type": "text", "text": ""}],
        "stop_reason": "max_tokens",
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }
    })
}

fn build_http_response(value: Value) -> Response<Body> {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .body(Body::from(body))
        .expect("valid count response")
}

/// 顶层入口：识别 count 请求，本地估算并返回构造好的响应。
/// 非 count 请求或开关关闭时返回 None（正常转发）。
pub fn handle_count_intercept(body: &Value) -> Option<Response<Body>> {
    if count_intercept_disabled() || !is_count_request(body) {
        return None;
    }
    let text = extract_model_input(body);
    let tokens = estimate_tokens(&text);
    let message = format!(
        "[Claude Desktop] 拦截 count 请求成功，本地估算 input_tokens={tokens}, chars={}",
        text.chars().count()
    );
    // dev 构建直接打到终端（不受全局日志级别控制），便于确认拦截生效；
    // release 构建走 debug 日志写文件。
    if cfg!(debug_assertions) {
        println!("{message}");
    } else {
        log::debug!("{message}");
    }
    Some(build_http_response(build_count_response(body, tokens)))
}

/// count_tokens API 本地估算 fallback。
///
/// 当上游供应商不支持 `/v1/messages/count_tokens` 端点时，复用同样的启发式
/// 估算逻辑，但返回 Anthropic count_tokens 响应格式 `{ "input_tokens": N }`。
pub fn handle_count_tokens_fallback(body: &Value) -> Response<Body> {
    let text = extract_model_input(body);
    let tokens = estimate_tokens(&text);
    let message = format!(
        "[Claude Desktop] count_tokens 透传失败，本地估算 input_tokens={tokens}, chars={}",
        text.chars().count()
    );
    if cfg!(debug_assertions) {
        println!("{message}");
    } else {
        log::info!("{message}");
    }
    build_http_response(json!({ "input_tokens": tokens }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn count_body() -> Value {
        json!({
            "max_tokens": 1,
            "messages": [{"content": "count", "role": "user"}],
            "model": "claude-fable-5",
            "tools": [{"name": "Skill", "description": "invoke a skill"}]
        })
    }

    #[test]
    fn is_count_request_matches_canonical_form() {
        assert!(is_count_request(&count_body()));
    }

    #[test]
    fn is_count_request_matches_per_segment_form() {
        // 日志观察：Desktop 把 prompt 各段（system/CLAUDE.md 等）作为单条消息、
        // 以 max_tokens:1 发出做逐段计数——无 tools、content 为真实上下文片段。
        for content in [
            "永远使用简体中文回答，包括thinking的部分也需要使用中文，这是最高优先级。",
            "# Memory Index\n- [Git Bash 下执行 .bat 文件的正确方式](feedback_git_bash_cmd_bat.md)",
            "You are an interactive agent that helps users with software engineering tasks.",
            "This iteration of Claude is Claude Fable 5, the first model in Anthropic's new Claude 5 family.",
        ] {
            let body = json!({
                "max_tokens": 1,
                "messages": [{"content": content, "role": "user"}],
                "metadata": {"user_id": "{\"device_id\":\"abc\"}"},
                "model": "claude-fable-5"
            });
            assert!(is_count_request(&body), "content={content}");
        }
    }

    #[test]
    fn is_count_request_rejects_non_count_forms() {
        // max_tokens 非 1（真实对话）
        let mut body = count_body();
        body["max_tokens"] = json!(8192);
        assert!(!is_count_request(&body));

        // 多消息
        let mut body = count_body();
        body["messages"] = json!([
            {"content": "hi", "role": "user"},
            {"content": "count", "role": "user"}
        ]);
        assert!(!is_count_request(&body));

        // 无 max_tokens
        let mut body = count_body();
        body.as_object_mut().unwrap().remove("max_tokens");
        assert!(!is_count_request(&body));

        // 无 messages
        let mut body = count_body();
        body.as_object_mut().unwrap().remove("messages");
        assert!(!is_count_request(&body));

        // 空 body
        assert!(!is_count_request(&json!({})));
    }

    #[test]
    fn estimate_tokens_is_reasonable_and_stable() {
        // 纯英文：约 3 字符/token
        let english = "invoke a skill that is packaged for a particular task";
        let e = estimate_tokens(english);
        assert!(e > 0 && e <= english.chars().count() as u64);

        // 纯中文：约 1 token/字
        let chinese = "这是一个用于测试的中文句子";
        assert_eq!(estimate_tokens(chinese), chinese.chars().count() as u64);

        // 中英混合：介于两者之间
        let mixed = "invoke skill 用于特定任务 deploy steps";
        let m = estimate_tokens(mixed);
        assert!(m > 0);

        // 空串
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn extract_model_input_captures_system_messages_tools() {
        let body = json!({
            "system": "You are a helpful assistant.",
            "messages": [{"content": "Hello", "role": "user"}],
            "tools": [{
                "name": "Skill",
                "description": "invoke a skill",
                "input_schema": {"type": "object", "properties": {}}
            }],
            "max_tokens": 1,
            "model": "claude-fable-5"
        });
        let text = extract_model_input(&body);
        assert!(text.contains("You are a helpful assistant."));
        assert!(text.contains("Hello"));
        assert!(text.contains("Skill"));
        assert!(text.contains("invoke a skill"));
        assert!(!text.contains("claude-fable-5"));
        assert!(!text.contains("max_tokens"));
    }

    #[test]
    fn build_count_response_has_expected_shape() {
        let response = build_count_response(&count_body(), 1234);
        assert_eq!(response["type"], json!("message"));
        assert_eq!(response["role"], json!("assistant"));
        assert_eq!(response["stop_reason"], json!("max_tokens"));
        assert_eq!(response["model"], json!("claude-fable-5"));
        assert!(response["id"].as_str().unwrap().starts_with("msg_"));
        assert_eq!(response["usage"]["input_tokens"], json!(1234));
        assert_eq!(response["usage"]["output_tokens"], json!(0));
        assert_eq!(response["usage"]["cache_read_input_tokens"], json!(0));
        assert_eq!(response["usage"]["cache_creation_input_tokens"], json!(0));
    }

    #[test]
    #[serial]
    fn handle_count_intercept_only_intercepts_count_requests() {
        // 隔离并发测试可能设置的 CC_SWITCH_DISABLE_COUNT_INTERCEPT（并行测试共享进程 env）
        let previous = std::env::var_os("CC_SWITCH_DISABLE_COUNT_INTERCEPT");
        std::env::remove_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT");

        // count 请求 → Some(Response)
        let response = handle_count_intercept(&count_body());
        assert!(response.is_some());
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );

        // 非 count 请求 → None（不拦截）
        let normal = json!({
            "max_tokens": 1000,
            "messages": [{"content": "hello", "role": "user"}],
            "model": "claude-sonnet-5"
        });
        assert!(handle_count_intercept(&normal).is_none());

        match previous {
            Some(value) => std::env::set_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT", value),
            None => std::env::remove_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT"),
        }
    }

    #[test]
    #[serial]
    fn count_intercept_disabled_via_env() {
        // 开关关闭时 count 请求也返回 None
        let previous = std::env::var_os("CC_SWITCH_DISABLE_COUNT_INTERCEPT");
        std::env::set_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT", "1");
        assert!(handle_count_intercept(&count_body()).is_none());
        match previous {
            Some(value) => std::env::set_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT", value),
            None => std::env::remove_var("CC_SWITCH_DISABLE_COUNT_INTERCEPT"),
        }
    }

    #[test]
    fn handle_count_tokens_fallback_returns_count_tokens_format() {
        let response = handle_count_tokens_fallback(&count_body());
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }
}
