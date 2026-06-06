//! 代理服务相关的 Tauri 命令
//!
//! 提供前端调用的 API 接口

use crate::error::AppError;
use crate::proxy::types::*;
use crate::proxy::{CircuitBreakerConfig, CircuitBreakerStats};
use crate::store::AppState;

/// 启动代理服务器（仅启动服务，不接管 Live 配置）
#[tauri::command]
pub async fn start_proxy_server(
    state: tauri::State<'_, AppState>,
) -> Result<ProxyServerInfo, String> {
    state.proxy_service.start().await
}

/// 停止代理服务器（仅停止服务，不恢复/清理 Live 接管状态）
#[tauri::command]
pub async fn stop_proxy_server(state: tauri::State<'_, AppState>) -> Result<(), String> {
    let takeover = state.proxy_service.get_takeover_status().await?;
    if takeover.claude
        || takeover.codex
        || takeover.gemini
        || takeover.opencode
        || takeover.openclaw
    {
        return Err(
            "仍有应用处于代理接管状态，请先在设置中关闭对应应用接管后再停止本地路由。".to_string(),
        );
    }

    state.proxy_service.stop().await
}

/// 停止代理服务器（恢复 Live 配置）
#[tauri::command]
pub async fn stop_proxy_with_restore(state: tauri::State<'_, AppState>) -> Result<(), String> {
    state.proxy_service.stop_with_restore().await
}

/// 获取各应用接管状态
#[tauri::command]
pub async fn get_proxy_takeover_status(
    state: tauri::State<'_, AppState>,
) -> Result<ProxyTakeoverStatus, String> {
    state.proxy_service.get_takeover_status().await
}

/// 为指定应用开启/关闭接管
#[tauri::command]
pub async fn set_proxy_takeover_for_app(
    state: tauri::State<'_, AppState>,
    app_type: String,
    enabled: bool,
) -> Result<(), String> {
    state
        .proxy_service
        .set_takeover_for_app(&app_type, enabled)
        .await
}

/// 获取代理服务器状态
#[tauri::command]
pub async fn get_proxy_status(state: tauri::State<'_, AppState>) -> Result<ProxyStatus, String> {
    state.proxy_service.get_status().await
}

/// 获取代理配置
#[tauri::command]
pub async fn get_proxy_config(state: tauri::State<'_, AppState>) -> Result<ProxyConfig, String> {
    state.proxy_service.get_config().await
}

/// 更新代理配置
#[tauri::command]
pub async fn update_proxy_config(
    state: tauri::State<'_, AppState>,
    config: ProxyConfig,
) -> Result<(), String> {
    state.proxy_service.update_config(&config).await
}

// ==================== Global & Per-App Config ====================

/// 获取全局代理配置
///
/// 返回统一的全局配置字段（代理开关、监听地址、端口、日志开关）
#[tauri::command]
pub async fn get_global_proxy_config(
    state: tauri::State<'_, AppState>,
) -> Result<GlobalProxyConfig, String> {
    let db = &state.db;
    db.get_global_proxy_config()
        .await
        .map_err(|e| e.to_string())
}

/// 更新全局代理配置
///
/// 更新统一的全局配置字段，会同时更新三行（claude/codex/gemini）
#[tauri::command]
pub async fn update_global_proxy_config(
    state: tauri::State<'_, AppState>,
    config: GlobalProxyConfig,
) -> Result<(), String> {
    let db = &state.db;
    db.update_global_proxy_config(config)
        .await
        .map_err(|e| e.to_string())
}

/// 获取指定应用的代理配置
///
/// 返回应用级配置（enabled、auto_failover、超时、熔断器等）
#[tauri::command]
pub async fn get_proxy_config_for_app(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<AppProxyConfig, String> {
    let db = &state.db;
    db.get_proxy_config_for_app(&app_type)
        .await
        .map_err(|e| e.to_string())
}

/// 更新指定应用的代理配置
///
/// 更新应用级配置（enabled、auto_failover、超时、熔断器等）
#[tauri::command]
pub async fn update_proxy_config_for_app(
    state: tauri::State<'_, AppState>,
    config: AppProxyConfig,
) -> Result<(), String> {
    let db = &state.db;
    let app_type = config.app_type.clone();
    let circuit_config = CircuitBreakerConfig::from(&config);

    db.update_proxy_config_for_app(config)
        .await
        .map_err(|e| e.to_string())?;

    state
        .proxy_service
        .update_circuit_breaker_config_for_app(&app_type, circuit_config)
        .await
}

async fn get_default_cost_multiplier_internal(
    state: &AppState,
    app_type: &str,
) -> Result<String, AppError> {
    let db = &state.db;
    db.get_default_cost_multiplier(app_type).await
}

#[cfg_attr(not(feature = "test-hooks"), doc(hidden))]
pub async fn get_default_cost_multiplier_test_hook(
    state: &AppState,
    app_type: &str,
) -> Result<String, AppError> {
    get_default_cost_multiplier_internal(state, app_type).await
}

/// 获取默认成本倍率
#[tauri::command]
pub async fn get_default_cost_multiplier(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<String, String> {
    get_default_cost_multiplier_internal(&state, &app_type)
        .await
        .map_err(|e| e.to_string())
}

async fn set_default_cost_multiplier_internal(
    state: &AppState,
    app_type: &str,
    value: &str,
) -> Result<(), AppError> {
    let db = &state.db;
    db.set_default_cost_multiplier(app_type, value).await
}

#[cfg_attr(not(feature = "test-hooks"), doc(hidden))]
pub async fn set_default_cost_multiplier_test_hook(
    state: &AppState,
    app_type: &str,
    value: &str,
) -> Result<(), AppError> {
    set_default_cost_multiplier_internal(state, app_type, value).await
}

/// 设置默认成本倍率
#[tauri::command]
pub async fn set_default_cost_multiplier(
    state: tauri::State<'_, AppState>,
    app_type: String,
    value: String,
) -> Result<(), String> {
    set_default_cost_multiplier_internal(&state, &app_type, &value)
        .await
        .map_err(|e| e.to_string())
}

async fn get_pricing_model_source_internal(
    state: &AppState,
    app_type: &str,
) -> Result<String, AppError> {
    let db = &state.db;
    db.get_pricing_model_source(app_type).await
}

#[cfg_attr(not(feature = "test-hooks"), doc(hidden))]
pub async fn get_pricing_model_source_test_hook(
    state: &AppState,
    app_type: &str,
) -> Result<String, AppError> {
    get_pricing_model_source_internal(state, app_type).await
}

/// 获取计费模式来源
#[tauri::command]
pub async fn get_pricing_model_source(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<String, String> {
    get_pricing_model_source_internal(&state, &app_type)
        .await
        .map_err(|e| e.to_string())
}

async fn set_pricing_model_source_internal(
    state: &AppState,
    app_type: &str,
    value: &str,
) -> Result<(), AppError> {
    let db = &state.db;
    db.set_pricing_model_source(app_type, value).await
}

#[cfg_attr(not(feature = "test-hooks"), doc(hidden))]
pub async fn set_pricing_model_source_test_hook(
    state: &AppState,
    app_type: &str,
    value: &str,
) -> Result<(), AppError> {
    set_pricing_model_source_internal(state, app_type, value).await
}

/// 设置计费模式来源
#[tauri::command]
pub async fn set_pricing_model_source(
    state: tauri::State<'_, AppState>,
    app_type: String,
    value: String,
) -> Result<(), String> {
    set_pricing_model_source_internal(&state, &app_type, &value)
        .await
        .map_err(|e| e.to_string())
}

/// 检查代理服务器是否正在运行
#[tauri::command]
pub async fn is_proxy_running(state: tauri::State<'_, AppState>) -> Result<bool, String> {
    Ok(state.proxy_service.is_running().await)
}

/// 检查是否处于 Live 接管模式
#[tauri::command]
pub async fn is_live_takeover_active(state: tauri::State<'_, AppState>) -> Result<bool, String> {
    state.proxy_service.is_takeover_active().await
}

/// 代理模式下切换供应商（热切换）
#[tauri::command]
pub async fn switch_proxy_provider(
    state: tauri::State<'_, AppState>,
    app_type: String,
    provider_id: String,
) -> Result<(), String> {
    // Block official providers during proxy takeover
    let provider = state
        .db
        .get_provider_by_id(&provider_id, &app_type)
        .map_err(|e| format!("读取供应商失败: {e}"))?
        .ok_or_else(|| format!("供应商不存在: {provider_id}"))?;
    if provider.category.as_deref() == Some("official") {
        return Err(
            "代理接管模式下不能切换到官方供应商 (Cannot switch to official provider during proxy takeover)"
                .to_string(),
        );
    }

    state
        .proxy_service
        .switch_proxy_target(&app_type, &provider_id)
        .await
}

// ==================== 故障转移相关命令 ====================

/// 获取供应商健康状态
#[tauri::command]
pub async fn get_provider_health(
    state: tauri::State<'_, AppState>,
    provider_id: String,
    app_type: String,
) -> Result<ProviderHealth, String> {
    let db = &state.db;
    db.get_provider_health(&provider_id, &app_type)
        .await
        .map_err(|e| e.to_string())
}

/// 重置熔断器
///
/// 重置后会检查是否应该切回队列中优先级更高的供应商：
/// 1. 检查自动故障转移是否开启
/// 2. 如果恢复的供应商在队列中优先级更高（queue_order 更小），则自动切换
#[tauri::command]
pub async fn reset_circuit_breaker(
    app_handle: tauri::AppHandle,
    state: tauri::State<'_, AppState>,
    provider_id: String,
    app_type: String,
) -> Result<(), String> {
    // 1. 重置数据库健康状态
    let db = &state.db;
    db.update_provider_health(&provider_id, &app_type, true, None)
        .await
        .map_err(|e| e.to_string())?;

    // 2. 如果代理正在运行，重置内存中的熔断器状态
    state
        .proxy_service
        .reset_provider_circuit_breaker(&provider_id, &app_type)
        .await?;

    // 3. 检查是否应该切回优先级更高的供应商（从 proxy_config 表读取）
    // 只有当该应用已被代理接管（enabled=true）且开启了自动故障转移时才执行
    let (app_enabled, auto_failover_enabled) = match db.get_proxy_config_for_app(&app_type).await {
        Ok(config) => (config.enabled, config.auto_failover_enabled),
        Err(e) => {
            log::error!("[{app_type}] Failed to read proxy_config: {e}, defaulting to disabled");
            (false, false)
        }
    };

    if app_enabled && auto_failover_enabled && state.proxy_service.is_running().await {
        // 获取当前供应商 ID
        let current_id = db
            .get_current_provider(&app_type)
            .map_err(|e| e.to_string())?;

        if let Some(current_id) = current_id {
            // 获取故障转移队列
            let queue = db
                .get_failover_queue(&app_type)
                .map_err(|e| e.to_string())?;

            // 找到恢复的供应商和当前供应商在队列中的位置（使用 sort_index）
            let restored_order = queue
                .iter()
                .find(|item| item.provider_id == provider_id)
                .and_then(|item| item.sort_index);

            let current_order = queue
                .iter()
                .find(|item| item.provider_id == current_id)
                .and_then(|item| item.sort_index);

            // 如果恢复的供应商优先级更高（sort_index 更小），则切换
            if let (Some(restored), Some(current)) = (restored_order, current_order) {
                if restored < current {
                    log::info!(
                        "[Recovery] 供应商 {provider_id} 已恢复且优先级更高 (P{restored} vs P{current})，自动切换"
                    );

                    // 获取供应商名称用于日志和事件
                    let provider_name = db
                        .get_all_providers(&app_type)
                        .ok()
                        .and_then(|providers| providers.get(&provider_id).map(|p| p.name.clone()))
                        .unwrap_or_else(|| provider_id.clone());

                    // 创建故障转移切换管理器并执行切换
                    let switch_manager =
                        crate::proxy::failover_switch::FailoverSwitchManager::new(db.clone());
                    if let Err(e) = switch_manager
                        .try_switch(Some(&app_handle), &app_type, &provider_id, &provider_name)
                        .await
                    {
                        log::error!("[Recovery] 自动切换失败: {e}");
                    }
                }
            }
        }
    }

    Ok(())
}

/// 获取熔断器配置
#[tauri::command]
pub async fn get_circuit_breaker_config(
    state: tauri::State<'_, AppState>,
) -> Result<CircuitBreakerConfig, String> {
    let db = &state.db;
    db.get_circuit_breaker_config()
        .await
        .map_err(|e| e.to_string())
}

/// 更新熔断器配置
#[tauri::command]
pub async fn update_circuit_breaker_config(
    state: tauri::State<'_, AppState>,
    config: CircuitBreakerConfig,
) -> Result<(), String> {
    let db = &state.db;

    // 1. 更新数据库配置
    db.update_circuit_breaker_config(&config)
        .await
        .map_err(|e| e.to_string())?;

    // 2. 如果代理正在运行，热更新内存中的熔断器配置
    state
        .proxy_service
        .update_circuit_breaker_configs(config)
        .await?;

    Ok(())
}

/// 获取熔断器统计信息（仅当代理服务器运行时）
#[tauri::command]
pub async fn get_circuit_breaker_stats(
    state: tauri::State<'_, AppState>,
    provider_id: String,
    app_type: String,
) -> Result<Option<CircuitBreakerStats>, String> {
    // 这个功能需要访问运行中的代理服务器的内存状态
    // 目前先返回 None，后续可以通过 ProxyService 暴露接口来实现
    let _ = (state, provider_id, app_type);
    Ok(None)
}

// =============================================================================
// 多 Provider 轮询模式（阶段 2 Tauri 命令）
// =============================================================================

/// 读多 Provider 配置（per-app）
#[tauri::command]
pub async fn get_multi_provider_config(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<MultiProviderConfig, String> {
    let cfg = state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map_err(|e| e.to_string())?;
    Ok(MultiProviderConfig {
        multi_provider_enabled: cfg.multi_provider_enabled,
        pool_strategy: cfg.pool_strategy,
        session_ttl_seconds: cfg.session_ttl_seconds,
        session_ttl_sliding: cfg.session_ttl_sliding,
        pool_include_official: cfg.pool_include_official,
    })
}

/// 复合命令：一次事务写多 Provider 模式
///
/// - 校验 TTL 60..=86400
/// - 校验 pool_strategy ∈ 合法枚举
/// - 开启时强制 `auto_failover_enabled = true`（多 Provider 强依赖）
/// - 关闭时不动 auto_failover（用户可能想保留故障队列）
#[tauri::command]
pub async fn set_multi_provider_config(
    state: tauri::State<'_, AppState>,
    app_type: String,
    config: MultiProviderConfig,
) -> Result<(), String> {
    // 1. 校验 TTL
    if !(60..=86400).contains(&config.session_ttl_seconds) {
        return Err(format!(
            "session_ttl_seconds 必须在 60..=86400 之间，收到 {}",
            config.session_ttl_seconds
        ));
    }
    // 2. 校验 strategy
    let _ = crate::proxy::multi_provider::PoolStrategy::from_db_str(&config.pool_strategy);

    // 3. 读现状
    let mut cfg = state
        .db
        .get_proxy_config_for_app(&app_type)
        .await
        .map_err(|e| e.to_string())?;

    // 4. 应用变更
    cfg.multi_provider_enabled = config.multi_provider_enabled;
    cfg.pool_strategy = config.pool_strategy;
    cfg.session_ttl_seconds = config.session_ttl_seconds;
    cfg.session_ttl_sliding = config.session_ttl_sliding;
    cfg.pool_include_official = config.pool_include_official;

    // 5. 强联动：开启 multi 时必开 auto_failover；关闭时不联动
    if config.multi_provider_enabled && !cfg.auto_failover_enabled {
        log::info!(
            "[{app_type}] [MP-LINK] multi_provider_enabled=true 触发 auto_failover_enabled=true"
        );
        cfg.auto_failover_enabled = true;
    }

    // 6. 守卫：开启时要求已 takeover（与 v3.15.0 guardrail 一致）
    if config.multi_provider_enabled && !cfg.enabled {
        return Err("需要先启用该应用的代理接管，再开启多 Provider 轮询".to_string());
    }

    // 7. 关闭时清空 session_bindings（避免悬挂绑定）
    if !config.multi_provider_enabled {
        let n = state
            .db
            .clear_session_bindings_for_app(&app_type)
            .map_err(|e| e.to_string())?;
        if n > 0 {
            log::info!("[{app_type}] [MP-CLEAR] 关闭多 Provider 模式，清除 {n} 条 session bindings");
        }
    }

    state
        .db
        .update_proxy_config_for_app(cfg)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// 读轮询池成员（pool_enabled=1 的 provider）
#[tauri::command]
pub async fn get_provider_pool(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<ProviderPoolItem>, String> {
    let all = state
        .db
        .get_all_providers(&app_type)
        .map_err(|e| e.to_string())?;
    let mut items: Vec<ProviderPoolItem> = all
        .into_values()
        .filter(|p| p.pool_enabled)
        .map(|p| ProviderPoolItem {
            provider_id: p.id.clone(),
            provider_name: p.name.clone(),
            app_type: app_type.clone(),
            weight: p.pool_weight,
            pool_enabled: true,
            sort_index: p.sort_index.unwrap_or(0),
            in_failover_queue: p.in_failover_queue,
        })
        .collect();
    items.sort_by_key(|i| i.sort_index);
    Ok(items)
}

/// 批量更新轮询池成员
///
/// 双源真相守护：pool_enabled 和 in_failover_queue 强一致
/// （多 Provider 池成员 ⊆ 故障队列）
#[tauri::command]
pub async fn set_provider_pool(
    state: tauri::State<'_, AppState>,
    app_type: String,
    items: Vec<ProviderPoolItem>,
) -> Result<(), String> {
    // 1. 读所有 provider
    let mut all = state
        .db
        .get_all_providers(&app_type)
        .map_err(|e| e.to_string())?;

    // 2. 算出 pool_enabled=true 的 id 集合
    let pool_ids: std::collections::HashSet<String> =
        items.iter().map(|i| i.provider_id.clone()).collect();

    // 3. 写回：每个 provider 的 pool_weight + pool_enabled
    for item in &items {
        let Some(p) = all.get_mut(&item.provider_id) else {
            return Err(format!("provider {} 不存在", item.provider_id));
        };
        // 池成员必须同时在故障队列里（v3.15.0 guardrail 强约束）
        p.pool_weight = item.weight.max(1);
        p.pool_enabled = true;
        p.in_failover_queue = true; // 双源同步
        p.sort_index = Some(item.sort_index);
    }
    // 池外的 provider：pool_enabled=false，**不动** in_failover_queue（用户可能手动配的）
    for p in all.values_mut() {
        if !pool_ids.contains(&p.id) {
            p.pool_enabled = false;
        }
    }

    // 4. 事务写回
    for p in all.values() {
        state
            .db
            .save_provider(&app_type, p)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// 列某 app 的所有 session 绑定（调试/UI 展示）
#[tauri::command]
pub async fn get_session_bindings(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<Vec<crate::proxy::multi_provider::SessionBinding>, String> {
    state
        .db
        .list_session_bindings_for_app(&app_type)
        .map_err(|e| e.to_string())
}

/// 清空某 app 的所有 session 绑定（紧急按钮 / 关闭 multi 时）
#[tauri::command]
pub async fn clear_session_bindings(
    state: tauri::State<'_, AppState>,
    app_type: String,
) -> Result<usize, String> {
    state
        .db
        .clear_session_bindings_for_app(&app_type)
        .map_err(|e| e.to_string())
}
