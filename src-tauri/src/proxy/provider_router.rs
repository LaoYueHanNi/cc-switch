//! 供应商路由器模块
//!
//! 负责选择和管理代理目标供应商，实现智能故障转移

use crate::app_config::AppType;
use crate::database::Database;
use crate::error::AppError;
use crate::provider::Provider;
use crate::proxy::circuit_breaker::{AllowResult, CircuitBreaker, CircuitBreakerConfig};
use crate::proxy::multi_provider;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// 供应商路由器
pub struct ProviderRouter {
    /// 数据库连接
    db: Arc<Database>,
    /// 熔断器管理器 - key 格式: "app_type:provider_id"
    circuit_breakers: Arc<RwLock<HashMap<String, Arc<CircuitBreaker>>>>,
    /// 轮询计数器（per-app AtomicU64，纯内存）
    pool_counter: Arc<multi_provider::PoolCounter>,
    /// 同 session 单飞锁：(app_type, session_id) -> Mutex
    /// 防并发同 session 选到不同 provider
    session_locks: Arc<RwLock<HashMap<(String, String), (Instant, Arc<tokio::sync::Mutex<()>>)>>>,
}

impl ProviderRouter {
    /// 创建新的供应商路由器
    pub fn new(db: Arc<Database>) -> Self {
        // 阶段 0 灰度开关：启动时一次性检查并日志记录
        if multi_provider::is_multi_provider_disabled() {
            log::info!(
                "[multi-provider] {} 检测到强制降级（=off），所有路由走旧路径",
                multi_provider::ENV_DISABLE_MULTI_PROVIDER
            );
        }
        Self {
            db,
            circuit_breakers: Arc::new(RwLock::new(HashMap::new())),
            pool_counter: Arc::new(multi_provider::PoolCounter::new()),
            session_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 阶段 0 占位：判断当前 app 是否启用多 Provider 模式
    ///
    /// 返回 `true` 时应走新路由（多 Provider + 轮询 + 故障队列）
    /// 返回 `false` 时走旧 `select_providers` 路径
    pub async fn is_multi_provider_enabled(&self, app_type: &str) -> bool {
        multi_provider::is_multi_provider_active(&self.db, app_type)
            .await
            .unwrap_or(false)
    }

    /// 顶层入口：选择可用 Provider（按 multi_provider_enabled 分流）
    ///
    /// - `multi_provider_enabled=false` ⇒ 委托 `select_providers_legacy`（0 行为变化）
    /// - `multi_provider_enabled=true`  ⇒ 三层决策（session 命中 / 池选 primary / 失败返 Err）
    ///
    /// 返回值永远是 `Vec<Provider>`（按 primary → fallbacks 顺序），
    /// 调用方（handler_context）只需 `.first()`，无需感知 Session 粘性细节。
    pub async fn select_providers(
        &self,
        app_type: &str,
        session_id: Option<&str>,
    ) -> Result<Vec<Provider>, AppError> {
        // 1. 灰度开关 / DB 字段判定
        if !self.is_multi_provider_enabled(app_type).await {
            return self.select_providers_legacy(app_type).await;
        }

        // 2. 多 Provider 模式：三层决策
        // 池空/全熔断时不降级 legacy——legacy 在相同配置下也会失败，徒增开销。
        self.select_providers_multi(app_type, session_id).await
    }

    /// 旧 select_providers 实现，0 行为变化（被 select_providers 委托）
    async fn select_providers_legacy(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
        let mut result = Vec::new();
        let mut total_providers = 0usize;
        let mut circuit_open_count = 0usize;

        // 检查该应用的自动故障转移开关是否开启（从 proxy_config 表读取）
        let auto_failover_enabled = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(config) => config.auto_failover_enabled,
            Err(e) => {
                log::error!("[{app_type}] 读取 proxy_config 失败: {e}，默认禁用故障转移");
                false
            }
        };

        if auto_failover_enabled {
            // 故障转移开启：仅按队列顺序依次尝试（P1 → P2 → ...）
            let all_providers = self.db.get_all_providers(app_type)?;

            // 使用 DAO 返回的排序结果，确保和前端展示一致
            let ordered_ids: Vec<String> = self
                .db
                .get_failover_queue(app_type)?
                .into_iter()
                .map(|item| item.provider_id)
                .collect();

            total_providers = ordered_ids.len();

            for provider_id in ordered_ids {
                let Some(provider) = all_providers.get(&provider_id).cloned() else {
                    continue;
                };

                let circuit_key = format!("{app_type}:{}", provider.id);
                let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

                if breaker.is_available().await {
                    result.push(provider);
                } else {
                    circuit_open_count += 1;
                }
            }
        } else {
            // 故障转移关闭：仅使用当前供应商，跳过熔断器检查
            let current_id = AppType::from_str(app_type)
                .ok()
                .and_then(|app_enum| {
                    crate::settings::get_effective_current_provider(&self.db, &app_enum)
                        .ok()
                        .flatten()
                })
                .or_else(|| self.db.get_current_provider(app_type).ok().flatten());

            if let Some(current_id) = current_id {
                if let Some(current) = self.db.get_provider_by_id(&current_id, app_type)? {
                    total_providers = 1;
                    result.push(current);
                }
            }
        }

        if result.is_empty() {
            if total_providers > 0 && circuit_open_count == total_providers {
                log::warn!("[{app_type}] [FO-004] 所有供应商均已熔断");
                return Err(AppError::AllProvidersCircuitOpen);
            } else {
                log::warn!("[{app_type}] [FO-005] 未配置供应商");
                return Err(AppError::NoProvidersConfigured);
            }
        }

        Ok(result)
    }

    /// 多 Provider 模式：从 session_bindings 命中或轮询池选 primary
    async fn select_providers_multi(
        &self,
        app_type: &str,
        session_id: Option<&str>,
    ) -> Result<Vec<Provider>, AppError> {
        // 拿同 session 单飞锁，避免并发请求选到不同 provider
        let _guard = if let Some(sid) = session_id {
            Some(self.acquire_session_lock(app_type, sid).await)
        } else {
            None
        };

        // Layer 1: session_id 命中
        if let Some(sid) = session_id {
            if let Some(bound) = self.db.get_session_binding(sid).map_err(AppError::from)? {
                if bound.app_type == app_type
                    && bound.expires_at > multi_provider::unix_now()
                {
                    // 二次校验：绑定的 provider 仍存在 + 在池中 + 熔断器允许
                    if let Some(p) = self
                        .db
                        .get_provider_by_id(&bound.provider_id, app_type)
                        .map_err(AppError::from)?
                    {
                        if p.pool_enabled
                            && self
                                .is_circuit_available(app_type, &p.id)
                                .await
                        {
                            self.db.touch_session_binding(sid).map_err(AppError::from)?;
                            log::debug!(
                                "[{app_type}] [MP-HIT] session={sid} 命中绑定 provider={}",
                                p.id
                            );
                            // SessionBound 模式下，fallback 链是除 primary 外的池成员
                            let fallbacks = self.load_pool_fallbacks(app_type, &p.id).await?;
                            let mut chain = vec![p];
                            chain.extend(fallbacks);
                            return Ok(chain);
                        }
                        log::debug!(
                            "[{app_type}] [MP-STALE] session={sid} 绑定失效（provider={} pool_enabled={} circuit_open={}），重新选",
                            bound.provider_id,
                            p.pool_enabled,
                            !self.is_circuit_available(app_type, &p.id).await
                        );
                    } else {
                        log::debug!(
                            "[{app_type}] [MP-STALE] session={sid} 绑定的 provider={} 已不存在",
                            bound.provider_id
                        );
                    }
                    // 失效 → 删绑定
                    let _ = self.db.remove_session_binding(sid);
                }
            }
        }

        // Layer 2: 从轮询池选 primary
        let pool = self.load_pool(app_type).await?;
        if pool.is_empty() {
            log::warn!("[{app_type}] [MP-EMPTY] 多 Provider 模式开启但池为空");
            return Err(AppError::NoProvidersConfigured);
        }

        let cfg = self.db.get_proxy_config_for_app(app_type).await?;
        let strategy = multi_provider::PoolStrategy::from_db_str(&cfg.pool_strategy);
        let primary = self
            .pick_by_strategy(&pool, strategy, app_type)
            .await?;
        let fallbacks: Vec<Provider> = pool.into_iter().filter(|p| p.id != primary.id).collect();

        // Layer 3: 写 session 绑定（短期粘性）
        if let Some(sid) = session_id {
            self.db
                .upsert_session_binding(
                    sid,
                    app_type,
                    &primary.id,
                    cfg.session_ttl_seconds as i64,
                    cfg.session_ttl_sliding,
                )
                .map_err(AppError::from)?;
            log::debug!(
                "[{app_type}] [MP-BIND] session={sid} 绑定新 provider={} (ttl={}s, sliding={})",
                primary.id,
                cfg.session_ttl_seconds,
                cfg.session_ttl_sliding
            );
        }

        let mut chain = vec![primary];
        chain.extend(fallbacks);

        // 阶段 2 概率清理 session_locks（防内存累积）
        self.maybe_cleanup_session_locks().await;

        Ok(chain)
    }

    /// 加载轮询池（pool_enabled=1 且不在熔断 Open），按 sort_index 排序
    async fn load_pool(&self, app_type: &str) -> Result<Vec<Provider>, AppError> {
        let all = self.db.get_all_providers(app_type)?;
        let include_official = self.is_pool_include_official(app_type).await;
        let mut pool: Vec<Provider> = all
            .into_values()
            .filter(|p| p.pool_enabled)
            .filter(|p| {
                // 过滤 official：除非显式 opt-in
                p.category.as_deref() != Some("official") || include_official
            })
            .collect();
        // 按 sort_index 升序（与故障队列排序一致）
        pool.sort_by_key(|p| p.sort_index.unwrap_or(usize::MAX));
        Ok(pool)
    }

    /// 单元素池场景：仅 primary，无 fallbacks
    async fn load_pool_fallbacks(
        &self,
        app_type: &str,
        primary_id: &str,
    ) -> Result<Vec<Provider>, AppError> {
        let pool = self.load_pool(app_type).await?;
        Ok(pool.into_iter().filter(|p| p.id != primary_id).collect())
    }

    async fn is_circuit_available(&self, app_type: &str, provider_id: &str) -> bool {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.is_available().await
    }

    async fn is_pool_include_official(&self, app_type: &str) -> bool {
        self.db
            .get_proxy_config_for_app(app_type)
            .await
            .map(|c| c.pool_include_official)
            .unwrap_or(false)
    }

    /// 拿同 session 的单飞锁（首次访问记录创建时间，便于后续清理）
    async fn acquire_session_lock(
        &self,
        app_type: &str,
        session_id: &str,
    ) -> Arc<tokio::sync::Mutex<()>> {
        let key = (app_type.to_string(), session_id.to_string());
        {
            let map = self.session_locks.read().await;
            if let Some((_, m)) = map.get(&key) {
                return m.clone();
            }
        }
        let mut map = self.session_locks.write().await;
        map.entry(key)
            .or_insert_with(|| (Instant::now(), Arc::new(tokio::sync::Mutex::new(()))))
            .1
            .clone()
    }

    /// 概率清理：每次 select_providers_multi 完成后 1% 触发
    /// 删创建时间 > 10 分钟的锁（session 早过期了，留着没意义）
    async fn maybe_cleanup_session_locks(&self) {
        // 1% 概率
        if !self.should_cleanup_locks() {
            return;
        }
        let now = Instant::now();
        let stale_after = std::time::Duration::from_secs(600); // 10 分钟
        let mut to_remove: Vec<(String, String)> = Vec::new();
        {
            let map = self.session_locks.read().await;
            for (k, (created, _)) in map.iter() {
                if now.duration_since(*created) > stale_after {
                    to_remove.push(k.clone());
                }
            }
        }
        if to_remove.is_empty() {
            return;
        }
        let mut map = self.session_locks.write().await;
        let before = map.len();
        for k in to_remove {
            map.remove(&k);
        }
        log::debug!(
            "[multi-provider] session_locks 清理 {} 条（剩 {} 条）",
            before - map.len(),
            map.len()
        );
    }

    fn should_cleanup_locks(&self) -> bool {
        use std::sync::atomic::{AtomicU64, Ordering};
        thread_local! {
            static COUNTER: AtomicU64 = const { AtomicU64::new(0) };
        }
        COUNTER.with(|c| {
            let n = c.fetch_add(1, Ordering::Relaxed);
            n % 100 == 0 // 每 100 次调用触发 1 次（约 1%）
        })
    }

    /// 按策略从池中选 1 个 provider
    async fn pick_by_strategy(
        &self,
        pool: &[Provider],
        strategy: multi_provider::PoolStrategy,
        app_type: &str,
    ) -> Result<Provider, AppError> {
        if pool.is_empty() {
            return Err(AppError::NoProvidersConfigured);
        }
        match strategy {
            multi_provider::PoolStrategy::RoundRobin => {
                let idx = (self.pool_counter.next(app_type) as usize) % pool.len();
                Ok(pool[idx].clone())
            }
            multi_provider::PoolStrategy::WeightedRandom => {
                // 阶段 1 简化实现：按 weight 分桶，每个 bucket 内用 counter 轮询
                // 等同于"SWRR 的简化版"：保证 weight 比例但有抖动
                let total: i64 = pool.iter().map(|p| p.pool_weight.max(1)).sum();
                if total <= 0 {
                    let idx = (self.pool_counter.next(app_type) as usize) % pool.len();
                    return Ok(pool[idx].clone());
                }
                let counter = self.pool_counter.next(app_type);
                // 用 counter 映射到 [0, total)
                let target = (counter % total as u64) as i64;
                let mut acc: i64 = 0;
                for p in pool {
                    acc += p.pool_weight.max(1);
                    if target < acc {
                        return Ok(p.clone());
                    }
                }
                Ok(pool[pool.len() - 1].clone())
            }
            multi_provider::PoolStrategy::LeastFailures => {
                let mut best = &pool[0];
                let mut best_fail: u32 = u32::MAX;
                for p in pool {
                    let failures = self
                        .db
                        .get_provider_health(&p.id, app_type)
                        .await
                        .map(|h| h.consecutive_failures)
                        .unwrap_or(0);
                    if failures < best_fail {
                        best = p;
                        best_fail = failures;
                    }
                }
                Ok(best.clone())
            }
            multi_provider::PoolStrategy::LowestLatency => {
                // 阶段 1 简化：基于最近 N 条请求的平均 latency（无 p50 列）
                // 若 provider 无历史数据，视为 u32::MAX（最差）
                let mut best = &pool[0];
                let mut best_lat: u32 = u32::MAX;
                for p in pool {
                    let lat = self
                        .db
                        .get_recent_avg_latency_ms(&p.id, app_type)
                        .await
                        .unwrap_or(None)
                        .unwrap_or(u32::MAX);
                    if lat < best_lat {
                        best = p;
                        best_lat = lat;
                    }
                }
                Ok(best.clone())
            }
        }
    }

    /// 请求执行前获取熔断器“放行许可”
    ///
    /// - Closed：直接放行
    /// - Open：超时到达后切到 HalfOpen 并放行一次探测
    /// - HalfOpen：按限流规则放行探测
    ///
    /// 注意：调用方必须在请求结束后通过 `record_result()` 释放 HalfOpen 名额，
    /// 否则会导致该 Provider 长时间无法进入探测状态。
    pub async fn allow_provider_request(&self, provider_id: &str, app_type: &str) -> AllowResult {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.allow_request().await
    }

    /// 记录供应商请求结果
    pub async fn record_result(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
        success: bool,
        error_msg: Option<String>,
    ) -> Result<(), AppError> {
        // 1. 按应用独立获取熔断器配置
        let failure_threshold = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => app_config.circuit_failure_threshold,
            Err(_) => 5, // 默认值
        };

        // 2. 更新熔断器状态
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;

        if success {
            breaker.record_success(used_half_open_permit).await;
        } else {
            breaker.record_failure(used_half_open_permit).await;
        }

        // 3. 更新数据库健康状态（使用配置的阈值）
        self.db
            .update_provider_health_with_threshold(
                provider_id,
                app_type,
                success,
                error_msg.clone(),
                failure_threshold,
            )
            .await?;

        Ok(())
    }

    /// 重置熔断器（手动恢复）
    pub async fn reset_circuit_breaker(&self, circuit_key: &str) {
        let breakers = self.circuit_breakers.read().await;
        if let Some(breaker) = breakers.get(circuit_key) {
            breaker.reset().await;
        }
    }

    /// 重置指定供应商的熔断器
    pub async fn reset_provider_breaker(&self, provider_id: &str, app_type: &str) {
        let circuit_key = format!("{app_type}:{provider_id}");
        self.reset_circuit_breaker(&circuit_key).await;
    }

    /// 仅释放 HalfOpen permit，不影响健康统计（neutral 接口）
    ///
    /// 用于整流器等场景：请求结果不应计入 Provider 健康度，
    /// 但仍需释放占用的探测名额，避免 HalfOpen 状态卡死
    pub async fn release_permit_neutral(
        &self,
        provider_id: &str,
        app_type: &str,
        used_half_open_permit: bool,
    ) {
        if !used_half_open_permit {
            return;
        }
        let circuit_key = format!("{app_type}:{provider_id}");
        let breaker = self.get_or_create_circuit_breaker(&circuit_key).await;
        breaker.release_half_open_permit();
    }

    /// 更新所有熔断器的配置（热更新）
    pub async fn update_all_configs(&self, config: CircuitBreakerConfig) {
        let breakers = self.circuit_breakers.read().await;
        for breaker in breakers.values() {
            breaker.update_config(config.clone()).await;
        }
    }

    /// 更新指定应用已创建熔断器的配置（热更新）
    pub async fn update_app_configs(&self, app_type: &str, config: CircuitBreakerConfig) {
        let prefix = format!("{app_type}:");
        let breakers = self.circuit_breakers.read().await;
        for (key, breaker) in breakers.iter() {
            if key.starts_with(&prefix) {
                breaker.update_config(config.clone()).await;
            }
        }
    }

    /// 获取熔断器状态
    #[allow(dead_code)]
    pub async fn get_circuit_breaker_stats(
        &self,
        provider_id: &str,
        app_type: &str,
    ) -> Option<crate::proxy::circuit_breaker::CircuitBreakerStats> {
        let circuit_key = format!("{app_type}:{provider_id}");
        let breakers = self.circuit_breakers.read().await;

        if let Some(breaker) = breakers.get(&circuit_key) {
            Some(breaker.get_stats().await)
        } else {
            None
        }
    }

    /// 获取或创建熔断器
    async fn get_or_create_circuit_breaker(&self, key: &str) -> Arc<CircuitBreaker> {
        // 先尝试读锁获取
        {
            let breakers = self.circuit_breakers.read().await;
            if let Some(breaker) = breakers.get(key) {
                return breaker.clone();
            }
        }

        // 如果不存在，获取写锁创建
        let mut breakers = self.circuit_breakers.write().await;

        // 双重检查，防止竞争条件
        if let Some(breaker) = breakers.get(key) {
            return breaker.clone();
        }

        // 从 key 中提取 app_type (格式: "app_type:provider_id")
        let app_type = key.split(':').next().unwrap_or("claude");

        // 按应用独立读取熔断器配置
        let config = match self.db.get_proxy_config_for_app(app_type).await {
            Ok(app_config) => crate::proxy::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: app_config.circuit_failure_threshold,
                success_threshold: app_config.circuit_success_threshold,
                timeout_seconds: app_config.circuit_timeout_seconds as u64,
                error_rate_threshold: app_config.circuit_error_rate_threshold,
                min_requests: app_config.circuit_min_requests,
            },
            Err(_) => crate::proxy::circuit_breaker::CircuitBreakerConfig::default(),
        };

        let breaker = Arc::new(CircuitBreaker::new(config));
        breakers.insert(key.to_string(), breaker.clone());

        breaker
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;
    use serde_json::json;
    use serial_test::serial;
    use std::env;
    use tempfile::TempDir;

    struct TempHome {
        #[allow(dead_code)]
        dir: TempDir,
        original_home: Option<String>,
        original_userprofile: Option<String>,
        original_test_home: Option<String>,
    }

    impl TempHome {
        fn new() -> Self {
            let dir = TempDir::new().expect("failed to create temp home");
            let original_home = env::var("HOME").ok();
            let original_userprofile = env::var("USERPROFILE").ok();
            let original_test_home = env::var("CC_SWITCH_TEST_HOME").ok();

            env::set_var("HOME", dir.path());
            env::set_var("USERPROFILE", dir.path());
            env::set_var("CC_SWITCH_TEST_HOME", dir.path());
            crate::settings::reload_settings().expect("reload settings");

            Self {
                dir,
                original_home,
                original_userprofile,
                original_test_home,
            }
        }
    }

    impl Drop for TempHome {
        fn drop(&mut self) {
            match &self.original_home {
                Some(value) => env::set_var("HOME", value),
                None => env::remove_var("HOME"),
            }

            match &self.original_userprofile {
                Some(value) => env::set_var("USERPROFILE", value),
                None => env::remove_var("USERPROFILE"),
            }

            match &self.original_test_home {
                Some(value) => env::set_var("CC_SWITCH_TEST_HOME", value),
                None => env::remove_var("CC_SWITCH_TEST_HOME"),
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_provider_router_creation() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());
        let router = ProviderRouter::new(db);

        let breaker = router.get_or_create_circuit_breaker("claude:test").await;
        assert!(breaker.allow_request().await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_disabled_uses_current_provider() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude", None).await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_order_ignoring_current() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 设置 sort_index 来控制顺序：b=1, a=2
        let mut provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        provider_a.sort_index = Some(2);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        db.add_to_failover_queue("claude", "b").unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude", None).await.unwrap();

        assert_eq!(providers.len(), 2);
        // 故障转移开启时：仅按队列顺序选择（忽略当前供应商）
        assert_eq!(providers[0].id, "b");
        assert_eq!(providers[1].id, "a");
    }

    #[tokio::test]
    #[serial]
    async fn test_failover_enabled_uses_queue_only_even_if_current_not_in_queue() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let mut provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        provider_b.sort_index = Some(1);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        // 只把 b 加入故障转移队列（模拟“当前供应商不在队列里”的常见配置）
        db.add_to_failover_queue("claude", "b").unwrap();

        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude", None).await.unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "b");
    }

    #[tokio::test]
    #[serial]
    async fn test_select_providers_does_not_consume_half_open_permit() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);

        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();

        db.add_to_failover_queue("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        // 启用自动故障转移（使用新的 proxy_config API）
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        router
            .record_result("b", "claude", false, false, Some("fail".to_string()))
            .await
            .unwrap();

        let providers = router.select_providers("claude", None).await.unwrap();
        assert_eq!(providers.len(), 2);

        assert!(router.allow_provider_request("b", "claude").await.allowed);
    }

    #[tokio::test]
    #[serial]
    async fn test_release_permit_neutral_frees_half_open_slot() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 配置熔断器：1 次失败即熔断，0 秒超时立即进入 HalfOpen
        db.update_circuit_breaker_config(&CircuitBreakerConfig {
            failure_threshold: 1,
            timeout_seconds: 0,
            ..Default::default()
        })
        .await
        .unwrap();

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        db.save_provider("claude", &provider_a).unwrap();
        db.add_to_failover_queue("claude", "a").unwrap();

        // 启用自动故障转移
        let mut config = db.get_proxy_config_for_app("claude").await.unwrap();
        config.auto_failover_enabled = true;
        db.update_proxy_config_for_app(config).await.unwrap();

        let router = ProviderRouter::new(db.clone());

        // 触发熔断：1 次失败
        router
            .record_result("a", "claude", false, false, Some("fail".to_string()))
            .await
            .unwrap();

        // 第一次请求：获取 HalfOpen 探测名额
        let first = router.allow_provider_request("a", "claude").await;
        assert!(first.allowed);
        assert!(first.used_half_open_permit);

        // 第二次请求应被拒绝（名额已被占用）
        let second = router.allow_provider_request("a", "claude").await;
        assert!(!second.allowed);

        // 使用 release_permit_neutral 释放名额（不影响健康统计）
        router
            .release_permit_neutral("a", "claude", first.used_half_open_permit)
            .await;

        // 第三次请求应被允许（名额已释放）
        let third = router.allow_provider_request("a", "claude").await;
        assert!(third.allowed);
        assert!(third.used_half_open_permit);
    }

    // ========== 阶段 0 灰度开关回归测试 ==========

    /// 阶段 0 老行为：multi_provider_enabled 默认 false，不影响 select_providers
    #[tokio::test]
    #[serial]
    async fn test_multi_provider_default_is_false_and_select_providers_unchanged() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let provider_a =
            Provider::with_id("a".to_string(), "Provider A".to_string(), json!({}), None);
        let provider_b =
            Provider::with_id("b".to_string(), "Provider B".to_string(), json!({}), None);
        db.save_provider("claude", &provider_a).unwrap();
        db.save_provider("claude", &provider_b).unwrap();
        db.set_current_provider("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        // 1) 默认 config：multi_provider_enabled = false
        let cfg = db.get_proxy_config_for_app("claude").await.unwrap();
        assert!(
            !cfg.multi_provider_enabled,
            "默认 multi_provider_enabled 必须为 false"
        );
        assert_eq!(cfg.pool_strategy, "round_robin");
        assert_eq!(cfg.session_ttl_seconds, 3600);
        assert!(!cfg.session_ttl_sliding);
        assert!(!cfg.pool_include_official);

        // 2) Provider 默认字段
        let pa = db.get_provider_by_id("a", "claude").unwrap().unwrap();
        assert!(!pa.pool_enabled);
        assert_eq!(pa.pool_weight, 1);

        // 3) 旧 select_providers 行为不变（与 test_failover_disabled_uses_current_provider 一致）
        let router = ProviderRouter::new(db.clone());
        let providers = router.select_providers("claude", None).await.unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "a");
    }

    /// 阶段 0 灰度开关：环境变量强制关闭 → is_multi_provider_enabled 返回 false
    #[tokio::test]
    #[serial]
    async fn test_gray_flag_env_var_forces_off() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 显式设 multi_provider_enabled=true（模拟"用户在 UI 打开了"）
        let mut cfg = db.get_proxy_config_for_app("claude").await.unwrap();
        cfg.multi_provider_enabled = true;
        db.update_proxy_config_for_app(cfg).await.unwrap();

        // 不设环境变量时：按 DB 字段，启用
        let router = ProviderRouter::new(db.clone());
        assert!(router.is_multi_provider_enabled("claude").await);

        // 设环境变量 = off：应被强制降级
        let prev = env::var("CC_SWITCH_MULTI_PROVIDER").ok();
        env::set_var("CC_SWITCH_MULTI_PROVIDER", "off");
        // is_multi_provider_disabled 立刻生效（不依赖 ProviderRouter 实例）
        assert!(super::multi_provider::is_multi_provider_disabled());
        assert!(!router.is_multi_provider_enabled("claude").await);
        // 恢复
        match prev {
            Some(v) => env::set_var("CC_SWITCH_MULTI_PROVIDER", v),
            None => env::remove_var("CC_SWITCH_MULTI_PROVIDER"),
        }
        // 恢复后应重新读 DB
        assert!(router.is_multi_provider_enabled("claude").await);
    }

    // ========== 阶段 1 多 Provider 模式单元测试 ==========

    async fn enable_multi_provider(db: &Database, app: &str) {
        let mut cfg = db.get_proxy_config_for_app(app).await.unwrap();
        cfg.multi_provider_enabled = true;
        cfg.auto_failover_enabled = true; // 强依赖
        db.update_proxy_config_for_app(cfg).await.unwrap();
    }

    fn make_pool_provider(id: &str, sort_index: usize) -> Provider {
        let mut p = Provider::with_id(
            id.to_string(),
            format!("P-{id}"),
            json!({}),
            None,
        );
        p.sort_index = Some(sort_index);
        p.pool_enabled = true;
        p
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_session_hit_returns_bound() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let p1 = make_pool_provider("a", 1);
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());

        // 第一次：无 session ⇒ 从池子选 primary，写 binding
        let chain1 = router
            .select_providers("claude", Some("sess-1"))
            .await
            .unwrap();
        assert_eq!(chain1.len(), 2);
        let primary_id_1 = chain1[0].id.clone();
        let binding = db.get_session_binding("sess-1").unwrap().expect("bound");
        assert_eq!(binding.provider_id, primary_id_1);

        // 第二次：同 session ⇒ 命中绑定
        let chain2 = router
            .select_providers("claude", Some("sess-1"))
            .await
            .unwrap();
        assert_eq!(chain2[0].id, primary_id_1, "session 应命中同一 provider");
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_session_miss_picks_round_robin() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let p1 = make_pool_provider("a", 1);
        let p2 = make_pool_provider("b", 2);
        let p3 = make_pool_provider("c", 3);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        db.save_provider("claude", &p3).unwrap();
        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());

        // 三个新 session 应各拿到不同 primary（轮询）
        let c1 = router
            .select_providers("claude", Some("s1"))
            .await
            .unwrap();
        let c2 = router
            .select_providers("claude", Some("s2"))
            .await
            .unwrap();
        let c3 = router
            .select_providers("claude", Some("s3"))
            .await
            .unwrap();

        let ids: std::collections::HashSet<_> =
            [c1[0].id.clone(), c2[0].id.clone(), c3[0].id.clone()]
                .into_iter()
                .collect();
        assert_eq!(ids.len(), 3, "三个 session 应轮询到 3 个不同 provider");
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_pool_empty_returns_err() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        // 没有 pool_enabled 的 provider
        let mut p1 = Provider::with_id("a".into(), "A".into(), json!({}), None);
        p1.pool_enabled = false;
        db.save_provider("claude", &p1).unwrap();
        db.set_current_provider("claude", "a").unwrap();

        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());
        // 池空 ⇒ NoProvidersConfigured（不降级 legacy，legacy 同样会失败）
        let err = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap_err();
        assert!(matches!(err, AppError::NoProvidersConfigured));
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_session_expired_rebinds() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let p1 = make_pool_provider("a", 1);
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());
        let first = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        let first_id = first[0].id.clone();

        // 模拟过期：把 expires_at 改到 1 秒前
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE session_bindings SET expires_at = ?1 WHERE session_id = ?2",
                rusqlite::params![crate::proxy::multi_provider::unix_now() - 1, "s"],
            )
            .unwrap();
        }

        // 再次请求：过期 ⇒ 重新轮询 + 重新写 binding
        let _second = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        // 重新绑定后，request_count 必然 ≥ 1（说明已 UPSERT 触发）
        let binding = db.get_session_binding("s").unwrap().expect("bound again");
        assert!(
            binding.request_count >= 2,
            "重新绑定后 request_count 应增长（至少 2）"
        );
        // first_id 非空（说明上一轮选过 primary）
        assert!(!first_id.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_session_stale_when_provider_removed() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let p1 = make_pool_provider("a", 1);
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());
        // 把 sess 绑到 a
        router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        // 手动把 binding 改成已删除的 provider
        {
            let conn = db.conn.lock().unwrap();
            conn.execute(
                "UPDATE session_bindings SET provider_id = 'ghost' WHERE session_id = 's'",
                [],
            )
            .unwrap();
        }
        // 再次请求：绑定失效 ⇒ 重新选（fallback 链不含 ghost）
        let chain = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        for p in &chain {
            assert_ne!(p.id, "ghost", "ghost provider 不应出现在 chain");
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_official_excluded_by_default() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let mut p1 = make_pool_provider("a", 1);
        p1.category = Some("official".into());
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        enable_multi_provider(&db, "claude").await;

        let router = ProviderRouter::new(db.clone());
        let chain = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        // 仅 b 应在池里（a 是 official 且未 opt-in）
        assert_eq!(chain[0].id, "b");
        assert_eq!(chain.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_multi_provider_official_included_when_opt_in() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let mut p1 = make_pool_provider("a", 1);
        p1.category = Some("official".into());
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        enable_multi_provider(&db, "claude").await;
        // opt-in official
        let mut cfg = db.get_proxy_config_for_app("claude").await.unwrap();
        cfg.pool_include_official = true;
        db.update_proxy_config_for_app(cfg).await.unwrap();

        let router = ProviderRouter::new(db.clone());
        let chain = router
            .select_providers("claude", Some("s"))
            .await
            .unwrap();
        // 池里有 2 个
        assert_eq!(chain.len(), 2);
    }

    #[tokio::test]
    #[serial]
    async fn test_legacy_call_with_multi_provider_disabled_goes_legacy() {
        let _home = TempHome::new();
        let db = Arc::new(Database::memory().unwrap());

        let p1 = make_pool_provider("a", 1);
        let p2 = make_pool_provider("b", 2);
        db.save_provider("claude", &p1).unwrap();
        db.save_provider("claude", &p2).unwrap();
        // 不调 enable_multi_provider——保持 multi_provider_enabled=false（默认 legacy 路径）
        // 但 auto_failover=true 以触发 legacy 队列分支
        let mut cfg = db.get_proxy_config_for_app("claude").await.unwrap();
        cfg.auto_failover_enabled = true;
        db.update_proxy_config_for_app(cfg).await.unwrap();

        // 把 a/b 加进 in_failover_queue，让 legacy 真能返回东西
        db.add_to_failover_queue("claude", "a").unwrap();
        db.add_to_failover_queue("claude", "b").unwrap();

        let router = ProviderRouter::new(db.clone());
        // 不设环境变量时，multi_provider_enabled=false ⇒ 走 legacy
        let chain = router
            .select_providers("claude", None)
            .await
            .unwrap();
        // legacy 行为：按 in_failover_queue 顺序返回
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].id, "a");
        assert_eq!(chain[1].id, "b");
    }
}
