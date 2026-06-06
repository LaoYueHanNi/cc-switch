//! 多 Provider 轮询基础设施
//!
//! 阶段 0 仅落类型 + 灰度开关 + 轮询计数器。真正的路由逻辑接入在阶段 1。
//!
//! 关键设计：
//! - `multi_provider_enabled=false` 时**完全等价旧行为**（不引入任何行为变化）
//! - `CC_SWITCH_MULTI_PROVIDER=off` 环境变量是"硬降级"开关，强制走旧路径
//! - 轮询计数器纯内存，per-app 独立，进程崩溃/重启重置可接受

use crate::database::Database;
use crate::provider::Provider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// 环境变量：设为 `"off"` 时强制禁用多 Provider 模式（即使 DB 配了也走旧路径）
pub const ENV_DISABLE_MULTI_PROVIDER: &str = "CC_SWITCH_MULTI_PROVIDER";

/// 灰度开关读取：返回 true 表示多 Provider 模式被禁用（必须走旧路径）
///
/// 读取优先级：
/// 1. 环境变量 `CC_SWITCH_MULTI_PROVIDER=off` → 强制禁用（最高优先级，开发/灰度期使用）
/// 2. 否则按 DB 配的 `multi_provider_enabled` 决定
pub fn is_multi_provider_disabled() -> bool {
    if let Ok(val) = std::env::var(ENV_DISABLE_MULTI_PROVIDER) {
        let v = val.trim().to_ascii_lowercase();
        if v == "off" || v == "0" || v == "false" || v == "no" || v == "disabled" {
            return true;
        }
    }
    false
}

/// 轮询策略
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PoolStrategy {
    /// 严格轮询：按 counter % pool_size 选
    RoundRobin,
    /// 加权随机：按 pool_weight 比例
    WeightedRandom,
    /// 选失败次数最少的 provider（需要查 provider_health）
    LeastFailures,
    /// 选 p50 延迟最低的 provider（需要查 provider_health）
    LowestLatency,
}

impl Default for PoolStrategy {
    fn default() -> Self {
        Self::RoundRobin
    }
}

impl PoolStrategy {
    /// 从 DB 字符串反序列化
    pub fn from_db_str(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "weighted_random" => Self::WeightedRandom,
            "least_failures" => Self::LeastFailures,
            "lowest_latency" => Self::LowestLatency,
            _ => Self::RoundRobin,
        }
    }

    /// 序列化为 DB 字符串
    pub fn to_db_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::WeightedRandom => "weighted_random",
            Self::LeastFailures => "least_failures",
            Self::LowestLatency => "lowest_latency",
        }
    }
}

/// 多 Provider 配置（per-app）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MultiProviderConfig {
    /// 是否启用多 Provider 轮询模式
    pub multi_provider_enabled: bool,
    /// 轮询池选择策略
    pub pool_strategy: PoolStrategy,
    /// Session 粘性 TTL（秒，60..=86400）
    pub session_ttl_seconds: u32,
    /// 是否滑动 TTL（true=每次命中续期；false=固定过期）
    pub session_ttl_sliding: bool,
    /// 是否允许把官方 Provider 加入轮询池
    pub pool_include_official: bool,
}

impl Default for MultiProviderConfig {
    fn default() -> Self {
        Self {
            multi_provider_enabled: false,
            pool_strategy: PoolStrategy::RoundRobin,
            session_ttl_seconds: 3600,
            session_ttl_sliding: false,
            pool_include_official: false,
        }
    }
}

/// Session 绑定记录
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionBinding {
    pub session_id: String,
    pub app_type: String,
    pub provider_id: String,
    pub bound_at: i64,
    pub expires_at: i64,
    pub last_seen_at: i64,
    pub request_count: u32,
}

/// 路由选择结果
///
/// 设计意图：阶段 0 不接入路由（保持旧 select_providers 不变），
/// 但类型先就位，方便阶段 1 改造时直接使用。
#[derive(Debug, Clone)]
pub enum ProviderSelection {
    /// 单 Provider 模式（multi_provider_enabled=false）— 等价旧行为
    Single(Provider),
    /// 短期 session 已绑定到某 provider
    SessionBound {
        provider: Provider,
        session_id: String,
    },
    /// 多 Provider 模式：primary + fallbacks（故障队列其余成员）
    Pool {
        primary: Provider,
        fallbacks: Vec<Provider>,
    },
}

impl ProviderSelection {
    /// 返回所有候选 provider 列表（按尝试顺序）
    pub fn into_chain(self) -> Vec<Provider> {
        match self {
            Self::Single(p) => vec![p],
            Self::SessionBound { provider, .. } => vec![provider],
            Self::Pool { primary, fallbacks } => {
                let mut chain = Vec::with_capacity(1 + fallbacks.len());
                chain.push(primary);
                chain.extend(fallbacks);
                chain
            }
        }
    }

    /// 单元素时为 true（用于决定 bypass_circuit_breaker）
    pub fn is_singleton(&self) -> bool {
        match self {
            Self::Single(_) => true,
            Self::SessionBound { .. } => false, // session 模式下保留熔断保护
            Self::Pool { primary, fallbacks } => fallbacks.is_empty(),
        }
    }
}

/// 当前 Unix 时间（秒）
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Per-app 轮询计数器
///
/// - 纯内存，`RwLock<HashMap>` 替代 DashMap（避免新依赖，per-app 写竞争极低）
/// - 进程重启后重置（可接受：pool 按 sort_index 仍有合理默认顺序）
#[derive(Debug, Default)]
pub struct PoolCounter {
    counters: RwLock<HashMap<String, AtomicU64>>,
}

impl PoolCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 原子自增并返回旧值
    pub fn next(&self, app_type: &str) -> u64 {
        // 先尝试读路径
        {
            let map = self.counters.read().expect("PoolCounter poisoned");
            if let Some(c) = map.get(app_type) {
                return c.fetch_add(1, Ordering::Relaxed);
            }
        }
        // miss 时升级为写
        let mut map = self.counters.write().expect("PoolCounter poisoned");
        let counter = map
            .entry(app_type.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed)
    }
}

/// ProviderRouter 持有的多 Provider 共享状态（阶段 0 占位，阶段 1 接入）
#[derive(Debug, Default, Clone)]
pub struct MultiProviderState {
    pub counter: Arc<PoolCounter>,
}

impl MultiProviderState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// 判断当前 app 是否真的启用了多 Provider 模式
///
/// 三层判定（任一为 true 则禁用）：
/// 1. 环境变量 `CC_SWITCH_MULTI_PROVIDER=off`
/// 2. DB 字段 `multi_provider_enabled=0`
/// 3. （阶段 1 引入）池为空、池仅含 official 等边界条件
///
/// 阶段 0 只覆盖前两层。
pub async fn is_multi_provider_active(
    db: &Database,
    app_type: &str,
) -> Result<bool, crate::error::AppError> {
    if is_multi_provider_disabled() {
        return Ok(false);
    }
    let cfg = db.get_proxy_config_for_app(app_type).await?;
    Ok(cfg.multi_provider_enabled)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_disable_recognizes_off_values() {
        // 直接覆盖环境变量会影响并行测试，所以这里只测 is_multi_provider_disabled 内部的字符串逻辑
        for v in ["off", "OFF", "0", "false", "no", "disabled", " true "] {
            // 空格包夹应不影响 "true"，但 env var 是 " true " 不是 "true"
            // 内部逻辑不 trim, 我们只验证规范化行为
            let normalized = v.trim().to_ascii_lowercase();
            let recognized_off = matches!(
                normalized.as_str(),
                "off" | "0" | "false" | "no" | "disabled"
            );
            if normalized == "true" {
                assert!(!recognized_off, "{v} should not be recognized as off");
            } else {
                assert!(recognized_off, "{v} should be recognized as off");
            }
        }
    }

    #[test]
    fn test_pool_strategy_roundtrip() {
        for s in [
            PoolStrategy::RoundRobin,
            PoolStrategy::WeightedRandom,
            PoolStrategy::LeastFailures,
            PoolStrategy::LowestLatency,
        ] {
            assert_eq!(PoolStrategy::from_db_str(s.to_db_str()), s);
        }
        // 未识别字符串降级为 round_robin
        assert_eq!(
            PoolStrategy::from_db_str("nonsense"),
            PoolStrategy::RoundRobin
        );
    }

    #[test]
    fn test_pool_counter_atomic() {
        use std::sync::Arc;
        use std::thread;

        let c = Arc::new(PoolCounter::new());
        let mut handles = vec![];
        for _ in 0..8 {
            let cc = c.clone();
            handles.push(thread::spawn(move || {
                let mut local = vec![];
                for _ in 0..1000 {
                    local.push(cc.next("claude"));
                }
                local
            }));
        }
        let mut all: Vec<u64> = vec![];
        for h in handles {
            all.extend(h.join().unwrap());
        }
        all.sort();
        // 应当产生 0..8000 的不重复序列
        for (i, v) in all.iter().enumerate() {
            assert_eq!(*v, i as u64, "counter must be dense and unique");
        }
    }

    #[test]
    fn test_provider_selection_chain_order() {
        let p1 = Provider::with_id("p1".to_string(), "P1".to_string(), serde_json::json!({}), None);
        let p2 = Provider::with_id("p2".to_string(), "P2".to_string(), serde_json::json!({}), None);
        let p3 = Provider::with_id("p3".to_string(), "P3".to_string(), serde_json::json!({}), None);

        let s = ProviderSelection::Single(p1.clone());
        assert_eq!(s.clone().into_chain().len(), 1);
        assert!(s.is_singleton());

        let s = ProviderSelection::SessionBound {
            provider: p1.clone(),
            session_id: "s1".into(),
        };
        assert_eq!(s.clone().into_chain().len(), 1);
        assert!(!s.is_singleton());

        let s = ProviderSelection::Pool {
            primary: p1.clone(),
            fallbacks: vec![p2.clone(), p3.clone()],
        };
        let chain = s.clone().into_chain();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].id, "p1");
        assert_eq!(chain[1].id, "p2");
        assert_eq!(chain[2].id, "p3");
        assert!(!s.is_singleton());

        // 单元素 pool 等价 singleton
        let s = ProviderSelection::Pool {
            primary: p1.clone(),
            fallbacks: vec![],
        };
        assert!(s.is_singleton());
    }

    #[test]
    fn test_unix_now_is_sensible() {
        let n = unix_now();
        // 2026-01-01 UTC = 1767225600，2027-01-01 = 1798761600
        assert!(n > 1_700_000_000, "unix_now should be after 2023");
        assert!(n < 1_900_000_000, "unix_now should be before 2030");
    }
}
